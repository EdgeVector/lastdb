//! Member proposals: signed `data` entries with `stream = proposal`.
//!
//! A member changes a database it received only by a proposal. A proposal never
//! writes a record. It names the publisher grants this node holds in
//! `seen_heads`, so AcceptEntry can fold the grant window and check that this
//! key may propose. The publisher decides each proposal once: it incorporates
//! it, or signs that it is not accepted.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use cloud_backup_protocol::Id;
use mutation_log::{
    author_data, data_record, EntryHash, NodeKey, Role, StreamKind, VerifiedHistory,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use super::chain_author::{self, Target};
use super::chain_error::ChainError;
use crate::host::Host;
use crate::profile::private_fs as disk;
use crate::registered_share::{self, RegisteredShare};

const BODY_MAX: usize = 64 * 1024;
const NAMESPACE_MAX: usize = 128;
const LEDGER_LIMIT: usize = 4 * 1024 * 1024;

/// Share-write contribution failures, in the owner route's terms.
#[derive(Debug)]
pub(crate) enum ContributionError {
    /// The append was not confirmed locally (503, retry).
    Unavailable,
    /// This member's folded grant does not allow a proposal (403, final).
    Forbidden,
    /// The mutation document cannot be a proposal (400).
    Invalid,
    /// The same idempotency key named a different mutation (409).
    Conflict,
}

impl std::fmt::Display for ContributionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Unavailable => "shared mutation was not confirmed",
            Self::Forbidden => "this member cannot submit this shared mutation",
            Self::Invalid => "invalid shared mutation or schema definition",
            Self::Conflict => "the idempotency key names a different shared mutation",
        })
    }
}

#[derive(Serialize, Deserialize)]
struct Submitted {
    writer_seq: u64,
    entry_hash: EntryHash,
    body_sha256: String,
}

type Ledger = BTreeMap<String, Submitted>;

fn ledger_path(home: &Path, database_id: &str) -> PathBuf {
    home.join("log-sync")
        .join(database_id)
        .join("proposals.json")
}

fn load_ledger(home: &Path, database_id: &str) -> Result<Ledger, ContributionError> {
    let path = ledger_path(home, database_id);
    match disk::read_limited(&path, LEDGER_LIMIT) {
        Ok(Some(bytes)) => {
            serde_json::from_slice(&bytes).map_err(|_| ContributionError::Unavailable)
        }
        Ok(None) => Ok(Ledger::new()),
        Err(_) => Err(ContributionError::Unavailable),
    }
}

fn store_ledger(home: &Path, database_id: &str, ledger: &Ledger) -> Result<(), ContributionError> {
    let bytes = serde_json::to_vec(ledger).map_err(|_| ContributionError::Unavailable)?;
    disk::write_private(&ledger_path(home, database_id), &bytes)
        .map_err(|_| ContributionError::Unavailable)
}

struct Proposed {
    namespace: String,
    record_key: String,
    mutation_type: String,
    expected: Value,
    record: Option<Value>,
    /// The address under each declared alternate key, as the member chose it.
    alternates: Option<Value>,
}

/// Map the owner submission document onto the history record convention:
/// namespace is the schema name, key is the hash key (joined with the range
/// key for a hash-range schema), value is the publisher
/// decision envelope (`mutation_type`, `expected`, optional `record` and
/// `alternates`). The publisher's acceptance signs the record itself, in the
/// layout of `signed_history_materialize::RecordValue`, with the alternate
/// addresses the store holds after it wrote the record.
fn parse(body: &[u8]) -> Result<Proposed, ContributionError> {
    let Ok(Value::Object(doc)) = serde_json::from_slice::<Value>(body) else {
        return Err(ContributionError::Invalid);
    };
    let text = |value: Option<&Value>| value.and_then(Value::as_str).map(str::to_owned);
    let namespace = text(doc.get("schema")).ok_or(ContributionError::Invalid)?;
    let key = doc.get("key_value").and_then(Value::as_object);
    let hash = text(key.and_then(|key| key.get("hash"))).ok_or(ContributionError::Invalid)?;
    let range = match key.and_then(|key| key.get("range")) {
        None | Some(Value::Null) => None,
        Some(range) => Some(text(Some(range)).ok_or(ContributionError::Invalid)?),
    };
    let record_key =
        mutation_log::join_record_key(&hash, range.as_deref()).ok_or(ContributionError::Invalid)?;
    let carries_file = doc.get("file").is_some_and(|file| !file.is_null());
    let mutation_type = text(doc.get("mutation_type")).ok_or(ContributionError::Invalid)?;
    if namespace.is_empty()
        || namespace.len() > NAMESPACE_MAX
        || record_key.is_empty()
        || record_key.len() > usize::from(u16::MAX)
        || carries_file
        || !matches!(mutation_type.as_str(), "create" | "replace" | "delete")
    {
        return Err(ContributionError::Invalid);
    }
    let expected = doc
        .get("expected")
        .ok_or(ContributionError::Invalid)?
        .clone();
    let record = if mutation_type == "delete" {
        None
    } else {
        match doc.get("record") {
            Some(record @ Value::Object(_)) => Some(record.clone()),
            _ => return Err(ContributionError::Invalid),
        }
    };
    let alternates = match doc.get("alternates") {
        Some(found @ Value::Object(_)) => Some(found.clone()),
        None | Some(Value::Null) => None,
        _ => return Err(ContributionError::Invalid),
    };
    Ok(Proposed {
        namespace,
        record_key,
        mutation_type,
        expected,
        record,
        alternates,
    })
}

/// The publisher decision envelope that goes into the signed proposal.
fn envelope(proposed: &Proposed, expected: Value) -> Result<Vec<u8>, ContributionError> {
    let mut envelope = serde_json::Map::new();
    envelope.insert(
        "mutation_type".into(),
        Value::String(proposed.mutation_type.clone()),
    );
    envelope.insert("expected".into(), expected);
    if let Some(record) = &proposed.record {
        envelope.insert("record".into(), record.clone());
    }
    if let Some(alternates) = &proposed.alternates {
        envelope.insert("alternates".into(), alternates.clone());
    }
    let value =
        serde_json::to_vec(&Value::Object(envelope)).map_err(|_| ContributionError::Invalid)?;
    if value.len() > BODY_MAX {
        return Err(ContributionError::Invalid);
    }
    Ok(value)
}

/// Name the expected record state in history terms. A revision a client read
/// is a hash of this node's local stamp, which no other node shares. When the
/// client's revision is still the one this node holds, the proposal names the
/// history entry that wrote that record instead, which every node shares. A
/// revision this node no longer holds names no entry, so the publisher cannot
/// match it and leaves the proposal submitted.
async fn history_expected(
    host: &Host,
    share: &RegisteredShare,
    proposed: &Proposed,
) -> Result<Value, ContributionError> {
    let kind = proposed.expected.get("type").and_then(Value::as_str);
    if kind == Some("absent") {
        return Ok(proposed.expected.clone());
    }
    let (Some("revision"), Some(revision)) = (
        kind,
        proposed.expected.get("revision").and_then(Value::as_str),
    ) else {
        return Err(ContributionError::Invalid);
    };
    let id =
        Id::try_from(share.id.as_str().to_owned()).map_err(|_| ContributionError::Unavailable)?;
    let store = registered_share::receiver_store(host, &id)
        .await
        .map_err(|_| ContributionError::Unavailable)?;
    let local = signed_history_materialize::read_revision(
        &store,
        &proposed.namespace,
        &proposed.record_key,
    )
    .await
    .map_err(|_| ContributionError::Unavailable)?;
    let winner = if local.as_deref() == Some(revision) {
        let key = (proposed.namespace.clone(), proposed.record_key.clone());
        super::histories_for(host)
            .with_mut(share.id.as_str(), |history| {
                history
                    .records()
                    .get(&key)
                    .map(|record| record.entry.to_hex())
            })
            .map_err(|_| ContributionError::Unavailable)?
    } else {
        None
    };
    Ok(serde_json::json!({ "type": "entry", "entry": winner }))
}

/// Author the proposal for `mutation_id`, or find the one already authored.
/// Returns its `writer_seq`, the submission sequence.
pub(crate) async fn submit(
    host: &Host,
    share: &RegisteredShare,
    mutation_id: &uuid::Uuid,
    body: &[u8],
) -> Result<u64, ContributionError> {
    let database_id = share.id.as_str();
    let proposed = parse(body)?;
    let digest = hex_digest(body);
    let mut ledger = load_ledger(&host.home, database_id)?;
    let key = mutation_id.to_string();
    if let Some(done) = ledger.get(&key) {
        return if done.body_sha256 == digest {
            Ok(done.writer_seq)
        } else {
            Err(ContributionError::Conflict)
        };
    }
    let expected = history_expected(host, share, &proposed).await?;
    let value = envelope(&proposed, expected)?;
    let authored = author_proposal(
        host,
        database_id,
        proposed.namespace,
        &proposed.record_key,
        &value,
    )?;
    ledger.insert(
        key,
        Submitted {
            writer_seq: authored.writer_seq,
            entry_hash: authored.entry_hash,
            body_sha256: digest,
        },
    );
    store_ledger(&host.home, database_id, &ledger)?;
    Ok(authored.writer_seq)
}

/// Sign `value` as a proposal on this node's member stream.
fn author_proposal(
    host: &Host,
    database_id: &str,
    namespace: String,
    record_key: &str,
    value: &[u8],
) -> Result<chain_author::Authored, ContributionError> {
    let identity = chain_author::identity_of(host).map_err(|_| ContributionError::Unavailable)?;
    let target = Target {
        home: &host.home,
        database_id,
        identity: &identity,
    };
    chain_author::author_member(&super::histories_for(host), &target, |mut ctx, history| {
        require_submitter(history, &identity.node_key)?;
        for (hash, lamport) in history.publisher_grants() {
            ctx.seen_heads.push(hash);
            ctx.seen_lamports.push(lamport);
        }
        let body = data_record(StreamKind::Proposal, namespace, record_key, value);
        author_data(&ctx, &identity.signing_key, body).map_err(|_| ChainError::Unconfirmed)
    })
    .map_err(|error| contribution_error(&error))
}

fn require_submitter(history: &VerifiedHistory, node_key: &NodeKey) -> Result<(), ChainError> {
    // Pending entries mean the stream tip is not known yet: signing now could fork it.
    if history.owner().is_none() || history.pending_len() > 0 {
        return Err(ChainError::Unconfirmed);
    }
    match history.grants().role_of(node_key) {
        Some(Role::Submit | Role::Write) => Ok(()),
        _ => Err(ChainError::Forbidden(
            "this member cannot submit this shared mutation",
        )),
    }
}

fn contribution_error(error: &ChainError) -> ContributionError {
    match error {
        ChainError::Forbidden(_) => ContributionError::Forbidden,
        _ => ContributionError::Unavailable,
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    app_identity_crypto::hex_lower(&Sha256::digest(bytes))
}

/// `submitted`, `published`, `not_accepted`, or `unknown`, from local history.
///
/// `published`: a publisher data entry cites the proposal in
/// `incorporated_from`. `not_accepted`: a publisher `heads` entry cites it. Both
/// are final. `submitted`: the publisher has not decided, or its decision has
/// not been pulled yet.
pub(crate) fn status(
    host: &Host,
    share: &RegisteredShare,
    mutation_id: &uuid::Uuid,
) -> Result<&'static str, ContributionError> {
    let database_id = share.id.as_str();
    let ledger = load_ledger(&host.home, database_id)?;
    let Some(done) = ledger.get(&mutation_id.to_string()) else {
        return Ok("unknown");
    };
    let identity = chain_author::identity_of(host).map_err(|_| ContributionError::Unavailable)?;
    let stream = VerifiedHistory::stream(identity.node_key, identity.device_id);
    let histories = super::histories_for(host);
    histories
        .with_mut(database_id, |history| {
            if history.proposal_incorporated(&stream, done.writer_seq) {
                "published"
            } else if history.proposal_declined(&done.entry_hash) {
                "not_accepted"
            } else if history.entry_hash_at(&stream, done.writer_seq) == Some(done.entry_hash) {
                "submitted"
            } else {
                "unknown"
            }
        })
        .map_err(|_| ContributionError::Unavailable)
}
