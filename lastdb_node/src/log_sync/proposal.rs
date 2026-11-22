//! Member proposals: signed `data` entries with `stream = proposal`.
//!
//! A proposal never writes a record. It names the publisher grants this node
//! holds in `seen_heads`, so AcceptEntry can fold the grant window and check
//! that this key may submit. The publisher later incorporates or ignores it.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

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
use crate::registered_share::RegisteredShare;

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
    value: Vec<u8>,
}

/// Map the owner submission document onto the history record convention:
/// namespace is the schema name, key is the hash key, value is the publisher
/// decision envelope (`mutation_type`, `expected`, optional `record`).
fn parse(body: &[u8]) -> Result<Proposed, ContributionError> {
    let Ok(Value::Object(doc)) = serde_json::from_slice::<Value>(body) else {
        return Err(ContributionError::Invalid);
    };
    let text = |value: Option<&Value>| value.and_then(Value::as_str).map(str::to_owned);
    let namespace = text(doc.get("schema")).ok_or(ContributionError::Invalid)?;
    let key = doc.get("key_value").and_then(Value::as_object);
    let record_key = text(key.and_then(|key| key.get("hash"))).ok_or(ContributionError::Invalid)?;
    let ranged = key.is_some_and(|key| key.get("range").is_some_and(|range| !range.is_null()));
    let carries_file = doc.get("file").is_some_and(|file| !file.is_null());
    let mutation_type = text(doc.get("mutation_type")).ok_or(ContributionError::Invalid)?;
    if namespace.is_empty()
        || namespace.len() > NAMESPACE_MAX
        || record_key.is_empty()
        || record_key.len() > usize::from(u16::MAX)
        || ranged
        || carries_file
        || !matches!(mutation_type.as_str(), "create" | "replace" | "delete")
    {
        return Err(ContributionError::Invalid);
    }
    let expected = doc.get("expected").ok_or(ContributionError::Invalid)?;
    let mut envelope = serde_json::Map::new();
    envelope.insert("mutation_type".into(), Value::String(mutation_type.clone()));
    envelope.insert("expected".into(), expected.clone());
    if mutation_type != "delete" {
        match doc.get("record") {
            Some(record @ Value::Object(_)) => {
                envelope.insert("record".into(), record.clone());
            }
            _ => return Err(ContributionError::Invalid),
        }
    }
    let value =
        serde_json::to_vec(&Value::Object(envelope)).map_err(|_| ContributionError::Invalid)?;
    if value.len() > BODY_MAX {
        return Err(ContributionError::Invalid);
    }
    Ok(Proposed {
        namespace,
        record_key,
        value,
    })
}

/// Author the proposal for `mutation_id`, or find the one already authored.
/// Returns its `writer_seq`, the submission sequence.
pub(crate) fn submit(
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
    let identity = chain_author::identity_of(host).map_err(|_| ContributionError::Unavailable)?;
    let target = Target {
        home: &host.home,
        database_id,
        identity: &identity,
    };
    let authored = chain_author::author_member(
        &super::histories_for(&host.home),
        &target,
        |mut ctx, history| {
            require_submitter(history, &identity.node_key)?;
            for (hash, lamport) in history.publisher_grants() {
                ctx.seen_heads.push(hash);
                ctx.seen_lamports.push(lamport);
            }
            let body = data_record(
                StreamKind::Proposal,
                proposed.namespace,
                &proposed.record_key,
                &proposed.value,
            );
            author_data(&ctx, &identity.signing_key, body).map_err(|_| ChainError::Unconfirmed)
        },
    )
    .map_err(|error| contribution_error(&error))?;
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

/// `submitted`, `published`, `discarded`, or `unknown`, from local history.
///
/// Published = publisher Direct cites `incorporated_from`. Discarded = a
/// publisher entry cites the proposal in `seen_heads` without incorporating it.
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
    let histories = super::histories_for(&host.home);
    histories
        .with_mut(database_id, |history| {
            if history.proposal_incorporated(&stream, done.writer_seq) {
                "published"
            } else if history.proposal_acknowledged(&stream, done.writer_seq) {
                "discarded"
            } else if history.entry_hash_at(&stream, done.writer_seq) == Some(done.entry_hash) {
                "submitted"
            } else {
                "unknown"
            }
        })
        .map_err(|_| ContributionError::Unavailable)
}
