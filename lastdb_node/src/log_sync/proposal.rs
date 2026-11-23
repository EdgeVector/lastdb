//! Member proposals: signed `data` entries with `stream = proposal`.
//!
//! A member changes a database it received only by a proposal. A proposal never
//! writes a record. It names the publisher grants this node holds in
//! `seen_heads`, so AcceptEntry can fold the grant window and check that this
//! key may propose. The publisher decides each proposal once: it incorporates
//! it, or signs that it is not accepted.

mod ledger;

use std::collections::BTreeMap;

use cloud_backup_protocol::Id;
use mutation_log::{author_data, data_record, NodeKey, Role, StreamKind, VerifiedHistory};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use ledger::{fits_ledger, submit_lock};
pub(super) use ledger::{load_ledger, store_ledger, Submitted};

use super::chain_author::{self, Target};
use super::chain_error::ChainError;
use crate::host::Host;
use crate::registered_share::{self, RegisteredShare};

const BODY_MAX: usize = 64 * 1024;
const NAMESPACE_MAX: usize = 128;

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

struct Proposed {
    namespace: String,
    hash: String,
    range: Option<String>,
    record_key: String,
    mutation_type: String,
    expected: Value,
    record: Option<Value>,
    alternates: signed_history_materialize::Alternates,
}

/// Map the owner submission document onto the history record convention.
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
    let (record, alternates) = if mutation_type == "delete" {
        if doc
            .get("alternates")
            .is_some_and(|v| !v.is_null() && v.as_object().is_none_or(|m| !m.is_empty()))
        {
            return Err(ContributionError::Invalid);
        }
        (None, BTreeMap::new())
    } else {
        let Some(record @ Value::Object(_)) = doc.get("record") else {
            return Err(ContributionError::Invalid);
        };
        let alts = match doc.get("alternates") {
            None | Some(Value::Null) => BTreeMap::new(),
            Some(val) => {
                serde_json::from_value(val.clone()).map_err(|_| ContributionError::Invalid)?
            }
        };
        (Some(record.clone()), alts)
    };
    Ok(Proposed {
        namespace,
        hash,
        range,
        record_key,
        mutation_type,
        expected,
        record,
        alternates,
    })
}

async fn canonicalize_proposed(
    host: &Host,
    share: &RegisteredShare,
    proposed: &mut Proposed,
) -> Result<(), ContributionError> {
    let id =
        Id::try_from(share.id.as_str().to_owned()).map_err(|_| ContributionError::Unavailable)?;
    let store = registered_share::receiver_store(host, &id)
        .await
        .map_err(|_| ContributionError::Unavailable)?;
    let declared = store
        .schema_manager()
        .get_schema_metadata(&proposed.namespace)
        .map_err(|_| ContributionError::Unavailable)?
        .is_some();
    if !declared {
        return Ok(());
    }
    proposed.record_key = signed_history_materialize::canonical_record_key(
        &store,
        &proposed.namespace,
        &proposed.hash,
        proposed.range.as_deref(),
    )
    .map_err(|_| ContributionError::Invalid)?;
    if proposed.mutation_type != "delete" {
        proposed.alternates = signed_history_materialize::resolve_alternates(
            &store,
            &proposed.namespace,
            std::mem::take(&mut proposed.alternates),
        )
        .map_err(|_| ContributionError::Invalid)?;
    }
    Ok(())
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
    if !proposed.alternates.is_empty() {
        let alts =
            serde_json::to_value(&proposed.alternates).map_err(|_| ContributionError::Invalid)?;
        envelope.insert("alternates".into(), alts);
    }
    let value =
        serde_json::to_vec(&Value::Object(envelope)).map_err(|_| ContributionError::Invalid)?;
    if value.len() > BODY_MAX {
        return Err(ContributionError::Invalid);
    }
    Ok(value)
}

/// Name the expected record state in history terms.
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
    let mut proposed = parse(body)?;
    let digest = hex_digest(body);
    let lock = submit_lock(&host.home, database_id);
    let _guard = lock.lock().await;
    let mut ledger = load_ledger(&host.home, database_id)?;
    let key = mutation_id.to_string();
    if let Some(done) = ledger.get(&key) {
        return if done.body_sha256 == digest {
            Ok(done.writer_seq)
        } else {
            Err(ContributionError::Conflict)
        };
    }
    canonicalize_proposed(host, share, &mut proposed).await?;
    let expected = history_expected(host, share, &proposed).await?;
    let value = envelope(&proposed, expected)?;
    fits_ledger(&ledger, &key, &digest)?;
    let authored = author_proposal(
        host,
        database_id,
        proposed.namespace,
        &proposed.record_key,
        &value,
    )?;
    if let Ok(pending) = super::history_store::pending_count(&host.home, database_id) {
        super::publish_state::note_pending(database_id, pending);
    }
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

/// True when this node already signed a proposal for `mutation_id`.
pub(crate) fn has_submitted(
    host: &Host,
    share: &RegisteredShare,
    mutation_id: &uuid::Uuid,
) -> Result<bool, ContributionError> {
    let ledger = load_ledger(&host.home, share.id.as_str())?;
    Ok(ledger.contains_key(&mutation_id.to_string()))
}

/// Sign `value` as a proposal on this node's member stream.
pub(super) fn author_proposal(
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

pub(super) fn hex_digest(bytes: &[u8]) -> String {
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
