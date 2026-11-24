//! Member proposals: signed `data` entries with `stream = proposal`.
//!
//! A member changes a database it received only by a proposal. A proposal never
//! writes a record. It names the publisher grants this node holds in
//! `seen_heads`, so AcceptEntry can fold the grant window and check that this
//! key may propose. The publisher decides each proposal once: it incorporates
//! it, or signs that it is not accepted.

mod ledger;
mod parse;

use std::path::Path;
use std::sync::Arc;

use cloud_backup_protocol::Id;
use mutation_log::{author_data, data_record, NodeKey, Role, StreamKind, VerifiedHistory};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use ledger::{fits_ledger, submit_lock};
pub(super) use ledger::{load_ledger, store_ledger, Submitted};
use parse::{canonicalize_proposed, envelope, parse, Proposed};

use super::chain_author::{self, Target};
use super::chain_error::ChainError;
use crate::host::Host;
use crate::registered_share::{self, RegisteredShare};

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

/// Name the expected record state in history terms.
async fn history_expected(
    host: &Host,
    share: &RegisteredShare,
    proposed: &Proposed,
) -> Result<Value, ContributionError> {
    let kind = proposed.expected.get("type").and_then(Value::as_str);
    if proposed.mutation_type == "create" && kind == Some("absent") {
        return Ok(proposed.expected.clone());
    }
    if !matches!(proposed.mutation_type.as_str(), "replace" | "delete") {
        return Err(ContributionError::Invalid);
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
    held_lock: Option<tokio::sync::OwnedMutexGuard<()>>,
) -> Result<u64, ContributionError> {
    let database_id = share.id.as_str();
    let mut proposed = parse(body)?;
    let digest = hex_digest(body);
    let _guard = if let Some(guard) = held_lock {
        guard
    } else {
        submit_lock(&host.home, database_id).lock_owned().await
    };
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

/// Check under `submit_lock` whether this node already signed a proposal for
/// `mutation_id`, returning the guard so the caller's decision cannot race.
pub(crate) async fn has_submitted(
    host: &Host,
    share: &RegisteredShare,
    mutation_id: &uuid::Uuid,
) -> Result<(bool, tokio::sync::OwnedMutexGuard<()>), ContributionError> {
    let guard = submit_lock(&host.home, share.id.as_str())
        .lock_owned()
        .await;
    let ledger = load_ledger(&host.home, share.id.as_str())?;
    Ok((ledger.contains_key(&mutation_id.to_string()), guard))
}

pub(super) fn fits_proposal_ledger(
    ledger: &ledger::Ledger,
    key: &str,
    digest: &str,
) -> Result<(), ContributionError> {
    fits_ledger(ledger, key, digest)
}

pub(super) fn proposal_submit_lock(home: &Path, database_id: &str) -> Arc<tokio::sync::Mutex<()>> {
    submit_lock(home, database_id)
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
