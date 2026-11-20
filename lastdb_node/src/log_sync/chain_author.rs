//! Author one signed entry on this node's device stream.
//!
//! The entry is accepted locally, appended to the authored log, queued on the
//! outbox, and the sync loop is woken. All of it happens under the history
//! lock, so two callers cannot take the same `writer_seq`.

use std::path::Path;

use app_identity_crypto::SigningKey;
use mutation_log::{
    entry_hash, AcceptOutcome, AuthorCtx, DatabaseId, EntryHash, NodeKey, RejectReason,
    SignedEntry, VerifiedHistory,
};

use super::apply::AuthorIdentity;
use super::birth;
use super::chain_error::ChainError;
use super::history_store::{self, HistoryMap};
use crate::host::Host;

/// Where an entry is authored.
pub(super) struct Target<'a> {
    pub(super) home: &'a Path,
    pub(super) database_id: &'a str,
    pub(super) identity: &'a AuthorIdentity,
}

/// The entry that was accepted.
pub(super) struct Authored {
    pub(super) entry_hash: EntryHash,
    pub(super) writer_seq: u64,
}

pub(super) fn identity_of(host: &Host) -> Result<AuthorIdentity, ChainError> {
    let device_id = super::device::device_id(&host.home).map_err(|_| ChainError::Unconfirmed)?;
    Ok(AuthorIdentity {
        signing_key: SigningKey::from_bytes(&host.keypair.secret_key_bytes()),
        node_key: NodeKey::from_bytes(host.keypair.public_key_bytes()),
        device_id,
    })
}

/// Author a publisher entry. The first one also authors the signed birth.
pub(super) fn author_publisher(
    histories: &HistoryMap,
    target: &Target<'_>,
    build: impl FnOnce(AuthorCtx, &VerifiedHistory) -> Result<SignedEntry, ChainError>,
) -> Result<Authored, ChainError> {
    author_with(histories, target, true, build)
}

/// Author a member entry on a history this node already holds.
pub(super) fn author_member(
    histories: &HistoryMap,
    target: &Target<'_>,
    build: impl FnOnce(AuthorCtx, &VerifiedHistory) -> Result<SignedEntry, ChainError>,
) -> Result<Authored, ChainError> {
    author_with(histories, target, false, build)
}

fn author_with(
    histories: &HistoryMap,
    target: &Target<'_>,
    publisher: bool,
    build: impl FnOnce(AuthorCtx, &VerifiedHistory) -> Result<SignedEntry, ChainError>,
) -> Result<Authored, ChainError> {
    let database = DatabaseId::from_hex(target.database_id)
        .map_err(|_| ChainError::Invalid("database id is not a 64-hex id"))?;
    let result = histories
        .with_mut(target.database_id, |history| {
            if publisher {
                ensure_publisher(history, target, database)?;
            }
            let entry = build(base_ctx(history, database, target.identity), history)?;
            accept_local(history, target, entry)
        })
        .map_err(|_| ChainError::Unconfirmed)?;
    if result.is_ok() {
        super::wake::request_wake(target.home);
    }
    result
}

pub(super) fn base_ctx(
    history: &VerifiedHistory,
    database_id: DatabaseId,
    identity: &AuthorIdentity,
) -> AuthorCtx {
    let stream = VerifiedHistory::stream(identity.node_key, identity.device_id);
    AuthorCtx {
        database_id,
        node_key: identity.node_key,
        device_id: identity.device_id,
        stream_head: history.stream_head(&stream),
        seen_heads: Vec::new(),
        seen_lamports: Vec::new(),
        authored_at_ms: fold_db::clock::unix_millis(),
    }
}

fn ensure_publisher(
    history: &mut VerifiedHistory,
    target: &Target<'_>,
    database: DatabaseId,
) -> Result<(), ChainError> {
    if history.owner().is_none() {
        let material = birth::load(target.home, target.database_id)
            .map_err(|_| ChainError::Unconfirmed)?
            .ok_or(ChainError::Conflict(
                "this database has no signed birth on this node",
            ))?;
        let ctx = base_ctx(history, database, target.identity);
        let entry =
            birth::entry(&material, &ctx, target.identity).map_err(|_| ChainError::Unconfirmed)?;
        accept_local(history, target, entry)?;
    }
    if history.owner() == Some(target.identity.node_key) {
        Ok(())
    } else {
        Err(ChainError::Forbidden(
            "this node is not the publisher of this database",
        ))
    }
}

fn accept_local(
    history: &mut VerifiedHistory,
    target: &Target<'_>,
    entry: SignedEntry,
) -> Result<Authored, ChainError> {
    let hash = entry_hash(&entry).map_err(|_| ChainError::Unconfirmed)?;
    let writer_seq = entry.header().writer_seq;
    match history.accept_entry(entry.clone()) {
        AcceptOutcome::Accept { .. } => {}
        AcceptOutcome::Pending { reason } => {
            tracing::warn!(target: "lastdbd::log_sync", ?reason, "authored entry is pending locally");
            return Err(ChainError::Unconfirmed);
        }
        AcceptOutcome::Reject { reason } => return Err(rejected(&reason)),
    }
    if history_store::append_authored(target.home, target.database_id, &entry).is_err() {
        // Memory must not run ahead of the log it is rebuilt from.
        *history = history_store::replay_authored(target.home, history.database_id());
        return Err(ChainError::Unconfirmed);
    }
    history.outbox_mut().enqueue(hash, entry);
    history_store::persist_outbox(target.home, target.database_id, history.outbox())
        .map_err(|_| ChainError::Unconfirmed)?;
    Ok(Authored {
        entry_hash: hash,
        writer_seq,
    })
}

fn rejected(reason: &RejectReason) -> ChainError {
    match reason {
        RejectReason::Unauthorized(message) => ChainError::Forbidden(message),
        RejectReason::StreamFrozen | RejectReason::Equivocation { .. } => {
            ChainError::Conflict("this device stream is frozen")
        }
        other => {
            tracing::warn!(target: "lastdbd::log_sync", reason = ?other, "authored entry was rejected locally");
            ChainError::Unconfirmed
        }
    }
}
