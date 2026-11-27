//! Author one signed entry on this node's device stream.
//!
//! The entry (together with `birth` when the publisher signs its first entry)
//! is accepted on a copy of the history, appended to the authored log in one
//! atomic batch, and the sync loop is woken. All of it happens under the
//! history lock, so two callers cannot take the same `writer_seq`.

use std::cell::Cell;
use std::path::Path;

use app_identity_crypto::SigningKey;
use mutation_log::{
    entry_hash, stream_id_of, AcceptOutcome, AuthorCtx, DatabaseId, EntryHash, NodeKey,
    RejectReason, SignedEntry, VerifiedHistory,
};

use super::birth;
use super::chain_error::ChainError;
use super::history_store::{self, HistoryMap};
use crate::host::Host;

pub(crate) use crate::author_identity::AuthorIdentity;

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

pub(crate) fn identity_of(host: &Host) -> Result<AuthorIdentity, ChainError> {
    let device_id = super::device::device_id(&host.home).map_err(|_| ChainError::Unconfirmed)?;
    Ok(AuthorIdentity {
        signing_key: SigningKey::from_bytes(&host.keypair.secret_key_bytes()),
        node_key: NodeKey::from_bytes(host.keypair.public_key_bytes()),
        device_id,
    })
}

/// Author a publisher entry. The first one also authors the signed birth in the
/// same atomic batch.
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
            let cause: Cell<Option<ChainError>> = Cell::new(None);
            let out: Cell<Option<Authored>> = Cell::new(None);
            history_store::commit_authored_batch(target.home, history, |next| {
                let mut batch = Vec::with_capacity(2);
                if publisher {
                    if let Err(err) = ensure_publisher(next, target, database, &mut batch) {
                        cause.set(Some(err));
                        return Err("publisher check failed".into());
                    }
                }
                let entry = match build(base_ctx(next, database, target.identity), next) {
                    Ok(entry) => entry,
                    Err(err) => {
                        cause.set(Some(err));
                        return Err("entry build failed".into());
                    }
                };
                match stage_local(next, entry, &mut batch) {
                    Ok(authored) => {
                        out.set(Some(authored));
                        Ok(batch)
                    }
                    Err(err) => {
                        cause.set(Some(err));
                        Err("entry acceptance failed".into())
                    }
                }
            })
            .map_err(|error| {
                if let Some(err) = cause.take() {
                    return err;
                }
                tracing::warn!(
                    target: "lastdbd::log_sync",
                    %error,
                    "authored entry was not appended to the log"
                );
                ChainError::Unconfirmed
            })?;
            out.take().ok_or(ChainError::Unconfirmed)
        })
        .map_err(|_| ChainError::Unconfirmed)?;
    if result.is_ok() {
        super::wake::wake_loop();
    }
    result
}

fn base_ctx(
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
    batch: &mut Vec<SignedEntry>,
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
        stage_local(history, entry, batch)?;
    }
    if history.owner() == Some(target.identity.node_key) {
        Ok(())
    } else {
        Err(ChainError::Forbidden(
            "this node is not the publisher of this database",
        ))
    }
}

fn stage_local(
    history: &mut VerifiedHistory,
    entry: SignedEntry,
    batch: &mut Vec<SignedEntry>,
) -> Result<Authored, ChainError> {
    if history.pending_on(&stream_id_of(&entry)) {
        tracing::warn!(
            target: "lastdbd::log_sync",
            "authored stream has entries waiting for a dependency"
        );
        return Err(ChainError::Unconfirmed);
    }
    let hash = entry_hash(&entry).map_err(|_| ChainError::Unconfirmed)?;
    let writer_seq = entry.header().writer_seq;
    match history.accept_entry(entry.clone()) {
        AcceptOutcome::Accept { .. } => {
            batch.push(entry);
            Ok(Authored {
                entry_hash: hash,
                writer_seq,
            })
        }
        AcceptOutcome::Pending { reason } => {
            tracing::warn!(target: "lastdbd::log_sync", ?reason, "authored entry is pending locally");
            Err(ChainError::Unconfirmed)
        }
        AcceptOutcome::Reject { reason } => Err(rejected(&reason)),
    }
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
