//! Member side of write leases: when this node may sign a direct entry, and
//! how a direct change of a received database is signed.
//!
//! A member's node decides every write from the newest lease it holds and its
//! own clock. It asks no relay. The counting rule has one home, in
//! `mutation_log`; this file only reads its answers (`latest_lease`,
//! `lease_covers`).

use std::cell::Cell;

use mutation_log::{
    author_data, data_record, AcceptOutcome, AuthorCtx, DatabaseId, EntryHash, LeaseView, NodeKey,
    RejectReason, Role, StreamKind, VerifiedHistory,
};

use super::chain_author;
use super::chain_error::ChainError;
use super::history_store;
use super::mirror::{encode_value, OwnedMutation};
use super::shared_key;
use crate::host::Host;
use crate::personal_seal::Sealer;
use crate::shared_seal;

/// The answer to a refused direct write, the same as for a database with no lease.
pub(crate) const REFUSED: &str = "a received database is read only";
/// The longest margin before a lease's expiry at which a node stops signing
/// direct entries.
const MARGIN_MAX_MS: u64 = 5 * 60 * 1000;

/// The lease a direct entry names.
pub(crate) struct Cover {
    /// Hash of the lease entry, cited in `seen_heads`.
    pub(crate) lease: EntryHash,
    /// Order value of the lease entry.
    pub(crate) lamport: u64,
    /// The node stops signing direct entries at this time, by its own clock.
    pub(crate) until_ms: u64,
}

/// How long before `expires_at_ms` a node stops signing direct entries: a
/// tenth of the lease length, at most five minutes. It covers a clock that
/// runs a little slow. The length is read from the lease entry itself, so every
/// node of a database uses the same margin.
pub(crate) fn margin_ms(lease: &LeaseView) -> u64 {
    let length = lease.expires_at_ms.saturating_sub(lease.authored_at_ms);
    (length / 10).min(MARGIN_MAX_MS)
}

/// The lease that lets `node` sign a direct entry at `now_ms`, if there is one.
///
/// All of these must hold: the node is a member, not the publisher; its current
/// role is `write` (the relay takes direct entries only from a current writer);
/// the newest lease it holds covers it; and `now_ms` is before the lease's
/// expiry less the margin.
pub(crate) fn cover(history: &VerifiedHistory, node: NodeKey, now_ms: u64) -> Option<Cover> {
    let owner = history.owner()?;
    if owner == node || history.grants().role_of(&node) != Some(Role::Write) {
        return None;
    }
    let lease = history.latest_lease()?;
    if !history.lease_covers(&lease.entry_hash, &node) {
        return None;
    }
    let until_ms = lease.expires_at_ms.saturating_sub(margin_ms(&lease));
    if now_ms >= until_ms {
        return None;
    }
    Some(Cover {
        lease: lease.entry_hash,
        lamport: lease.lamport,
        until_ms,
    })
}

/// True when this node may sign a direct entry in the received database now.
/// The history is rebuilt from the log on disk when it is not in memory, so
/// this works after a restart and with no relay.
pub(crate) fn may_write_direct(host: &Host, database_id: &str) -> bool {
    let node = NodeKey::from_bytes(host.keypair.public_key_bytes());
    super::histories_for(host)
        .with_mut(database_id, |history| {
            cover(history, node, fold_db::clock::unix_millis()).is_some()
        })
        .unwrap_or(false)
}

/// Sign the entries of a write to a received database, when `selector` names
/// one. `Ok(None)` means it names no received signed-log database.
///
/// The write is already in the store. When an entry cannot be signed (the
/// lease ended in between, or the log was not confirmed), the keys go back to
/// what the history says; if that revert fails, 503 is returned so `change`
/// never queues a proposal over an un-reverted commit. When all are signed, the
/// store is brought to the history and the live stored revisions are returned.
pub(crate) async fn mirror_received(
    host: &Host,
    selector: &str,
    signed: &[OwnedMutation],
) -> Result<Option<Vec<Option<String>>>, ChainError> {
    let Ok(Some(share)) = crate::registered_share::resolve(&host.home, selector) else {
        return Ok(None);
    };
    if !share.signed_log {
        return Ok(None);
    }
    let database_id = share.id.as_str();
    let store = crate::registered_share::receiver_store(host, &share.id)
        .await
        .map_err(|_| ChainError::Unconfirmed)?;
    match sign_all(host, &store, database_id, signed) {
        Ok(()) => {
            let histories = super::histories_for(host);
            if let Err(error) =
                super::materialize::rematerialize(&histories, database_id, &store).await
            {
                tracing::warn!(
                    target: "lastdbd::log_sync",
                    id = %database_id,
                    %error,
                    "a signed direct write could not be applied to the store again"
                );
                histories.mark_materialize_retry(database_id);
            }
            Ok(Some(live_revisions(&store, signed).await))
        }
        Err(error) => {
            let keys: Vec<(String, String)> = signed
                .iter()
                .map(|item| (item.schema.clone(), item.record_key.clone()))
                .collect();
            if !super::materialize::revert_unsigned(host, database_id, &store, &keys).await {
                return Err(ChainError::Unconfirmed);
            }
            Err(error)
        }
    }
}

async fn live_revisions(
    store: &fold_db::fold_db_core::FoldDB,
    signed: &[OwnedMutation],
) -> Vec<Option<String>> {
    let mut out = Vec::with_capacity(signed.len());
    for item in signed {
        let rev = signed_history_materialize::read_revision(store, &item.schema, &item.record_key)
            .await
            .ok()
            .flatten();
        out.push(rev);
    }
    out
}

fn sign_all(
    host: &Host,
    store: &fold_db::fold_db_core::FoldDB,
    database_id: &str,
    signed: &[OwnedMutation],
) -> Result<(), ChainError> {
    if signed.is_empty() {
        return Ok(());
    }
    let mut encoded = Vec::with_capacity(signed.len());
    for item in signed {
        let declared = store
            .schema_manager()
            .get_schema_metadata(&item.schema)
            .map_err(|_| ChainError::Unconfirmed)?
            .is_some();
        if !declared {
            return Err(ChainError::Invalid(
                "schema is not declared on this database",
            ));
        }
        let value = encode_value(item)
            .map_err(|_| ChainError::Invalid("the write does not carry its record fields"))?;
        encoded.push((item, value));
    }
    let database = DatabaseId::from_hex(database_id)
        .map_err(|_| ChainError::Invalid("database id is not a 64-hex id"))?;
    let identity = chain_author::identity_of(host)?;
    let sealer = shared_key::sealer(host, database_id).map_err(|_| ChainError::Unconfirmed)?;
    let stream = VerifiedHistory::stream(identity.node_key, identity.device_id);
    let histories = super::histories_for(host);
    histories
        .with_mut(database_id, |history| {
            if history.pending_len() > 0 || history.pending_on(&stream) {
                return Err(ChainError::Unconfirmed);
            }
            let now_ms = fold_db::clock::unix_millis();
            let cover =
                cover(history, identity.node_key, now_ms).ok_or(ChainError::Forbidden(REFUSED))?;
            let cause: Cell<Option<ChainError>> = Cell::new(None);
            let bctx = BatchCtx {
                database,
                identity: &identity,
                cover: &cover,
                sealer: &sealer,
                now_ms,
            };
            history_store::commit_authored_batch(&host.home, history, |next| {
                stage_direct_batch(next, &bctx, &encoded, &cause)
            })
            .map_err(|_| cause.take().unwrap_or(ChainError::Unconfirmed))
        })
        .map_err(|_| ChainError::Unconfirmed)??;
    super::wake::wake_loop();
    Ok(())
}

struct BatchCtx<'a> {
    database: DatabaseId,
    identity: &'a chain_author::AuthorIdentity,
    cover: &'a Cover,
    sealer: &'a Sealer,
    now_ms: u64,
}

fn stage_direct_batch(
    next: &mut VerifiedHistory,
    bctx: &BatchCtx<'_>,
    encoded: &[(&OwnedMutation, Vec<u8>)],
    cause: &Cell<Option<ChainError>>,
) -> Result<Vec<mutation_log::SignedEntry>, String> {
    let stream = VerifiedHistory::stream(bctx.identity.node_key, bctx.identity.device_id);
    let mut batch = Vec::with_capacity(encoded.len());
    for (item, value) in encoded {
        let mut ctx = AuthorCtx {
            database_id: bctx.database,
            node_key: bctx.identity.node_key,
            device_id: bctx.identity.device_id,
            stream_head: next.stream_head(&stream),
            seen_heads: Vec::new(),
            seen_lamports: Vec::new(),
            authored_at_ms: bctx.now_ms,
        };
        ctx.cite(bctx.cover.lease, bctx.cover.lamport);
        let key = (item.schema.clone(), item.record_key.clone());
        if let Some(winner) = next.records().get(&key) {
            ctx.cite(winner.entry, winner.position.lamport);
        }
        let body = shared_seal::seal_data(
            bctx.sealer,
            ctx.node_key,
            ctx.device_id,
            ctx.next_writer_seq(),
            data_record(
                StreamKind::Direct,
                item.schema.clone(),
                &item.record_key,
                value,
            ),
        )?;
        let entry =
            author_data(&ctx, &bctx.identity.signing_key, body).map_err(|e| e.to_string())?;
        match next.accept_entry(entry.clone()) {
            AcceptOutcome::Accept { .. } => batch.push(entry),
            AcceptOutcome::Reject { reason } => {
                cause.set(Some(match reason {
                    RejectReason::Unauthorized(msg) => ChainError::Forbidden(msg),
                    RejectReason::StreamFrozen | RejectReason::Equivocation { .. } => {
                        ChainError::Conflict("this device stream is frozen")
                    }
                    _ => ChainError::Unconfirmed,
                }));
                return Err("authored direct entry rejected".into());
            }
            AcceptOutcome::Pending { .. } => return Err("authored direct entry pending".into()),
        }
    }
    Ok(batch)
}
