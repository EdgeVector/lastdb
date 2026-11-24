//! Member side of write leases: when this node may sign a direct entry, and
//! how a direct change of a received database is signed.
//!
//! A member's node decides every write from the newest lease it holds and its
//! own clock. It asks no relay. The counting rule has one home, in
//! `mutation_log`; this file only reads its answers (`latest_lease`,
//! `lease_covers`).

use mutation_log::{
    author_data, data_record, EntryHash, LeaseView, NodeKey, Role, StreamKind, VerifiedHistory,
};

use super::chain_author::{self, Target};
use super::chain_error::ChainError;
use super::mirror::{encode_value, OwnedMutation};
use crate::host::Host;

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
    let lamport = history.get_accepted(&lease.entry_hash)?.position.lamport;
    Some(Cover {
        lease: lease.entry_hash,
        lamport,
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
            let applied = match super::histories_for(host).snapshot(database_id) {
                Ok(history) => super::materialize::apply_history_records(&store, &history).await,
                Err(error) => Err(error),
            };
            if let Err(error) = applied {
                tracing::warn!(
                    target: "lastdbd::log_sync",
                    id = %database_id,
                    %error,
                    "a signed direct write could not be applied to the store again"
                );
                super::histories_for(host).mark_materialize_retry(database_id);
            }
            Ok(Some(live_revisions(&store, signed).await))
        }
        Err(error) => {
            if let Err(revert_error) = try_revert_unsigned(host, database_id, &store, signed).await
            {
                tracing::warn!(
                    target: "lastdbd::log_sync",
                    id = %database_id,
                    error = %revert_error,
                    "an unsigned write could not be reverted to the history"
                );
                super::histories_for(host).mark_materialize_retry(database_id);
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

async fn try_revert_unsigned(
    host: &Host,
    database_id: &str,
    store: &fold_db::fold_db_core::FoldDB,
    signed: &[OwnedMutation],
) -> Result<(), String> {
    let keys: Vec<(String, String)> = signed
        .iter()
        .map(|item| (item.schema.clone(), item.record_key.clone()))
        .collect();
    let history = super::histories_for(host).snapshot(database_id)?;
    signed_history_materialize::reconcile_keys(store, &history, &keys).await
}

fn sign_all(
    host: &Host,
    store: &fold_db::fold_db_core::FoldDB,
    database_id: &str,
    signed: &[OwnedMutation],
) -> Result<(), ChainError> {
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
    let identity = chain_author::identity_of(host)?;
    let target = Target {
        home: &host.home,
        database_id,
        identity: &identity,
    };
    let histories = super::histories_for(host);
    for (item, value) in encoded {
        chain_author::author_member(&histories, &target, |mut ctx, history| {
            // Entries that wait for a dependency mean the stream tip is not known.
            if history.pending_len() > 0 {
                return Err(ChainError::Unconfirmed);
            }
            let cover = cover(history, identity.node_key, ctx.authored_at_ms)
                .ok_or(ChainError::Forbidden(REFUSED))?;
            ctx.seen_heads.push(cover.lease);
            ctx.seen_lamports.push(cover.lamport);
            let body = data_record(
                StreamKind::Direct,
                item.schema.clone(),
                &item.record_key,
                &value,
            );
            author_data(&ctx, &identity.signing_key, body).map_err(|_| ChainError::Unconfirmed)
        })?;
    }
    Ok(())
}
