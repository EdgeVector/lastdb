//! After FoldDB commits an `/v1/write` to an owned database or to the personal
//! database, mirror it onto the signed log.

use std::collections::HashMap;

use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::{KeyValue, MutationType};
use serde_json::Value;

use crate::exec::reply::HostError;
use crate::host::Host;
use crate::log_sync::{mirror_owned_write, wake_loop, write_target, OwnedMutation};

/// One write item's fields needed to author a signed `data` entry.
pub(super) struct SignedInput<'a> {
    pub(super) schema: &'a str,
    pub(super) key: &'a KeyValue,
    pub(super) op: MutationType,
    pub(super) record: Option<&'a HashMap<String, Value>>,
    pub(super) alternates: &'a signed_history_materialize::Alternates,
}

/// The 64-hex database id when `selector` names an owned shareable database or
/// a received signed-log share.
fn shared_database_id(host: &Host, selector: Option<&str>) -> Option<String> {
    let selector = selector?;
    if let Ok(entry) = crate::host::owned::find_shareable_database(&host.home, selector) {
        return Some(entry.id);
    }
    if let Ok(Some(share)) = crate::registered_share::resolve(&host.home, selector) {
        if share.signed_log {
            return Some(share.id.as_str().to_owned());
        }
    }
    None
}

/// Hold the per-database async write lock across commit + mirror for an owned
/// shareable database or a received signed-log share so `pull::rematerialize`
/// and `incorporate::resolve_owned` cannot interleave.
pub(super) async fn lock_shared_write(
    host: &Host,
    selector: Option<&str>,
) -> Option<tokio::sync::OwnedMutexGuard<()>> {
    let id = shared_database_id(host, selector)?;
    let lock = crate::log_sync::histories_for(host).write_lock(&id);
    Some(lock.lock_owned().await)
}

/// True when `selector` names an owned shareable database or a received
/// signed-log share.
pub(super) fn is_shared_target(host: &Host, selector: Option<&str>) -> bool {
    shared_database_id(host, selector).is_some()
}

/// Refuse features that signed-history shared databases cannot carry.
pub(super) fn check_shared_item(
    index: usize,
    has_file: bool,
    key: &KeyValue,
) -> Result<(), HostError> {
    if has_file {
        return Err(super::bad("files are not supported in shared databases").at(index));
    }
    if key.range.is_some()
        && key
            .hash
            .as_deref()
            .is_some_and(|h| h.contains(mutation_log::RANGE_SEPARATOR))
    {
        return Err(super::bad("hash key cannot contain unit separator").at(index));
    }
    Ok(())
}

/// Capture canonical schema/key/fields/alternates for the signed-history mirror.
pub(super) fn plan<'a>(
    store: &FoldDB,
    items: impl IntoIterator<Item = SignedInput<'a>>,
) -> Vec<OwnedMutation> {
    items
        .into_iter()
        .filter_map(|item| {
            let hash = item.key.hash.as_deref()?;
            let record_key = signed_history_materialize::canonical_record_key(
                store,
                item.schema,
                hash,
                item.key.range.as_deref(),
            )
            .ok()
            .or_else(|| mutation_log::join_record_key(hash, item.key.range.as_deref()))?;
            let fields = item.record.map(|map| {
                Value::Object(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            });
            let alternates = if item.op == MutationType::Delete {
                signed_history_materialize::Alternates::new()
            } else {
                signed_history_materialize::resolve_alternates(
                    store,
                    item.schema,
                    item.alternates.clone(),
                )
                .unwrap_or_else(|_| item.alternates.clone())
            };
            Some(OwnedMutation {
                schema: item.schema.to_owned(),
                record_key,
                op: item.op,
                fields,
                alternates,
            })
        })
        .collect()
}

/// The `(schema, record key)` of each mutation, as the history names records.
fn keys_of(signed: &[OwnedMutation]) -> Vec<(String, String)> {
    signed
        .iter()
        .map(|item| (item.schema.clone(), item.record_key.clone()))
        .collect()
}

/// A write to a received database reaches here only while a write lease covers
/// this node (see `app_api::database`). Its entries are signed as direct
/// entries that name the lease. A selector that names no received database
/// signs nothing.
async fn mirror_received(
    host: &Host,
    selector: Option<&str>,
    signed: &[OwnedMutation],
    revisions: &mut [Value],
) -> Result<(), HostError> {
    let Some(selector) = selector else {
        return Ok(());
    };
    match crate::log_sync::mirror_received_write(host, selector, signed).await {
        Ok(Some(live)) => {
            for (slot, found) in revisions.iter_mut().zip(live) {
                if let Some(rev) = found {
                    *slot = Value::String(rev);
                }
            }
            Ok(())
        }
        Ok(None) => Ok(()),
        Err(error) => {
            let reply = error.reply();
            let mut refusal = HostError::new(reply.status, reply.message);
            if let Some(code) = reply.code {
                refusal = refusal.with_code(code);
            }
            Err(if reply.retry {
                refusal.with_retry_after(crate::exec::database_share::RETRY_AFTER_SECS)
            } else {
                refusal
            })
        }
    }
}

/// Author the signed entries of an owned shareable write; a received database
/// is signed by `mirror_received`. The personal database is not signed
/// here: its write is already in the spool, and this wakes the sync loop.
///
/// The store validated and committed the write first. When its entries cannot
/// all be signed, the unsigned records are reverted to the history, so the
/// store never holds a record the history does not.
pub(super) async fn mirror_committed(
    host: &Host,
    selector: Option<&str>,
    signed: &[OwnedMutation],
    revisions: &mut [Value],
) -> Result<(), HostError> {
    let Some(target) = write_target(host, selector) else {
        return mirror_received(host, selector, signed, revisions).await;
    };
    if target.personal {
        // The commit already put the write in the spool of accepted writes, which
        // is the upload queue of the personal database. Nothing is signed here.
        wake_loop();
        return Ok(());
    }
    let Err(error) = mirror_owned_write(host, &target.id, signed) else {
        return Ok(());
    };
    tracing::warn!(
        target: "lastdbd::log_sync",
        %error,
        "owned write was not mirrored to the signed log"
    );
    let keys = keys_of(signed);
    if let Some(store) = crate::log_sync::owned_store(host, &target.id) {
        let _ = crate::log_sync::revert_unsigned(host, &target.id, &store, &keys).await;
    } else {
        crate::log_sync::mark_dirty_keys(host, &target.id, &keys);
    }
    Err(HostError::new(
        503,
        "signed history mirror was not confirmed",
    ))
}
