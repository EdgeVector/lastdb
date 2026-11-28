//! After FoldDB commits an `/v1/write` to an owned database or to the personal
//! database, mirror it onto the signed log.

use fold_db::fold_db_core::FoldDB;
use fold_db::record_replacement::RecordTarget;
use fold_db::schema::types::{KeyValue, Mutation, MutationType};
use serde_json::Value;

use crate::exec::reply::HostError;
use crate::host::Host;
use crate::log_sync::owned_attachment::{OwnedFile, OwnedInline};
use crate::log_sync::{mirror_owned_write, wake_loop, write_target, OwnedMutation};

/// The 64-hex database id when `selector` names an owned shareable database or
/// a received signed-log share.
pub(super) fn shared_database_id(host: &Host, selector: Option<&str>) -> Option<String> {
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

/// Refuse shared-history key shapes that cannot be encoded on the wire.
pub(super) fn check_shared_item(index: usize, key: &KeyValue) -> Result<(), HostError> {
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

/// Enforce the deterministic shared-database 64 KiB `StoredRecord` cap before
/// FoldDB commits a shared write, matching `signed_history_materialize` so an
/// acknowledged write cannot later fail materialization.
pub(super) fn check_shared_mutation(
    store: &FoldDB,
    index: usize,
    mutation: &Mutation,
) -> Result<(), HostError> {
    if mutation.mutation_type == MutationType::Delete {
        return Ok(());
    }
    let Some(hash) = mutation.key_value.hash.as_deref().filter(|h| !h.is_empty()) else {
        return Ok(());
    };
    let key = signed_history_materialize::canonical_record_key(
        store,
        &mutation.schema_name,
        hash,
        mutation.key_value.range.as_deref(),
    )
    .and_then(|rk| signed_history_materialize::key_value(store, &mutation.schema_name, &rk))
    .unwrap_or_else(|_| mutation.key_value.clone());
    let alternates = signed_history_materialize::resolve_alternates(
        store,
        &mutation.schema_name,
        mutation.alternates.clone(),
    )
    .unwrap_or_else(|_| mutation.alternates.clone());
    let target = RecordTarget {
        schema: mutation.schema_name.clone(),
        key,
        alternates,
    };
    let file = mutation.file.clone();
    let inline_file = match &mutation.inline_write {
        Some(fold_db::record_value::InlineWrite::Put(file)) => Some(file.marker.clone()),
        Some(fold_db::record_value::InlineWrite::Clear) | None => None,
    };
    // Inline Put also charges the base64 payload against the 64 KiB record cap
    // via the marker size already stored on the marker; the separate inline row
    // is bounded by MAX_INLINE_FILE_BYTES at link time.
    signed_history_materialize::check_shared_record_size_with_attachments(
        &target,
        &mutation.record_fields,
        file,
        inline_file,
    )
    .map_err(|error| HostError::from(error).at(index))
}

/// Capture canonical schema/key/fields/alternates for the signed-history mirror.
pub(super) fn plan(
    store: &FoldDB,
    mutations: &[Mutation],
) -> Result<Vec<OwnedMutation>, HostError> {
    mutations
        .iter()
        .enumerate()
        .map(|(index, mutation)| {
            let hash = mutation
                .key_value
                .hash
                .as_deref()
                .ok_or_else(|| super::bad("shared write has no primary hash").at(index))?;
            let record_key = signed_history_materialize::canonical_record_key(
                store,
                &mutation.schema_name,
                hash,
                mutation.key_value.range.as_deref(),
            )
            .map_err(|error| super::bad(error).at(index))?;
            let mut mutation = mutation.clone();
            mutation.key_value =
                signed_history_materialize::key_value(store, &mutation.schema_name, &record_key)
                    .map_err(|error| super::bad(error).at(index))?;
            if mutation.mutation_type != MutationType::Delete {
                mutation.alternates = signed_history_materialize::resolve_alternates(
                    store,
                    &mutation.schema_name,
                    mutation.alternates,
                )
                .map_err(|error| super::bad(error).at(index))?;
            }
            Ok(OwnedMutation {
                mutation,
                record_key,
                file: OwnedFile::Keep,
            })
        })
        .collect()
}

/// Attach file and inline intents from mutations onto planned entries.
/// A Keep is resolved to the retained attachment so history winners stay
/// self-contained for late joiners.
pub(super) async fn attach_files(
    host: &Host,
    database_id: &str,
    store: &fold_db::fold_db_core::FoldDB,
    signed: &mut [OwnedMutation],
    mutations: &[Mutation],
) -> Result<(), HostError> {
    if signed.len() != mutations.len() {
        return Err(HostError::internal(
            "signed history plan length differs from the write list",
        ));
    }
    let histories = crate::log_sync::histories_for(host);
    for (item, mutation) in signed.iter_mut().zip(mutations.iter()) {
        if mutation.mutation_type == MutationType::Delete {
            continue;
        }
        item.file = OwnedFile::from_mutation(mutation.file_write.as_ref(), mutation.file.as_ref())
            .map_err(super::bad)?;
        item.file = item
            .file
            .clone()
            .resolve_keep(
                &histories,
                database_id,
                store,
                &item.mutation.schema_name,
                &item.record_key,
            )
            .await
            .map_err(super::bad)?;
        item.mutation.inline_write = OwnedInline::from_mutation(mutation.inline_write.as_ref())
            .resolve_keep(
                &histories,
                database_id,
                &item.mutation.schema_name,
                &item.record_key,
            )
            .map_err(super::bad)?
            .as_write();
    }
    Ok(())
}

/// The `(schema, record key)` of each mutation, as the history names records.
fn keys_of(signed: &[OwnedMutation]) -> Vec<(String, String)> {
    signed
        .iter()
        .map(|item| (item.mutation.schema_name.clone(), item.record_key.clone()))
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
