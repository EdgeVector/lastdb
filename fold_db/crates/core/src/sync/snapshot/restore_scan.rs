//! Namespace restore, namespace scan and key-ownership helpers for snapshots.

use super::*;
mod blob_roots;
mod proofs;
pub(super) use proofs::{begin_reference_restore, finish_reference_restore};

#[derive(Clone, Copy)]
pub(super) enum RestoreMode<'a> {
    Personal,
    Scoped(&'a [String]),
}

#[derive(Default)]
pub(super) struct NamespaceRestoreResult {
    pub(super) entries: usize,
    pub(super) batches: usize,
    pub(super) org_skipped: usize,
}

pub(super) async fn restore_namespace(
    store: &dyn NamespacedStore,
    namespace: &NamespaceData,
    mode: RestoreMode<'_>,
) -> SyncResult<NamespaceRestoreResult> {
    let kv = store.open_namespace(&namespace.name).await?;
    let existing = namespace_rows(kv.as_ref()).await?;
    use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
    let decoded = namespace
        .entries
        .iter()
        .map(|entry| {
            let key = BASE64.decode(&entry.key).map_err(|error| {
                SyncError::Serialization(format!("invalid key base64: {error}"))
            })?;
            let value = BASE64.decode(&entry.value).map_err(|error| {
                SyncError::Serialization(format!("invalid value base64: {error}"))
            })?;
            Ok((key, value))
        })
        .collect::<SyncResult<Vec<_>>>()?;
    let is_main = namespace.name == "main";
    blob_roots::clear_existing(store, &kv, existing, mode, is_main).await?;
    if is_main {
        blob_roots::prepare_incoming(store, &decoded, mode).await?;
    }
    let mut report = NamespaceRestoreResult::default();
    for chunk in decoded.chunks(128) {
        let mut items = Vec::with_capacity(chunk.len());
        for (key, value) in chunk {
            let key = key.clone();
            if is_main
                && (proofs::is_reference_proof(&key)
                    || crate::db_operations::atom_store::BlobRefEdge::is_target_key(&key))
            {
                continue;
            }
            // Node-local rows never travel in a snapshot: the peer-apply row
            // (the restoring node writes its own `seen` after the install) and
            // the `gcatoms-*` probe checkpoints and markers, which describe the
            // lap of the node that wrote them and must not steer this one.
            if crate::sync::policy::is_node_local_gc_scratch_key(&key) {
                continue;
            }
            match mode {
                RestoreMode::Personal if is_org_scoped_key_bytes(&key) => {
                    report.org_skipped = report.org_skipped.saturating_add(1);
                    continue;
                }
                RestoreMode::Scoped(prefixes)
                    if !namespace_key_belongs_to_any_target(&key, prefixes, is_main) =>
                {
                    return Err(SyncError::Serialization(
                        "scoped snapshot contained an out-of-scope key".to_string(),
                    ));
                }
                _ => {}
            }
            items.push((key, value.clone()));
        }
        if !items.is_empty() {
            report.entries = report.entries.saturating_add(items.len());
            report.batches = report.batches.saturating_add(1);
            kv.restore_batch_put(items).await?;
        }
    }
    Ok(report)
}

async fn namespace_rows(kv: &dyn crate::storage::KvStore) -> SyncResult<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut cursor: Option<crate::storage::traits::PhysicalScanCursor> = None;
    let mut rows = Vec::new();
    loop {
        let page = kv
            .scan_range_physical_paged(b"", b"\xff", cursor.as_ref(), 128, 1)
            .await?;
        rows.extend(page.rows);
        let Some(next) = page.next_cursor else { break };
        if cursor.as_ref() == Some(&next) {
            return Err(SyncError::Serialization(
                "snapshot namespace walk did not advance".into(),
            ));
        }
        cursor = Some(next);
    }
    Ok(rows)
}

pub(super) fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

pub(super) async fn collect_snapshot_namespaces(
    store: &dyn NamespacedStore,
    mode: SnapshotNamespaceMode<'_>,
) -> SyncResult<Vec<NamespaceData>> {
    use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

    let runtime = store.disk_pack_runtime();
    let boundary = runtime.as_ref().map(|runtime| runtime.snapshot_boundary());
    let ns_names = store.list_namespaces().await?;
    let mut namespaces = Vec::with_capacity(ns_names.len());

    for ns_name in &ns_names {
        if snapshot_should_skip_namespace(ns_name) {
            continue;
        }

        let kv = store.open_namespace(ns_name).await?;
        let entries = match &mode {
            SnapshotNamespaceMode::All => namespace_rows(kv.as_ref())
                .await?
                .into_iter()
                .filter(|(k, _)| !crate::sync::policy::is_node_local_gc_scratch_key(k))
                .map(|(k, v)| SnapshotEntry {
                    key: BASE64.encode(&k),
                    value: BASE64.encode(&v),
                })
                .collect(),
            SnapshotNamespaceMode::Scoped(target_prefixes) => namespace_rows(kv.as_ref())
                .await?
                .into_iter()
                .filter(|(k, _)| {
                    namespace_key_belongs_to_any_target(k, target_prefixes, ns_name == "main")
                })
                .map(|(k, v)| SnapshotEntry {
                    key: BASE64.encode(&k),
                    value: BASE64.encode(&v),
                })
                .collect(),
        };

        namespaces.push(NamespaceData {
            name: ns_name.clone(),
            entries,
        });
    }

    if runtime.as_ref().map(|runtime| runtime.snapshot_boundary()) != boundary {
        return Err(SyncError::Storage(
            "native data changed during the snapshot; no complete backup was created".into(),
        ));
    }
    Ok(namespaces)
}

/// Org-scoped storage keys that must be dropped on personal restore after
/// the org-crypto map was removed (see [`storage_prefix_for_key`]).
pub(super) fn is_org_scoped_key_bytes(key: &[u8]) -> bool {
    std::str::from_utf8(key)
        .ok()
        .and_then(storage_prefix_for_key)
        .is_some()
}

pub(super) fn namespace_key_belongs_to_any_target(
    key: &[u8],
    target_prefixes: &[String],
    is_main: bool,
) -> bool {
    if is_main {
        return std::str::from_utf8(key)
            .ok()
            .and_then(|key| {
                crate::storage::current_keys::exact_scope(key)
                    .ok()
                    .flatten()
            })
            .is_some_and(|scope| target_prefixes.iter().any(|prefix| prefix == scope));
    }
    key_belongs_to_any_target(key, target_prefixes)
}

pub(super) fn key_belongs_to_any_target(key: &[u8], target_prefixes: &[String]) -> bool {
    target_prefixes
        .iter()
        .any(|target_prefix| key_belongs_to_target(key, target_prefix))
}

pub(super) fn key_belongs_to_target(key: &[u8], target_prefix: &str) -> bool {
    let Ok(key) = std::str::from_utf8(key) else {
        return false;
    };
    let prefixed = format!("{target_prefix}:");
    if key.starts_with(&prefixed) {
        return true;
    }
    for native_prefix in ["emb:", "graveyard:emb:"] {
        if key
            .strip_prefix(native_prefix)
            .is_some_and(|rest| rest.starts_with(&prefixed))
        {
            return true;
        }
    }
    storage_prefix_for_key(key) == Some(target_prefix)
}
