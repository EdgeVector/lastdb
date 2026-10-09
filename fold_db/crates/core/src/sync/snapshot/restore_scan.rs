//! Namespace restore, namespace scan and key-ownership helpers for snapshots.

use super::*;

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
    let existing = kv.scan_prefix(&[]).await?;
    let keys: Vec<Vec<u8>> = existing
        .into_iter()
        .map(|(key, _)| key)
        .filter(|key| match mode {
            RestoreMode::Personal => !is_org_scoped_key_bytes(key),
            RestoreMode::Scoped(prefixes) => key_belongs_to_any_target(key, prefixes),
        })
        .collect();
    if !keys.is_empty() {
        kv.batch_delete(keys).await?;
    }

    let mut report = NamespaceRestoreResult::default();
    use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
    for chunk in namespace.entries.chunks(RESTORE_BATCH_SIZE) {
        let mut items = Vec::with_capacity(chunk.len());
        for entry in chunk {
            let key = BASE64.decode(&entry.key).map_err(|error| {
                SyncError::Serialization(format!("invalid key base64: {error}"))
            })?;
            match mode {
                RestoreMode::Personal if is_org_scoped_key_bytes(&key) => {
                    report.org_skipped = report.org_skipped.saturating_add(1);
                    continue;
                }
                RestoreMode::Scoped(prefixes) if !key_belongs_to_any_target(&key, prefixes) => {
                    return Err(SyncError::Serialization(
                        "scoped snapshot contained an out-of-scope key".to_string(),
                    ));
                }
                _ => {}
            }
            let value = BASE64.decode(&entry.value).map_err(|error| {
                SyncError::Serialization(format!("invalid value base64: {error}"))
            })?;
            items.push((key, value));
        }
        if !items.is_empty() {
            report.entries = report.entries.saturating_add(items.len());
            report.batches = report.batches.saturating_add(1);
            kv.restore_batch_put(items).await?;
        }
    }
    Ok(report)
}

pub(super) fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

/// A whole-store photograph fails on the first unreadable row. Name the
/// namespace in the error: the at-rest codec error carries no row identity,
/// and without it an operator cannot find the row that blocks every
/// compaction (the 2026-09-29 primary failed 44 times with no clue).
pub(super) fn namespace_scan_error(
    ns_name: &str,
    e: &crate::storage::error::StorageError,
) -> SyncError {
    SyncError::Storage(format!("snapshot scan of namespace '{ns_name}': {e}"))
}

pub(super) async fn collect_snapshot_namespaces(
    store: &dyn NamespacedStore,
    mut mode: SnapshotNamespaceMode<'_>,
) -> SyncResult<Vec<NamespaceData>> {
    use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

    let ns_names = store.list_namespaces().await?;
    let mut namespaces = Vec::with_capacity(ns_names.len());

    for ns_name in &ns_names {
        if snapshot_should_skip_namespace(ns_name) {
            continue;
        }

        let kv = store.open_namespace(ns_name).await?;
        let entries = match &mut mode {
            SnapshotNamespaceMode::All => kv
                .scan_prefix(&[])
                .await
                .map_err(|e| namespace_scan_error(ns_name, &e))?
                .into_iter()
                .map(|(k, v)| SnapshotEntry {
                    key: BASE64.encode(&k),
                    value: BASE64.encode(&v),
                })
                .collect(),
            SnapshotNamespaceMode::Reporting(report) => {
                let scan = kv
                    .scan_prefix_partition_undecryptable(&[])
                    .await
                    .map_err(|e| namespace_scan_error(ns_name, &e))?;
                for key in &scan.undecryptable {
                    report.undecryptable.push(UndecryptableRow {
                        namespace: ns_name.clone(),
                        key_b64: BASE64.encode(key),
                    });
                }
                scan.rows
                    .into_iter()
                    .map(|(k, v)| SnapshotEntry {
                        key: BASE64.encode(&k),
                        value: BASE64.encode(&v),
                    })
                    .collect()
            }
            SnapshotNamespaceMode::Scoped(target_prefixes) => kv
                .scan_prefix(&[])
                .await
                .map_err(|e| namespace_scan_error(ns_name, &e))?
                .into_iter()
                .filter(|(k, _)| key_belongs_to_any_target(k, target_prefixes))
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
