//! Node-global immutable files survive every logical restore scope.

use super::*;
use crate::storage::{disk_pack::transaction, KvStore};
use std::sync::Arc;

pub(super) fn is_main_file_key(key: &[u8]) -> bool {
    key.starts_with(b"cas_blob:")
}

/// Keep the existing object for each identity, including its encryption bytes
/// and metadata. A restore never replaces a shared file with an older copy.
pub(super) async fn merge_missing(
    store: &dyn NamespacedStore,
    kv: &Arc<dyn KvStore>,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    key_prefix: &[u8],
    mode: RestoreMode<'_>,
) -> SyncResult<NamespaceRestoreResult> {
    if matches!(mode, RestoreMode::Scoped(_)) && !entries.is_empty() {
        return Err(SyncError::Serialization(
            "scoped snapshot cannot contain node-global file objects".into(),
        ));
    }
    for (key, _) in &entries {
        validate_identity(key, key_prefix)?;
    }
    let mut report = NamespaceRestoreResult::default();
    let mut remaining = entries.as_slice();
    while !remaining.is_empty() {
        let width = batch_len(remaining);
        let chunk = &remaining[..width];
        let count = transaction::background_scope(store.disk_pack_runtime(), async {
            let found = kv
                .exists_many(chunk.iter().map(|(key, _)| key.clone()).collect())
                .await?;
            if found.len() != chunk.len() {
                return Err(SyncError::Storage(
                    "restore file existence batch lost alignment".into(),
                ));
            }
            let missing: Vec<_> = chunk
                .iter()
                .zip(found)
                .filter(|(_, present)| !present)
                .map(|(entry, _)| entry.clone())
                .collect();
            let count = missing.len();
            if count != 0 {
                kv.restore_batch_put(missing).await?;
            }
            Ok(count)
        })
        .await?;
        report.entries += count;
        report.batches += usize::from(count != 0);
        remaining = &remaining[width..];
    }
    Ok(report)
}

/// One large file can stand alone; additional files share a 16 MiB allowance.
/// Native existence reads include stored bodies, so key count alone is not a bound.
fn batch_len(entries: &[(Vec<u8>, Vec<u8>)]) -> usize {
    let mut bytes = 0usize;
    let mut count = 0;
    for (key, value) in entries.iter().take(128) {
        let next = bytes
            .saturating_add(key.len())
            .saturating_add(value.len())
            .saturating_add(256);
        if count != 0 && next > 16 * 1024 * 1024 {
            break;
        }
        bytes = next;
        count += 1;
    }
    count
}

fn validate_identity(key: &[u8], key_prefix: &[u8]) -> SyncResult<()> {
    let hash = key
        .strip_prefix(key_prefix)
        .and_then(|key| key.strip_prefix(b"sha256:"));
    if !hash.is_some_and(|hash| {
        hash.len() == 64
            && hash
                .iter()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    }) {
        return Err(SyncError::Serialization(
            "restore file key is not a current SHA-256 identity".into(),
        ));
    }
    Ok(())
}
