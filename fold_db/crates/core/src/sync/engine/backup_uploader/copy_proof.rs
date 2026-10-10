//! Local copy proof and the existing serving mutation fence.

use super::*;

impl SyncEngine {
    pub(super) async fn acquire_backup_mutation_fence(
        &self,
        primary_resume_root: bool,
    ) -> SyncResult<Option<tokio::sync::OwnedRwLockWriteGuard<()>>> {
        // Primary resume already owns this non-reentrant fence across its cut.
        if primary_resume_root {
            return Ok(None);
        }
        let router = self.photograph_mutation_router.lock().await.clone();
        match router {
            Some(router) => Ok(Some(router.fence_mutations().await)),
            None if self.backup_only_mode.load(Ordering::Acquire) => Ok(None),
            None => Err(SyncError::Storage(
                "normal backup cut requires a serving mutation fence".into(),
            )),
        }
    }
}

pub(super) fn require_atom_photograph_copy(
    store: &crate::storage::laststore::LastStoreNamespacedStore,
    manifest: &BackupManifest,
    cloud_presence: Option<&CloudChunkPresence>,
    fresh_resume_root: bool,
    backup_only: bool,
) -> SyncResult<()> {
    let copy_cloud = if backup_only && !fresh_resume_root {
        cloud_presence
    } else {
        None
    };
    let copy = store
        .atom_photograph_copy_report(manifest, copy_cloud, !backup_only)
        .map_err(|e| SyncError::Storage(format!("atom keep-set copy check failed: {e}")))?;
    if copy.is_complete() {
        return Ok(());
    }
    tracing::error!(
        target: "fold_db::sync::backup",
        generation = manifest.counter,
        atom_chunks = manifest.atom_chunks.len(),
        local_chunks = copy.local_chunks,
        verified_local_prefixes = copy.verified_local_prefixes,
        verified_cloud_copies = copy.verified_cloud_copies,
        missing_local_chunks = copy.missing_local_chunks,
        mismatched_local_prefixes = copy.mismatched_local_prefixes,
        unexpected_local_chunks = copy.unexpected_local_chunks,
        invalid_manifest_refs = copy.invalid_manifest_refs,
        "atom keep-set lacks a local file or a verified cloud copy; packing-lock cut refused"
    );
    Err(SyncError::Storage(
        "atom keep-set lacks a local file or a verified cloud copy; packing-lock cut refused"
            .into(),
    ))
}
