//! Native whole-home maintenance through the existing node owner boundary.

use super::*;
use laststore::disk_pack::maintenance::{space_usage, MaintenanceReceipt, SpacePolicy};

/// Explicit native maintenance options. Preview is the default at the node route.
#[derive(Clone, Copy, Debug)]
pub struct NativeMaintenanceOptions {
    /// Report current bytes and files without a physical rewrite or deletion.
    pub dry_run: bool,
    /// Release only unneeded generation files; do not rewrite live data.
    pub reclaim_only: bool,
    /// An explicit grace override; None keeps the product's 24-hour default.
    pub grace_ms: Option<u64>,
}

/// Physical whole-home result. Logical records, atom versions, and keys do not change.
#[derive(serde::Serialize)]
pub struct NativeMaintenanceReport {
    pub dry_run: bool,
    pub executed: bool,
    pub reclaim_only: bool,
    pub generation: u64,
    pub frontier: u64,
    pub disk_cap_bytes: u64,
    pub files_before: u64,
    pub files_after: u64,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub records_copied: u64,
    pub tombstones_copied: u64,
    pub parts_before: u64,
    pub parts_after: u64,
    pub files_removed: u64,
    pub bytes_removed: u64,
    pub reclaim_deferred: bool,
}

impl PackRuntime {
    /// The writer durably fenced older schema writers before v2 R-edge proofs.
    pub fn schema_record_refs_supported(&self) -> bool {
        self.view
            .read()
            .expect("native complete read view")
            .current
            .required_features
            & laststore::disk_pack::FEATURE_SCHEMA_RECORD_REFS
            != 0
    }

    /// Serialize with complete writes, publish replacements, install their view,
    /// then reclaim while new read captures remain excluded by the view lock.
    pub async fn maintain_native(
        self: &Arc<Self>,
        options: NativeMaintenanceOptions,
    ) -> StorageResult<NativeMaintenanceReport> {
        let gate = self.enter_write().await;
        let runtime = Arc::clone(self);
        super::super::laststore::LastStoreKvStore::run_blocking(move || {
            let _gate = gate;
            runtime.maintain_native_blocking(options)
        })
        .await
    }

    fn maintain_native_blocking(
        &self,
        options: NativeMaintenanceOptions,
    ) -> StorageResult<NativeMaintenanceReport> {
        if self.failed.load(Ordering::Acquire) {
            return Err(StorageError::BackendError(
                "native maintenance requires complete recovery of the failed writer".into(),
            ));
        }
        let policy = SpacePolicy {
            grace_ms: options.grace_ms.unwrap_or(86_400_000),
            ..self.space_policy.clone()
        };
        let before = space_usage(self.home.path(), policy.max_files).map_err(map)?;
        let mut writer = self.journal.lock().expect("native journal owner");
        let mut receipt = MaintenanceReceipt::default();
        if !options.dry_run && !options.reclaim_only {
            receipt = match writer.pack(&policy) {
                Ok(receipt) => receipt,
                Err(error) => {
                    self.failed.store(true, Ordering::Release);
                    return Err(map(error));
                }
            };
        }
        // All older calls already hold their own generation. New calls take the
        // view read lock before their lifetime hold, so no retired view can escape.
        let mut view = self.view.write().expect("native complete read view");
        if !view.accepted.is_empty() {
            return Err(StorageError::BackendError(
                "native maintenance cannot replace an accepted write view".into(),
            ));
        }
        view.current = writer.current().clone();
        if !options.dry_run {
            let reclaimed = writer.reclaim(&policy).map_err(map)?;
            receipt.files_removed = reclaimed.files_removed;
            receipt.bytes_removed = reclaimed.bytes_removed;
            receipt.reclaim_deferred = reclaimed.reclaim_deferred;
            view.current = writer.current().clone();
        }
        let current = view.current.clone();
        drop(view);
        let after = space_usage(self.home.path(), policy.max_files).map_err(map)?;
        Ok(NativeMaintenanceReport {
            dry_run: options.dry_run,
            executed: !options.dry_run,
            reclaim_only: options.reclaim_only,
            generation: current.generation,
            frontier: current.frontier,
            disk_cap_bytes: policy.disk_cap_bytes,
            files_before: before.files,
            files_after: after.files,
            bytes_before: before.bytes,
            bytes_after: after.bytes,
            records_copied: receipt.records_copied,
            tombstones_copied: receipt.tombstones_copied,
            parts_before: receipt.parts_before,
            parts_after: receipt.parts_after,
            files_removed: receipt.files_removed,
            bytes_removed: receipt.bytes_removed,
            reclaim_deferred: receipt.reclaim_deferred,
        })
    }
}
