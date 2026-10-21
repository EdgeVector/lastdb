//! Native whole-home maintenance through the existing node owner boundary.

use super::*;
use laststore::disk_pack::maintenance::{space_usage, MaintenanceReceipt, SpacePolicy, SpaceUsage};

/// Explicit native maintenance options. Preview is the default at the node route.
#[derive(Clone, Copy, Debug)]
pub struct NativeMaintenanceOptions {
    /// Report current bytes and files without a physical rewrite or deletion.
    pub dry_run: bool,
    /// Release only unneeded generation files; do not rewrite live data.
    pub reclaim_only: bool,
    /// An explicit grace override; None keeps the product's 24-hour default.
    pub grace_ms: Option<u64>,
    /// Measure the native root before and after. The measure walks every file,
    /// so the automatic high-water run leaves it off.
    pub report_usage: bool,
    /// Most old generations one reclaim retires, oldest first. `None` reclaims all
    /// of them under one hold; a bounded call lets writers in between calls.
    pub reclaim_generations: Option<usize>,
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
    /// Old generations remain after a bounded reclaim.
    pub reclaim_remaining: bool,
    /// The reclaim step failed after the pack published its replacement. Only the
    /// automatic worker sees this field set; an explicit call returns the error.
    pub reclaim_error: Option<String>,
    /// Milliseconds in the pack step and in the reclaim step.
    pub pack_millis: u64,
    pub reclaim_millis: u64,
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
        self.maintain_native_inner(options, false).await
    }

    /// The call of the automatic worker. A reclaim error after a published pack
    /// comes back in the report, so the worker keeps the pack result and backs off.
    pub(super) async fn maintain_native_for_worker(
        self: &Arc<Self>,
        options: NativeMaintenanceOptions,
    ) -> StorageResult<NativeMaintenanceReport> {
        self.maintain_native_inner(options, true).await
    }

    async fn maintain_native_inner(
        self: &Arc<Self>,
        options: NativeMaintenanceOptions,
        keep_pack_on_reclaim_error: bool,
    ) -> StorageResult<NativeMaintenanceReport> {
        let gate = self.enter_write().await;
        let runtime = Arc::clone(self);
        super::super::laststore::LastStoreKvStore::run_blocking(move || {
            let _gate = gate;
            runtime.maintain_native_blocking(options, keep_pack_on_reclaim_error)
        })
        .await
    }

    fn maintain_native_blocking(
        &self,
        options: NativeMaintenanceOptions,
        keep_pack_on_reclaim_error: bool,
    ) -> StorageResult<NativeMaintenanceReport> {
        self.require_initialized()?;
        if self.failed.load(Ordering::Acquire) {
            return Err(StorageError::BackendError(
                "native maintenance requires complete recovery of the failed writer".into(),
            ));
        }
        let policy = SpacePolicy {
            grace_ms: options.grace_ms.unwrap_or(86_400_000),
            reclaim_generations: options.reclaim_generations.unwrap_or(usize::MAX),
            ..self.space_policy.clone()
        };
        let usage = |home: &std::path::Path| match options.report_usage {
            true => space_usage(home, policy.max_files).map_err(map),
            false => Ok(SpaceUsage::default()),
        };
        let before = usage(self.home.path())?;
        let mut writer = self.journal.lock().expect("native journal owner");
        let mut receipt = MaintenanceReceipt::default();
        let (mut pack_millis, mut reclaim_millis) = (0, 0);
        if !options.dry_run && !options.reclaim_only {
            let started = std::time::Instant::now();
            receipt = match writer.pack(&policy) {
                Ok(receipt) => receipt,
                Err(error) => {
                    // A failure before the replacement is published leaves the
                    // writer healthy; only a poisoned writer needs a restart.
                    if writer.is_poisoned() {
                        self.failed.store(true, Ordering::Release);
                    }
                    let error = map(error);
                    self.parts.record_failure(&error);
                    return Err(error);
                }
            };
            pack_millis = millis_since(started);
            // The replacement is published: the count is final even if reclaim fails.
            let run = parts::RunCounts {
                parts_before: receipt.parts_before,
                parts_after: receipt.parts_after,
                records: receipt.records_copied + receipt.tombstones_copied,
            };
            self.parts.record_run(&run, pack_millis, writer.current());
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
        let mut reclaim_error = None;
        if !options.dry_run {
            let started = std::time::Instant::now();
            // The pack is published. Only the worker keeps its result on a reclaim error.
            let keep_pack = keep_pack_on_reclaim_error && !options.reclaim_only;
            reclaim_error = self.note_reclaim(writer.reclaim(&policy), &mut receipt, keep_pack)?;
            view.current = writer.current().clone();
            reclaim_millis = millis_since(started);
            if !options.reclaim_only {
                self.parts.add_run_millis(reclaim_millis);
            }
        }
        let current = view.current.clone();
        drop(view);
        let after = usage(self.home.path())?;
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
            reclaim_remaining: receipt.reclaim_remaining,
            reclaim_error,
            pack_millis,
            reclaim_millis,
        })
    }
}

impl PackRuntime {
    /// Record one reclaim result in the ledger and the receipt. An error comes back as
    /// text when the caller keeps the pack result, and as an error otherwise.
    fn note_reclaim(
        &self,
        result: Result<MaintenanceReceipt, laststore::Error>,
        receipt: &mut MaintenanceReceipt,
        keep_pack: bool,
    ) -> StorageResult<Option<String>> {
        match result {
            Ok(reclaimed) => {
                receipt.files_removed = reclaimed.files_removed;
                receipt.bytes_removed = reclaimed.bytes_removed;
                receipt.reclaim_deferred = reclaimed.reclaim_deferred;
                receipt.reclaim_remaining = reclaimed.reclaim_remaining;
                if !reclaimed.reclaim_deferred {
                    self.parts.record_reclaim_ok();
                }
                Ok(None)
            }
            Err(error) => {
                let error = map(error);
                self.parts.record_reclaim_failure(&error);
                if keep_pack {
                    Ok(Some(error.to_string()))
                } else {
                    Err(error)
                }
            }
        }
    }
}

fn millis_since(started: std::time::Instant) -> u64 {
    started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}
