//! Native whole-home maintenance through the existing node owner boundary.

use super::*;
use laststore::disk_pack::maintenance::{space_usage, MaintenanceReceipt, SpacePolicy, SpaceUsage};
use tracing::Instrument;

mod publish;

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

    /// Publish replacements under the write gate, then release it before cleanup.
    /// A separate maintenance owner excludes competing cleanup and pack calls.
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
        let runtime = Arc::clone(self);
        // This owner survives cancellation of the HTTP caller. Keep its serial
        // lease in the blocking closure until every file operation ends.
        tokio::spawn(
            async move {
                let maintenance = Arc::clone(&runtime.maintenance_gate).lock_owned().await;
                let measuring = Arc::clone(&runtime);
                let before = super::super::laststore::LastStoreKvStore::run_blocking(move || {
                    measuring.maintenance_usage(options)
                })
                .await?;
                let gate = runtime.enter_write().await;
                super::super::laststore::LastStoreKvStore::run_blocking(move || {
                    let _maintenance = maintenance;
                    runtime.maintain_native_blocking(
                        options,
                        keep_pack_on_reclaim_error,
                        before,
                        gate,
                    )
                })
                .await
            }
            .in_current_span(),
        )
        .await
        .map_err(|error| StorageError::BackendError(format!("native maintenance owner: {error}")))?
    }

    fn maintenance_policy(&self, options: NativeMaintenanceOptions) -> SpacePolicy {
        SpacePolicy {
            grace_ms: options.grace_ms.unwrap_or(86_400_000),
            reclaim_generations: options.reclaim_generations.unwrap_or(usize::MAX),
            ..self.space_policy.clone()
        }
    }

    fn maintenance_usage(&self, options: NativeMaintenanceOptions) -> StorageResult<SpaceUsage> {
        if options.report_usage {
            space_usage(self.home.path(), self.space_policy.max_files).map_err(map)
        } else {
            Ok(SpaceUsage::default())
        }
    }

    fn maintain_native_blocking(
        &self,
        options: NativeMaintenanceOptions,
        keep_pack_on_reclaim_error: bool,
        before: SpaceUsage,
        gate: tokio::sync::OwnedMutexGuard<()>,
    ) -> StorageResult<NativeMaintenanceReport> {
        self.require_initialized()?;
        if self.failed.load(Ordering::Acquire) {
            return Err(StorageError::BackendError(
                "native maintenance requires complete recovery of the failed writer".into(),
            ));
        }
        let policy = self.maintenance_policy(options);
        let (mut receipt, pack_millis, plan, current) =
            self.prepare_maintenance(options, &policy)?;
        // The installed floor and all active old reads protect their complete
        // closures. Future output cannot appear in an older deletion plan.
        drop(gate);
        let mut reclaim_millis = 0;
        let mut reclaim_error = None;
        if !options.dry_run {
            let started = std::time::Instant::now();
            // The pack is published. Only the worker keeps its result on a reclaim error.
            let keep_pack = keep_pack_on_reclaim_error && !options.reclaim_only;
            reclaim_error =
                self.note_reclaim(plan.execute(&policy, false), &mut receipt, keep_pack)?;
            reclaim_millis = millis_since(started);
            if !options.reclaim_only {
                self.parts.add_run_millis(reclaim_millis);
            }
        }
        let after = self.maintenance_usage(options)?;
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
