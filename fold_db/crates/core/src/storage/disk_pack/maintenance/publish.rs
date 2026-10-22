//! The short owner handoff before concurrent old-file cleanup.
use super::*;
use laststore::disk_pack::journal::ReclaimPlan;

impl PackRuntime {
    pub(super) fn prepare_maintenance(
        &self,
        options: NativeMaintenanceOptions,
        policy: &SpacePolicy,
    ) -> StorageResult<(MaintenanceReceipt, u64, ReclaimPlan, Current)> {
        let mut writer = self.journal.lock().expect("native journal owner");
        let mut receipt = MaintenanceReceipt::default();
        let mut pack_millis = 0;
        if !options.dry_run && !options.reclaim_only {
            let started = std::time::Instant::now();
            receipt = match writer.pack(policy) {
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
        let plan = writer.capture_reclaim().map_err(map)?;
        Ok((receipt, pack_millis, plan, view.current.clone()))
    }
}
