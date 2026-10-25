//! The maintenance owner survives caller cancellation while ordinary writes proceed.
use super::*;
use laststore::disk_pack::maintenance::PreparedCompaction;

impl PackRuntime {
    pub(super) async fn copy_native_pack(
        self: &Arc<Self>,
        options: NativeMaintenanceOptions,
    ) -> StorageResult<PreparedCompaction> {
        let runtime = Arc::clone(self);
        let plan = super::super::super::laststore::LastStoreKvStore::run_blocking(move || {
            runtime.require_initialized()?;
            if runtime.failed.load(Ordering::Acquire) {
                return Err(StorageError::BackendError(
                    "native compaction requires a recovered writer".into(),
                ));
            }
            runtime
                .v1()?
                .writer
                .lock()
                .expect("native snapshot writer")
                .prepare_pack(options.full_copy)
                .map_err(map)
        })
        .await?;
        self.parts.progress.phase(2);
        let policy = self.maintenance_policy(options);
        let mut output =
            super::super::super::laststore::LastStoreKvStore::run_blocking(move || {
                plan.copy(&policy).map_err(map)
            })
            .await?;
        // A bounded number of off-gate rounds reduces the final handoff delta.
        // If writers still outrun it, finish_pack refuses rather than holding
        // the gate for an unbounded copy. Every accepted write remains live.
        self.parts.progress.phase(3);
        for _ in 0..8 {
            let runtime = Arc::clone(self);
            let (next, changed) =
                super::super::super::laststore::LastStoreKvStore::run_blocking(move || {
                    let cut = runtime
                        .v1()?
                        .writer
                        .lock()
                        .expect("native snapshot writer")
                        .capture_pack_root()
                        .map_err(map)?;
                    let changed = output.catch_up(cut).map_err(map)?;
                    Ok((output, changed))
                })
                .await?;
            output = next;
            if changed <= 256 {
                break;
            }
            tokio::task::yield_now().await;
        }
        Ok(output)
    }
}
