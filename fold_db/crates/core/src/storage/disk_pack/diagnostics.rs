//! Explicit isolated-copy storage checkpoints; never part of a normal read or write.
use super::*;
use laststore::disk_run::home::RunHomeDiagnostics;

#[derive(serde::Serialize)]
pub struct NativeStorageDiagnostics {
    pub main: RunHomeDiagnostics,
    pub cas: RunHomeDiagnostics,
}

impl PackRuntime {
    /// Physical totals are for independently captured home roots. Inventory is
    /// checkpoint-only: the caller must stop its load before requesting it.
    /// All CPU and file work stays off the async executor, including inventory.
    pub async fn diagnostics_native(
        &self,
        include_inventory: bool,
    ) -> StorageResult<Option<NativeStorageDiagnostics>> {
        if !laststore::commit_timing::enabled() {
            return Ok(None);
        }
        if include_inventory
            && self
                .queue
                .lock()
                .expect("native diagnostic queue")
                .request_count()
                > 0
        {
            return Err(StorageError::InvalidOperation(
                "storage inventory requires a checkpoint without accepted writes".into(),
            ));
        }
        let main = Arc::clone(&self.physical.home);
        let cas = Arc::clone(&self.physical.cas);
        tokio::task::spawn_blocking(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
            let main_cut = main.diagnostic_checkpoint(deadline).map_err(map)?;
            let cas_cut = cas.diagnostic_checkpoint(deadline).map_err(map)?;
            Ok(Some(NativeStorageDiagnostics {
                main: main_cut.finish(&main, include_inventory, deadline),
                cas: cas_cut.finish(&cas, include_inventory, deadline),
            }))
        })
        .await?
    }
}
