//! Cut a local immutable backup without blocking later writes during file copy.

use super::*;
use laststore::disk_pack::backup::{prepare_referenced_backup, BackupReceipt};
use laststore::disk_pack::lifetime::{DurableReference, ReferencePurpose};
use std::path::PathBuf;

impl PackRuntime {
    /// Flush earlier accepted work, retain its complete root, then release the
    /// write gate before file copy. The bundle excludes node identity material.
    pub async fn backup_native(
        self: &Arc<Self>,
        destination: PathBuf,
    ) -> StorageResult<BackupReceipt> {
        self.flush().await?;
        let gate = Arc::clone(&self.write_gate).lock_owned().await;
        let runtime = Arc::clone(self);
        super::super::laststore::LastStoreKvStore::run_blocking(move || {
            runtime.require_initialized()?;
            if runtime.failed.load(Ordering::Acquire) {
                return Err(StorageError::BackendError(
                    "native backup requires writer recovery".into(),
                ));
            }
            let writer = runtime.journal.lock().expect("native backup journal");
            let reference = DurableReference::create(
                runtime.home.path(),
                writer.current(),
                ReferencePurpose::Backup,
                0,
            )
            .map_err(map)?;
            let prepared =
                prepare_referenced_backup(&runtime.home, reference, &destination).map_err(map)?;
            let backup = prepared
                .reserve(&writer, &runtime.space_policy)
                .map_err(map)?;
            drop(writer);
            drop(gate);
            backup.create().map_err(map)
        })
        .await
    }
}
