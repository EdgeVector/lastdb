//! Read the saved selection without acquiring a source hold or cloud admission.
use super::{invalid, store::LocalStore, PackRuntime, StorageResult};

impl PackRuntime {
    pub fn native_home_id(&self) -> uuid::Uuid {
        self.home.descriptor().identity
    }

    pub fn native_backup_capture_status(
        &self,
    ) -> StorageResult<Option<super::NativeBackupCaptureStatus>> {
        let root = self.home.path().join("cloud-export-v1");
        match std::fs::symlink_metadata(&root) {
            Ok(meta) if meta.is_dir() => {}
            Ok(_) => return Err(invalid("backup capture sidecar directory")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        }
        let store = LocalStore::open(self.home.path())?;
        let Some(record) = store.load()? else {
            return Ok(None);
        };
        let mut status = record.status(0);
        status.spool_bytes = store.spool_bytes(&record)?;
        status.last_accepted_backup = store.accepted()?;
        Ok(Some(status))
    }
}
