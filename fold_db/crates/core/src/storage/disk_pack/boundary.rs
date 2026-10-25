//! Durable logical change signal for native backup, independent of compaction.

use super::{PackRuntime, StorageError, StorageResult};

/// The native home and its last completely published logical frontier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CoreNativeBackupBoundary {
    pub source_home: uuid::Uuid,
    pub frontier: u64,
}

impl PackRuntime {
    /// Read the complete disk boundary without a flush, record read or capture.
    pub fn native_backup_boundary(&self) -> StorageResult<CoreNativeBackupBoundary> {
        let view = self.view.read().map_err(|_| {
            StorageError::BackendError("native backup boundary view lock poisoned".into())
        })?;
        Ok(CoreNativeBackupBoundary {
            source_home: self.home_identity(),
            frontier: view.current.frontier(),
        })
    }

    /// Subscribe before the first boundary read. The watch is only a wake signal;
    /// the getter and validated accepted backup determine actual coverage.
    /// Drop any borrowed watch value before awaiting another operation.
    pub fn subscribe_native_backup_boundary(
        &self,
    ) -> tokio::sync::watch::Receiver<CoreNativeBackupBoundary> {
        self.native_backup_boundary.subscribe()
    }

    /// Called after the view lock drops. A delayed notification cannot replace
    /// a newer frontier, and unchanged compaction roots produce no wake.
    pub(super) fn publish_native_backup_boundary(&self, frontier: u64) {
        self.native_backup_boundary.send_if_modified(|boundary| {
            if frontier <= boundary.frontier {
                return false;
            }
            boundary.frontier = frontier;
            true
        });
    }
}
