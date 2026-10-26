//! Local-only release requires the owner's durable proof of no remote submission.
use super::{PackRuntime, StorageResult};
use cloud_backup_protocol::v2::Begin;
use std::sync::Arc;

impl PackRuntime {
    /// The caller must hold its owner lock and prove begin_submitted=false from
    /// durable state. This is never evidence for a submitted or unknown cloud job.
    /// No capture occurs merely to cancel unused local work.
    pub async fn cancel_unsubmitted_byte_backup_capture(
        self: &Arc<Self>,
        begin: &Begin,
        key: &[u8; 32],
    ) -> StorageResult<()> {
        super::run_capture::cancel(self, begin, key).await
    }
}
