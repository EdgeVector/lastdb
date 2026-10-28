//! Both work kinds use one serialized transfer, inventory and publication path.
use super::{log_capture::LogCapture, BackupError, Result};
use cloud_backup_protocol::v2::{jobs::JobResult, transfer::ObjectReceipt, Begin, Commit};
use cloud_backup_protocol::Digest;
use fold_db::storage::disk_pack::byte_backup::{
    ByteBackupCapture, ByteBackupCaptureStatus, ByteBackupObject,
};
use std::io::Read;

pub(super) enum Capture {
    Snapshot(ByteBackupCapture),
    Log(LogCapture),
}
impl Capture {
    pub fn begin(&self) -> Result<Begin> {
        match self {
            Self::Snapshot(c) => Ok(c.begin()?),
            Self::Log(c) => c.begin(),
        }
    }
    pub fn object_count(&self) -> Result<u64> {
        match self {
            Self::Snapshot(c) => Ok(c.object_count()?),
            Self::Log(c) => c.object_count(),
        }
    }
    pub fn object(&self, index: u64) -> Result<ByteBackupObject> {
        match self {
            Self::Snapshot(c) => Ok(c.object(index)?),
            Self::Log(c) => c.object(index),
        }
    }
    pub fn reader(&self, index: u64) -> Result<Box<dyn Read + Send>> {
        match self {
            Self::Snapshot(c) => Ok(Box::new(c.reader(index)?)),
            Self::Log(c) => c.reader(index),
        }
    }
    pub fn confirm(&self, index: u64, receipt: &ObjectReceipt) -> Result<()> {
        match self {
            Self::Snapshot(c) => Ok(c.confirm(index, receipt)?),
            Self::Log(c) => c.confirm(index, receipt),
        }
    }
    pub fn root_receipt_id(&self) -> Result<Digest> {
        match self {
            Self::Snapshot(c) => Ok(c.root_receipt_id()?),
            Self::Log(c) => c.root_receipt_id(),
        }
    }
    pub fn status(&self) -> Result<ByteBackupCaptureStatus> {
        match self {
            Self::Snapshot(c) => Ok(c.status()?),
            Self::Log(c) => c.status(),
        }
    }
    pub fn recommended_parallelism(&self) -> Result<usize> {
        Ok(lastdb_storage_policy::BACKUP_RESOURCES.concurrent_streams)
    }
    pub fn set_coordinator_spool_bytes(&self, bytes: u64) -> Result<()> {
        match self {
            Self::Snapshot(c) => Ok(c.set_coordinator_spool_bytes(bytes)?),
            Self::Log(c) => c.budget(bytes),
        }
    }
    pub fn seal_commit(&self, commit: &Commit) -> Result<()> {
        match self {
            Self::Snapshot(c) => {
                if c.root_receipt_id()? != commit.root_receipt_id {
                    return Err(BackupError::Identity);
                }
                Ok(())
            }
            Self::Log(c) => c.seal_commit(commit),
        }
    }
    pub fn terminal(&self, result: &JobResult) -> Result<()> {
        match self {
            Self::Snapshot(c) => Ok(c.terminal(result)?),
            Self::Log(c) => c.terminal(result),
        }
    }
}
