//! Upload position of the spool. It is part of the spool's state row, so the position and the
//! acknowledgement it confirms are one write.

use super::Error;
use cloud_backup_protocol::CloudScope;
use serde::{Deserialize, Serialize};

/// Identity of the destination this home publishes to.
#[derive(Clone)]
pub struct UploadIdentity {
    /// Account and generation admitted by the cloud.
    pub scope: CloudScope,
    /// Cloud environment selected by the saved connection.
    pub environment: String,
    /// Database identifier in that scope.
    pub database_id: String,
    /// Device assigning sequence numbers to these edits.
    pub device_id: String,
    /// Signing identity of those cloud entries.
    pub writer_key: String,
}

/// The confirmed local coordinates of an upload.
#[derive(Clone, Copy)]
pub struct UploadPosition {
    /// Highest writer sequence known to have been saved for this device.
    pub writer_seq: u64,
    /// Accepted spool position confirmed or covered locally.
    pub spool_through: u64,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::storage::disk_backup::accepted) struct Checkpoint {
    scope: CloudScope,
    environment: String,
    database_id: String,
    device_id: String,
    writer_key: String,
    writer_seq: u64,
    spool_through: u64,
}

impl Checkpoint {
    pub(super) fn new(identity: &UploadIdentity, position: UploadPosition) -> Self {
        Self {
            scope: identity.scope.clone(),
            environment: identity.environment.clone(),
            database_id: identity.database_id.clone(),
            device_id: identity.device_id.clone(),
            writer_key: identity.writer_key.clone(),
            writer_seq: position.writer_seq,
            spool_through: position.spool_through,
        }
    }

    pub(super) fn writer_seq(&self) -> u64 {
        self.writer_seq
    }

    /// The position, when this checkpoint belongs to `identity`.
    pub(super) fn position(&self, identity: &UploadIdentity) -> Result<UploadPosition, Error> {
        if self.writer_seq == u64::MAX {
            return Err(Error::Gap);
        }
        if self.scope != identity.scope
            || self.environment != identity.environment
            || self.database_id != identity.database_id
            || self.device_id != identity.device_id
            || self.writer_key != identity.writer_key
        {
            return Err(Error::Conflict);
        }
        Ok(UploadPosition {
            writer_seq: self.writer_seq,
            spool_through: self.spool_through,
        })
    }
}
