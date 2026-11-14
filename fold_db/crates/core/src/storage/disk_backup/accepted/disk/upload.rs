//! Durable upload progress paired with the local accepted spool.

use super::{io, Error, Log};
use cloud_backup_protocol::CloudScope;
use serde::{Deserialize, Serialize};
use std::path::Path;

const FILE: &str = "cloud-upload.json";

/// Identity of the destination this home publishes to.
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

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    version: u8,
    scope: CloudScope,
    environment: String,
    database_id: String,
    device_id: String,
    writer_key: String,
    writer_seq: u64,
    spool_through: u64,
}

impl Checkpoint {
    fn read(root: &Path, identity: &UploadIdentity) -> Result<Option<UploadPosition>, Error> {
        let Some(saved) = io::read_json::<Self>(&root.join(FILE))? else {
            return Ok(None);
        };
        if saved.version != 1 || saved.writer_seq == u64::MAX {
            return Err(Error::Gap);
        }
        if saved.scope != identity.scope
            || saved.environment != identity.environment
            || saved.database_id != identity.database_id
            || saved.device_id != identity.device_id
            || saved.writer_key != identity.writer_key
        {
            return Err(Error::Conflict);
        }
        Ok(Some(UploadPosition {
            writer_seq: saved.writer_seq,
            spool_through: saved.spool_through,
        }))
    }

    fn write(
        root: &Path,
        identity: &UploadIdentity,
        position: UploadPosition,
    ) -> Result<(), Error> {
        let checkpoint = Self {
            version: 1,
            scope: identity.scope.clone(),
            environment: identity.environment.clone(),
            database_id: identity.database_id.clone(),
            device_id: identity.device_id.clone(),
            writer_key: identity.writer_key.clone(),
            writer_seq: position.writer_seq,
            spool_through: position.spool_through,
        };
        io::write_json(&root.join(FILE), &checkpoint)
    }
}

impl Log {
    pub(in crate::storage) fn upload_position(
        &self,
        identity: &UploadIdentity,
    ) -> Result<Option<UploadPosition>, Error> {
        let mut inner = self.lock()?;
        inner.refuse_gap()?;
        let Some(position) = Checkpoint::read(&self.root, identity)? else {
            return Ok(None);
        };
        let status = inner.status();
        if position.spool_through < status.acked {
            // A snapshot or an earlier client may have acknowledged writes
            // without saving a writer position. Reconcile before allocating.
            return Ok(None);
        }
        if position.spool_through > status.acked {
            // The cloud confirmation was stored before an interrupted local ack.
            inner.acknowledge(&self.root, position.spool_through)?;
        }
        Ok(Some(position))
    }

    pub(in crate::storage) fn confirm_upload(
        &self,
        identity: &UploadIdentity,
        after: u64,
        position: UploadPosition,
    ) -> Result<(), Error> {
        let mut inner = self.lock()?;
        inner.refuse_gap()?;
        let status = inner.status();
        if after != status.acked || position.spool_through < after {
            return Err(Error::Behind);
        }
        if position.spool_through > status.head || position.writer_seq == u64::MAX {
            return Err(Error::Ahead);
        }
        if status.snapshot_required {
            return Err(Error::Snapshot);
        }
        if let Some(previous) = Checkpoint::read(&self.root, identity)? {
            if previous.spool_through > after || previous.writer_seq > position.writer_seq {
                return Err(Error::Conflict);
            }
        }
        // Writing the confirmation first means a crash never discards a
        // pending entry with no durable record of its cloud outcome. The
        // spool's normal acknowledgement retires the already-confirmed bytes.
        Checkpoint::write(&self.root, identity, position)?;
        inner.acknowledge(&self.root, position.spool_through)
    }
}
