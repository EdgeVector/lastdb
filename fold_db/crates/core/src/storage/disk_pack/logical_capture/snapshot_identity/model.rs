//! Inactive preparation retains original anchors and introduces explicit bases.

use super::*;
use cloud_backup_protocol::{DecimalU64, NativeRoot};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotIdentityPosition {
    pub sequence: DecimalU64,
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SnapshotIdentityPeer {
    pub origin: String,
    pub anchor_sha256: String,
    pub base: SnapshotIdentityPosition,
    pub position: SnapshotIdentityPosition,
    pub retained_receipts: DecimalU64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SnapshotIdentityReport {
    pub version: u8,
    pub prepared: bool,
    pub startup_allowed: bool,
    pub replacement_allowed: bool,
    pub source_root: NativeRoot,
    pub candidate_root: NativeRoot,
    pub prepared_root: NativeRoot,
    pub local_origin: String,
    pub anchor_sha256: String,
    pub authored_base: SnapshotIdentityPosition,
    pub peers: Vec<SnapshotIdentityPeer>,
    pub logical_inclusion: SnapshotInclusion,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PreparedHead {
    pub version: u8,
    pub origin: String,
    pub birth_main: uuid::Uuid,
    pub birth_cas: uuid::Uuid,
    pub anchor_sha256: String,
    pub base: SnapshotIdentityPosition,
    pub position: SnapshotIdentityPosition,
    pub encoded_backlog_bytes: DecimalU64,
    pub authored_key_version: u8,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PreparedPeer {
    pub version: u8,
    pub origin: String,
    pub anchor_sha256: String,
    pub base: SnapshotIdentityPosition,
    pub position: SnapshotIdentityPosition,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PreparedRoster {
    pub version: u8,
    pub origins: Vec<String>,
    pub receipt_count: DecimalU64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Preparation {
    pub version: u8,
    pub source_root: NativeRoot,
    pub candidate_root: NativeRoot,
    pub source_map_sha256: String,
    pub candidate_map_sha256: String,
    pub local_origin: String,
    pub signature: String,
}

impl SnapshotIdentityPosition {
    pub(super) fn of(position: &SnapshotDevicePosition) -> Self {
        Self {
            sequence: position.sequence,
            sha256: position.sha256.clone(),
        }
    }

    pub(in crate::storage::disk_pack::logical_capture) fn validate_for(
        &self,
        position: &SnapshotDevicePosition,
    ) -> StorageResult<()> {
        unhex::<32>(&self.sha256)?;
        if self.sequence.get() > position.sequence.get()
            || (self.sequence == position.sequence && self.sha256 != position.sha256)
            || (self.sequence.get() == 0 && self.sha256 != position.anchor_sha256)
        {
            return Err(invalid(
                "snapshot retained base differs from its original chain",
            ));
        }
        Ok(())
    }
}
