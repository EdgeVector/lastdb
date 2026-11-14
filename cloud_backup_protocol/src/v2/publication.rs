//! Stream heads and accepted backups for one stream.

use super::{hash, inventory::InventoryRoot, limits, Begin, Job, Version};
use crate::{DecimalU64, Digest, Id, ProtocolError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Identity of one accepted commit: id, input digest, and ordinal.
pub struct Head {
    /// Commit id within the stream.
    pub commit_id: Id,
    /// SHA-256 of the commit input.
    pub commit_sha256: Digest,
    /// 1-based commit position on the stream. Zero is rejected.
    pub ordinal: DecimalU64,
}
impl Head {
    /// Reject ordinal zero.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.ordinal.get() == 0 {
            return Err(ProtocolError::invalid("v2.head.ordinal"));
        }
        Ok(())
    }
}

/// This immutable record is returned only from the service's accepted store.
/// A caller can compute consistent hashes without owning stored-byte authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedBackup {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Begin value that was accepted with this backup.
    pub begin: Begin,
    /// SHA-256 of the commit input.
    pub commit_sha256: Digest,
    /// 1-based commit position on the stream. Zero is rejected.
    pub ordinal: DecimalU64,
    /// Receipt id of the inventory root object.
    pub root_receipt_id: Digest,
    /// Page, object, and stored-byte summary of the inventory index.
    pub inventory_root: InventoryRoot,
}
impl AcceptedBackup {
    /// Require the ordinal and commit digest to match the begin value and the rebuilt commit input.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        #[derive(Serialize)]
        struct CommitInput<'a> {
            version: Version,
            job: &'a Job,
            begin_sha256: Digest,
            root_receipt_id: &'a Digest,
            inventory_root: &'a InventoryRoot,
        }

        self.begin.validate()?;
        self.head().validate()?;
        self.inventory_root.validate()?;
        let expected_ordinal = self
            .begin
            .predecessor
            .as_ref()
            .map_or(0, |head| head.ordinal.get())
            .checked_add(1)
            .map(DecimalU64::new)
            .ok_or_else(|| ProtocolError::invalid("v2.begin.ordinal"))?;
        let commit_input = CommitInput {
            version: self.version,
            job: self.job(),
            begin_sha256: self.begin_sha256()?,
            root_receipt_id: &self.root_receipt_id,
            inventory_root: &self.inventory_root,
        };
        limits::encoded(&commit_input, limits::CONTROL_JSON_MAX_BYTES)?;
        let expected_commit_sha256 =
            hash::input("lastdb:cloud-byte-backup:commit:v2", &commit_input)?;
        if self.ordinal != expected_ordinal || self.commit_sha256 != expected_commit_sha256 {
            return Err(ProtocolError::invalid("v2.accepted.commit"));
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }

    /// Job from the stored begin value.
    #[must_use]
    pub fn job(&self) -> &Job {
        &self.begin.job
    }

    /// SHA-256 of the stored begin value.
    pub fn begin_sha256(&self) -> Result<Digest, ProtocolError> {
        self.begin.input_sha256()
    }

    /// Head formed from this backup's commit id, commit digest, and ordinal.
    #[must_use]
    pub fn head(&self) -> Head {
        Head {
            commit_id: self.job().commit_id.clone(),
            commit_sha256: self.commit_sha256.clone(),
            ordinal: self.ordinal,
        }
    }
}
