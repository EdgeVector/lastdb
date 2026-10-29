//! Commit inputs, accepted backups, and publication replies for one stream.

use super::{
    hash,
    inventory::{InventoryProof, InventoryRoot},
    limits, Begin, ClientDescriptor, Job, Version,
};
use crate::{DecimalU64, Digest, Id, PendingReason, ProtocolError, TerminalFailure};
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Publish request: the begin digest, the root receipt, and the inventory summary.
pub struct Commit {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Stream and commit id this value belongs to.
    pub job: Job,
    /// SHA-256 of the Begin value.
    pub begin_sha256: Digest,
    /// Receipt id of the inventory root object.
    pub root_receipt_id: Digest,
    /// Page, object, and stored-byte summary of the inventory index.
    pub inventory_root: InventoryRoot,
}
impl Commit {
    /// Validate the job and inventory root, and reject oversized JSON.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        self.inventory_root.validate()?;
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
    /// Domain-separated SHA-256 of this commit, after validation.
    pub fn input_sha256(&self) -> Result<Digest, ProtocolError> {
        self.validate()?;
        hash::input("lastdb:cloud-byte-backup:commit:v2", self)
    }
    /// Require this commit's job and begin digest to match `begin`.
    pub fn validate_for(&self, begin: &Begin) -> Result<(), ProtocolError> {
        self.validate()?;
        if self.job != begin.job || self.begin_sha256 != begin.input_sha256()? {
            return Err(ProtocolError::invalid("v2.commit.begin"));
        }
        Ok(())
    }
    /// Pure concrete-inventory proof only. Service verified-receipt lookups and
    /// atomic authority, quota, job and predecessor checks remain mandatory.
    pub fn validate_inventory(&self, proof: &InventoryProof) -> Result<(), ProtocolError> {
        self.validate()?;
        proof.validate_for(
            &self.job.stream.scope,
            &self.inventory_root,
            &self.root_receipt_id,
        )
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
        self.begin.validate()?;
        self.head().validate()?;
        if self.ordinal != self.begin.ordinal()?
            || self.commit_sha256 != self.commit_input()?.input_sha256()?
        {
            return Err(ProtocolError::invalid("v2.accepted.commit"));
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }

    /// Job from the stored begin value.
    pub fn job(&self) -> &Job {
        &self.begin.job
    }

    /// Client descriptor from the stored begin value.
    pub fn descriptor(&self) -> &ClientDescriptor {
        &self.begin.descriptor
    }

    /// SHA-256 of the stored begin value.
    pub fn begin_sha256(&self) -> Result<Digest, ProtocolError> {
        self.begin.input_sha256()
    }

    /// Rebuild the commit input from this backup's begin digest, root receipt, and inventory summary.
    pub fn commit_input(&self) -> Result<Commit, ProtocolError> {
        Ok(Commit {
            version: self.version,
            job: self.job().clone(),
            begin_sha256: self.begin_sha256()?,
            root_receipt_id: self.root_receipt_id.clone(),
            inventory_root: self.inventory_root.clone(),
        })
    }
    /// Head formed from this backup's commit id, commit digest, and ordinal.
    pub fn head(&self) -> Head {
        Head {
            commit_id: self.job().commit_id.clone(),
            commit_sha256: self.commit_sha256.clone(),
            ordinal: self.ordinal,
        }
    }
    /// Require this backup's begin and rebuilt commit to equal the provided values.
    pub fn validate_for(&self, begin: &Begin, commit: &Commit) -> Result<(), ProtocolError> {
        self.validate()?;
        commit.validate_for(begin)?;
        if self.begin != *begin || self.commit_input()? != *commit {
            return Err(ProtocolError::invalid("v2.accepted.input"));
        }
        Ok(())
    }
    /// Check `proof` against this backup's scope, inventory root, and root receipt id.
    pub fn validate_inventory(&self, proof: &InventoryProof) -> Result<(), ProtocolError> {
        self.validate()?;
        self.commit_input()?.validate_inventory(proof)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Ask for the publication state of one job.
pub struct CommitStatusRequest {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Stream and commit id this value belongs to.
    pub job: Job,
}
impl CommitStatusRequest {
    /// Validate the job and the encoded size.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
/// Publication is pending, accepted, or rejected for one commit input.
pub enum CommitResponse {
    /// The commit is not decided yet.
    Pending {
        /// Protocol version. V2 serializes this field as the JSON integer 2.
        version: Version,
        /// Stream and commit id this value belongs to.
        job: Job,
        /// SHA-256 of the Begin value.
        begin_sha256: Digest,
        /// SHA-256 of the commit input.
        commit_sha256: Digest,
        /// Why the commit is still pending.
        reason: PendingReason,
    },
    /// The commit was accepted.
    Accepted {
        /// Backup recorded for this commit.
        backup: Box<AcceptedBackup>,
    },
    /// The commit was rejected and will not succeed with the same input.
    Rejected {
        /// Protocol version. V2 serializes this field as the JSON integer 2.
        version: Version,
        /// Stream and commit id this value belongs to.
        job: Job,
        /// SHA-256 of the Begin value.
        begin_sha256: Digest,
        /// SHA-256 of the commit input.
        commit_sha256: Digest,
        /// Why this commit cannot succeed.
        failure: TerminalFailure,
    },
}
impl CommitResponse {
    /// Validate the accepted backup, or the pending or rejected job, and the encoded size.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::Accepted { backup } => backup.validate()?,
            Self::Pending { job, .. } => job.validate()?,
            Self::Rejected { job, failure, .. } => {
                job.validate()?;
                limits::encoded(failure, limits::CONTROL_JSON_MAX_BYTES)?;
            }
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
    /// Match an accepted backup, or the pending or rejected ids and digests, to this begin and commit.
    pub fn validate_for(&self, begin: &Begin, commit: &Commit) -> Result<(), ProtocolError> {
        self.validate()?;
        commit.validate_for(begin)?;
        match self {
            Self::Accepted { backup } => backup.validate_for(begin, commit),
            Self::Pending {
                job,
                begin_sha256,
                commit_sha256,
                ..
            }
            | Self::Rejected {
                job,
                begin_sha256,
                commit_sha256,
                ..
            } => {
                if job != &commit.job
                    || begin_sha256 != &commit.begin_sha256
                    || commit_sha256 != &commit.input_sha256()?
                {
                    return Err(ProtocolError::invalid("v2.commit_response.input"));
                }
                Ok(())
            }
        }
    }
    /// Require the response job to be the status request's job.
    pub fn validate_for_status(&self, request: &CommitStatusRequest) -> Result<(), ProtocolError> {
        self.validate()?;
        request.validate()?;
        let job = match self {
            Self::Accepted { backup } => backup.job(),
            Self::Pending { job, .. } | Self::Rejected { job, .. } => job,
        };
        if job != &request.job {
            return Err(ProtocolError::invalid("v2.commit_status.job"));
        }
        Ok(())
    }
}

mod heads;
pub use heads::*;
