use super::{
    hash,
    inventory::{InventoryProof, InventoryRoot},
    limits, Begin, ClientDescriptor, Job, Version,
};
use crate::{DecimalU64, Digest, Id, PendingReason, ProtocolError, TerminalFailure};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Head {
    pub commit_id: Id,
    pub commit_sha256: Digest,
    pub ordinal: DecimalU64,
}
impl Head {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.ordinal.get() == 0 {
            return Err(ProtocolError::invalid("v2.head.ordinal"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Commit {
    pub version: Version,
    pub job: Job,
    pub begin_sha256: Digest,
    pub root_receipt_id: Digest,
    pub inventory_root: InventoryRoot,
}
impl Commit {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        self.inventory_root.validate()?;
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
    pub fn input_sha256(&self) -> Result<Digest, ProtocolError> {
        self.validate()?;
        hash::input("lastdb:cloud-byte-backup:commit:v2", self)
    }
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
    pub version: Version,
    pub begin: Begin,
    pub commit_sha256: Digest,
    pub ordinal: DecimalU64,
    pub root_receipt_id: Digest,
    pub inventory_root: InventoryRoot,
}
impl AcceptedBackup {
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

    pub fn job(&self) -> &Job {
        &self.begin.job
    }

    pub fn descriptor(&self) -> &ClientDescriptor {
        &self.begin.descriptor
    }

    pub fn begin_sha256(&self) -> Result<Digest, ProtocolError> {
        self.begin.input_sha256()
    }

    pub fn commit_input(&self) -> Result<Commit, ProtocolError> {
        Ok(Commit {
            version: self.version,
            job: self.job().clone(),
            begin_sha256: self.begin_sha256()?,
            root_receipt_id: self.root_receipt_id.clone(),
            inventory_root: self.inventory_root.clone(),
        })
    }
    pub fn head(&self) -> Head {
        Head {
            commit_id: self.job().commit_id.clone(),
            commit_sha256: self.commit_sha256.clone(),
            ordinal: self.ordinal,
        }
    }
    pub fn validate_for(&self, begin: &Begin, commit: &Commit) -> Result<(), ProtocolError> {
        self.validate()?;
        commit.validate_for(begin)?;
        if self.begin != *begin || self.commit_input()? != *commit {
            return Err(ProtocolError::invalid("v2.accepted.input"));
        }
        Ok(())
    }
    pub fn validate_inventory(&self, proof: &InventoryProof) -> Result<(), ProtocolError> {
        self.validate()?;
        self.commit_input()?.validate_inventory(proof)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitStatusRequest {
    pub version: Version,
    pub job: Job,
}
impl CommitStatusRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum CommitResponse {
    Pending {
        version: Version,
        job: Job,
        begin_sha256: Digest,
        commit_sha256: Digest,
        reason: PendingReason,
    },
    Accepted {
        backup: Box<AcceptedBackup>,
    },
    Rejected {
        version: Version,
        job: Job,
        begin_sha256: Digest,
        commit_sha256: Digest,
        failure: TerminalFailure,
    },
}
impl CommitResponse {
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
