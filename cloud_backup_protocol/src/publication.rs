//! One control-store transaction accepts the backup and updates its sole head.

use crate::jobs::JobBeginInput;
use crate::{
    BackupJob, DecimalU64, Digest, Id, NativeRoot, ProtocolError, RecoveryDescriptor,
    ReferenceRoot, Version,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupHead {
    pub commit_id: Id,
    pub commit_sha256: Digest,
    pub ordinal: DecimalU64,
}

impl BackupHead {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.ordinal.get() == 0 {
            return Err(ProtocolError::invalid("backup_head.ordinal"));
        }
        Ok(())
    }
}

/// An immutable submission to one existing active job.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitInput {
    pub version: Version,
    pub job: BackupJob,
    pub job_input_sha256: Digest,
    pub reference_root: ReferenceRoot,
    pub encrypted_manifest_receipt_id: Digest,
}

impl CommitInput {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        self.reference_root.validate()?;
        crate::limits::encoded_json_bytes(self, crate::limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }

    pub fn input_sha256(&self) -> Result<Digest, ProtocolError> {
        crate::input_digest("lastdb:native-backup:commit:v1", self)
    }

    pub fn validate_for(&self, begin: &JobBeginInput) -> Result<(), ProtocolError> {
        self.validate()?;
        begin.validate()?;
        if self.job != begin.job || self.job_input_sha256 != begin.input_sha256()? {
            return Err(ProtocolError::invalid("commit.job_input"));
        }
        Ok(())
    }
}

/// The immutable accepted record is also the commit receipt. No second head exists.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedBackup {
    pub version: Version,
    pub job: BackupJob,
    pub job_input_sha256: Digest,
    pub commit_sha256: Digest,
    pub ordinal: DecimalU64,
    pub native_root: NativeRoot,
    pub recovery: RecoveryDescriptor,
    pub reference_root: ReferenceRoot,
    pub encrypted_manifest_receipt_id: Digest,
}

impl AcceptedBackup {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        self.native_root.validate()?;
        self.recovery.validate_for(&self.job.stream.scope)?;
        self.reference_root.validate()?;
        if self.ordinal.get() == 0
            || self.native_root.home_id != self.job.stream.source_home
            || self.commit_sha256 != self.commit_input().input_sha256()?
        {
            return Err(ProtocolError::invalid("accepted_backup"));
        }
        crate::limits::encoded_json_bytes(self, crate::limits::RECEIPT_JSON_MAX_BYTES).map(|_| ())
    }

    /// Full begin linkage requires the authoritative frozen job input.
    pub fn validate_for(
        &self,
        begin: &JobBeginInput,
        commit: &CommitInput,
    ) -> Result<(), ProtocolError> {
        self.validate()?;
        commit.validate_for(begin)?;
        if self.commit_input() != *commit
            || self.native_root != begin.native_root
            || self.recovery != begin.recovery
            || self.ordinal != begin.ordinal()?
        {
            return Err(ProtocolError::invalid("accepted_backup.input"));
        }
        Ok(())
    }

    #[must_use]
    pub fn head(&self) -> BackupHead {
        BackupHead {
            commit_id: self.job.commit_id.clone(),
            commit_sha256: self.commit_sha256.clone(),
            ordinal: self.ordinal,
        }
    }

    #[must_use]
    pub fn commit_input(&self) -> CommitInput {
        CommitInput {
            version: self.version,
            job: self.job.clone(),
            job_input_sha256: self.job_input_sha256.clone(),
            reference_root: self.reference_root.clone(),
            encrypted_manifest_receipt_id: self.encrypted_manifest_receipt_id.clone(),
        }
    }
}

mod api;
pub use api::*;
