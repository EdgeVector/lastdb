//! Public backup job lifecycle. Provider effects remain in private worker records.

use crate::publication::{AcceptedBackup, BackupHead};
use crate::{
    BackupJob, DecimalU64, Digest, NativeRoot, ProtocolError, RecoveryDescriptor, Version,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobBeginInput {
    pub version: Version,
    pub job: BackupJob,
    pub native_root: NativeRoot,
    pub recovery: RecoveryDescriptor,
    pub predecessor: Option<BackupHead>,
}

impl JobBeginInput {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        self.native_root.validate()?;
        self.recovery.validate_for(&self.job.stream.scope)?;
        if self.native_root.home_id != self.job.stream.source_home {
            return Err(ProtocolError::invalid("job_begin.native_root"));
        }
        if let Some(predecessor) = &self.predecessor {
            predecessor.validate()?;
            if predecessor.commit_id == self.job.commit_id || predecessor.ordinal.get() == u64::MAX
            {
                return Err(ProtocolError::invalid("job_begin.predecessor"));
            }
        }
        crate::limits::encoded_json_bytes(self, crate::limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }

    pub fn input_sha256(&self) -> Result<Digest, ProtocolError> {
        crate::input_digest("lastdb:native-backup:job-begin:v1", self)
    }

    pub fn ordinal(&self) -> Result<DecimalU64, ProtocolError> {
        let prior = self
            .predecessor
            .as_ref()
            .map_or(0, |head| head.ordinal.get());
        prior
            .checked_add(1)
            .map(DecimalU64::new)
            .ok_or_else(|| ProtocolError::invalid("job_begin.ordinal"))
    }

    pub fn validate_initial(&self) -> Result<(), ProtocolError> {
        self.validate()?;
        if self.predecessor.is_some() {
            return Err(ProtocolError::invalid("job_begin.initial.predecessor"));
        }
        Ok(())
    }

    /// The acceptance transaction checks this same predecessor and native cut.
    pub fn validate_successor(&self, previous: &AcceptedBackup) -> Result<(), ProtocolError> {
        self.validate()?;
        previous.validate()?;
        if self.predecessor.as_ref() != Some(&previous.head())
            || self.job.stream != previous.job.stream
            || self.native_root.home_digest != previous.native_root.home_digest
            || self.native_root.generation < previous.native_root.generation
            || self.native_root.frontier < previous.native_root.frontier
            || (self.native_root.generation == previous.native_root.generation
                && self.native_root != previous.native_root)
        {
            return Err(ProtocolError::invalid("job_begin.successor"));
        }
        Ok(())
    }
}

pub type JobBeginRequest = JobBeginInput;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloseReason {
    UserRequested,
    SourceUnavailable,
    GenerationChanged,
}

/// Closure fences acceptance. It does not assert that an old provider write failed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum JobStatus {
    Active,
    Published { backup: Box<AcceptedBackup> },
    Closed { reason: CloseReason },
}

/// The saved input hash is returned even when a recovery caller lost its local job.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobResult {
    pub version: Version,
    pub job: BackupJob,
    pub job_input_sha256: Digest,
    pub status: JobStatus,
}

impl JobResult {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        if let JobStatus::Published { backup } = &self.status {
            backup.validate()?;
            if backup.job != self.job || backup.job_input_sha256 != self.job_input_sha256 {
                return Err(ProtocolError::invalid("job_result.published"));
            }
        }
        crate::limits::encoded_json_bytes(self, crate::limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }

    pub fn validate_for_begin(&self, input: &JobBeginInput) -> Result<(), ProtocolError> {
        input.validate()?;
        self.validate_identity(&input.job, &input.input_sha256()?)?;
        if let JobStatus::Published { backup } = &self.status {
            backup.validate_for(input, &backup.commit_input())?;
        }
        Ok(())
    }

    pub fn validate_for_status(&self, request: &JobStatusRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        self.validate()?;
        if self.job != request.job {
            return Err(ProtocolError::invalid("job_result.job"));
        }
        Ok(())
    }

    /// A prior accepted result wins a close race and remains the original result.
    pub fn validate_for_close(&self, request: &JobCloseRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        self.validate_identity(&request.job, &request.job_input_sha256)?;
        if matches!(self.status, JobStatus::Active) {
            return Err(ProtocolError::invalid("job_close.result"));
        }
        Ok(())
    }

    fn validate_identity(&self, job: &BackupJob, input: &Digest) -> Result<(), ProtocolError> {
        self.validate()?;
        if &self.job != job || &self.job_input_sha256 != input {
            return Err(ProtocolError::invalid("job_result.identity"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobStatusRequest {
    pub version: Version,
    pub job: BackupJob,
}

impl JobStatusRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        crate::limits::encoded_json_bytes(self, crate::limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobCloseRequest {
    pub version: Version,
    pub job: BackupJob,
    pub job_input_sha256: Digest,
    pub reason: CloseReason,
}

impl JobCloseRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        crate::limits::encoded_json_bytes(self, crate::limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

// Typed ProtocolError is the error body for these operations.
pub type JobBeginResult = JobResult;
pub type JobStatusResult = JobResult;
pub type JobCloseResult = JobResult;
