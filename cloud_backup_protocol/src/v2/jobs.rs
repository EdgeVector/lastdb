use super::{hash, limits, AcceptedBackup, ClientDescriptor, Destination, Head, Job, Version};
use crate::{DecimalU64, Digest, ProtocolError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Begin {
    pub version: Version,
    pub job: Job,
    pub descriptor: ClientDescriptor,
    /// An empty list preserves legacy admission and provider identities.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_destinations: Vec<Destination>,
    /// Explicit terminal-upload contract for new CI R2 allocations. Omission
    /// preserves legacy admission and hashes; false is never an opt-in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r2_multipart: Option<bool>,
    pub predecessor: Option<Head>,
    /// Exact recovery dependencies, separate from publication order. None is
    /// legacy and conservatively protects the predecessor chain. Some([])
    /// declares an independent recovery base. Emit only after capability admission.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_dependencies: Option<Vec<Head>>,
}
impl Begin {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        super::common::validate_destinations(&self.required_destinations)?;
        if self.r2_multipart == Some(false) {
            return Err(ProtocolError::invalid("v2.begin.r2_multipart"));
        }
        self.job.validate()?;
        self.descriptor.validate()?;
        if let Some(head) = &self.predecessor {
            head.validate()?;
            if head.commit_id == self.job.commit_id || head.ordinal.get() == u64::MAX {
                return Err(ProtocolError::invalid("v2.begin.predecessor"));
            }
        }
        self.validate_dependencies()?;
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }

    fn validate_dependencies(&self) -> Result<(), ProtocolError> {
        let Some(dependencies) = &self.recovery_dependencies else {
            return Ok(());
        };
        if dependencies.len() > 2 {
            return Err(ProtocolError::limit("v2.begin.recovery_dependencies"));
        }
        for (offset, dependency) in dependencies.iter().enumerate() {
            dependency.validate()?;
            if dependency.commit_id == self.job.commit_id
                || self
                    .predecessor
                    .as_ref()
                    .is_none_or(|head| dependency.ordinal > head.ordinal)
                || dependencies[..offset]
                    .iter()
                    .any(|prior| prior.commit_id == dependency.commit_id)
                || (offset > 0 && dependencies[offset - 1].ordinal >= dependency.ordinal)
            {
                return Err(ProtocolError::invalid("v2.begin.recovery_dependencies"));
            }
        }
        Ok(())
    }

    /// A retention planner must keep these exact accepted points recursively.
    /// Legacy omission never means an independent snapshot.
    pub fn dependencies(&self) -> &[Head] {
        self.recovery_dependencies
            .as_deref()
            .unwrap_or(self.predecessor.as_slice())
    }
    pub fn input_sha256(&self) -> Result<Digest, ProtocolError> {
        self.validate()?;
        hash::input("lastdb:cloud-byte-backup:begin:v2", self)
    }
    pub fn ordinal(&self) -> Result<DecimalU64, ProtocolError> {
        self.validate()?;
        self.predecessor
            .as_ref()
            .map_or(0, |head| head.ordinal.get())
            .checked_add(1)
            .map(DecimalU64::new)
            .ok_or_else(|| ProtocolError::invalid("v2.begin.ordinal"))
    }
}
pub type JobBeginRequest = Begin;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloseReason {
    UserRequested,
    SourceUnavailable,
    GenerationChanged,
}

/// Close fences acceptance and never proves that an older provider effect failed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum JobStatus {
    Active,
    Published { backup: Box<AcceptedBackup> },
    Closed { reason: CloseReason },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobResult {
    pub version: Version,
    pub job: Job,
    pub begin_sha256: Digest,
    pub status: JobStatus,
}
impl JobResult {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        if let JobStatus::Published { backup } = &self.status {
            backup.validate()?;
            if backup.job() != &self.job || backup.begin_sha256()? != self.begin_sha256 {
                return Err(ProtocolError::invalid("v2.job_result.published"));
            }
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
    pub fn validate_for_begin(&self, begin: &Begin) -> Result<(), ProtocolError> {
        self.validate()?;
        if self.job != begin.job || self.begin_sha256 != begin.input_sha256()? {
            return Err(ProtocolError::invalid("v2.job_result.begin"));
        }
        if let JobStatus::Published { backup } = &self.status {
            backup.validate_for(begin, &backup.commit_input()?)?;
        }
        Ok(())
    }
    pub fn validate_for_status(&self, request: &JobStatusRequest) -> Result<(), ProtocolError> {
        self.validate()?;
        request.validate()?;
        if self.job != request.job {
            return Err(ProtocolError::invalid("v2.job_result.job"));
        }
        Ok(())
    }
    pub fn validate_for_close(&self, request: &JobCloseRequest) -> Result<(), ProtocolError> {
        self.validate()?;
        request.validate()?;
        if self.job != request.job
            || self.begin_sha256 != request.begin_sha256
            || matches!(self.status, JobStatus::Active)
        {
            return Err(ProtocolError::invalid("v2.job_result.close"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobStatusRequest {
    pub version: Version,
    pub job: Job,
}
impl JobStatusRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobCloseRequest {
    pub version: Version,
    pub job: Job,
    pub begin_sha256: Digest,
    pub reason: CloseReason,
}
impl JobCloseRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}
