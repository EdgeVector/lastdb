//! Begin, status, and close messages for one stream commit.

use super::{hash, limits, AcceptedBackup, ClientDescriptor, Destination, Head, Job, Version};
use crate::{DecimalU64, Digest, ProtocolError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Open one commit on a stream: descriptor, destinations, predecessor, and recovery dependencies.
pub struct Begin {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Stream and commit id this value belongs to.
    pub job: Job,
    /// Opaque client JSON stored with the job. The transport does not interpret it.
    pub descriptor: ClientDescriptor,
    /// An empty list preserves legacy admission and provider identities.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_destinations: Vec<Destination>,
    /// Explicit terminal-upload contract for new CI R2 allocations. Omission
    /// preserves legacy admission and hashes; false is never an opt-in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r2_multipart: Option<bool>,
    /// Previous accepted head. Absent starts a stream. The same commit id or a max ordinal is rejected.
    pub predecessor: Option<Head>,
    /// Exact recovery dependencies, separate from publication order. None is
    /// legacy and conservatively protects the predecessor chain. Some([])
    /// declares an independent recovery base. Emit only after capability admission.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_dependencies: Option<Vec<Head>>,
}
impl Begin {
    /// Reject `r2_multipart: false`, a bad job or descriptor, a bad predecessor or dependency list, or oversized JSON.
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
    /// Domain-separated SHA-256 of this begin value, after validation.
    pub fn input_sha256(&self) -> Result<Digest, ProtocolError> {
        self.validate()?;
        hash::input("lastdb:cloud-byte-backup:begin:v2", self)
    }
    /// Predecessor ordinal plus one, or 1 when there is no predecessor.
    pub fn ordinal(&self) -> Result<DecimalU64, ProtocolError> {
        self.validate()?;
        self.predecessor
            .as_ref()
            .map_or(0, |head| head.ordinal.get())
            .checked_add(1)
            .map(DecimalU64::new)
            .ok_or_else(|| ProtocolError::invalid("v2.begin.ordinal"))
    }
    /// The acceptance transaction checks this exact predecessor atomically.
    /// Any native frontier or format ordering is solely a client rule.
    pub fn validate_successor(&self, previous: &AcceptedBackup) -> Result<(), ProtocolError> {
        self.validate()?;
        previous.validate()?;
        if self.job.stream != previous.job().stream
            || self.predecessor.as_ref() != Some(&previous.head())
        {
            return Err(ProtocolError::invalid("v2.begin.successor"));
        }
        Ok(())
    }
}
/// Name of a begin request. It is the `Begin` value.
pub type JobBeginRequest = Begin;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Why a job was closed without publication.
pub enum CloseReason {
    /// The caller asked to close the job.
    UserRequested,
    /// The client source is no longer available.
    SourceUnavailable,
    /// The account generation changed.
    GenerationChanged,
}

/// Close fences acceptance and never proves that an older provider effect failed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum JobStatus {
    /// The job is open. Close validation rejects this state.
    Active,
    /// The job published this backup.
    Published {
        /// Backup this job published.
        backup: Box<AcceptedBackup>,
    },
    /// The job closed without publication.
    Closed {
        /// Why the job closed without publication.
        reason: CloseReason,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Current state of one job, bound to the begin digest.
pub struct JobResult {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Stream and commit id this value belongs to.
    pub job: Job,
    /// SHA-256 of the Begin value.
    pub begin_sha256: Digest,
    /// Whether the job is active, published, or closed.
    pub status: JobStatus,
}
impl JobResult {
    /// Reject a bad job, a published backup that disagrees with this job or begin digest, or oversized JSON.
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
    /// Require the same job and begin digest, and check a published backup against that begin.
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
    /// Require the same job as the status request.
    pub fn validate_for_status(&self, request: &JobStatusRequest) -> Result<(), ProtocolError> {
        self.validate()?;
        request.validate()?;
        if self.job != request.job {
            return Err(ProtocolError::invalid("v2.job_result.job"));
        }
        Ok(())
    }
    /// Require the same job and begin digest, and reject a result that is still `Active`.
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
/// Ask for the state of one job.
pub struct JobStatusRequest {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Stream and commit id this value belongs to.
    pub job: Job,
}
impl JobStatusRequest {
    /// Validate the job and the encoded size.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Close one job. Closing does not prove an older provider write failed.
pub struct JobCloseRequest {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Stream and commit id this value belongs to.
    pub job: Job,
    /// SHA-256 of the Begin value.
    pub begin_sha256: Digest,
    /// Why the caller is closing the job.
    pub reason: CloseReason,
}
impl JobCloseRequest {
    /// Validate the job and the encoded size.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}
