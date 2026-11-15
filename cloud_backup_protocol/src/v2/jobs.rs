//! Begin, status, and close messages for one cold-blob job.

use super::{hash, limits, Destination, Job, Version};
use crate::{Digest, ProtocolError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Open one cold-blob upload job on a stream.
pub struct Begin {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Stream and commit id this value belongs to.
    pub job: Job,
    /// The providers that hold the objects of this job. The list is not empty.
    pub required_destinations: Vec<Destination>,
}
impl Begin {
    /// Reject an empty or duplicate destination list, a bad job, or oversized JSON.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        super::common::validate_destinations(&self.required_destinations)?;
        if self.required_destinations.is_empty() {
            return Err(ProtocolError::invalid("v2.begin.required_destinations"));
        }
        self.job.validate()?;
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }

    /// Domain-separated SHA-256 of this begin value, after validation.
    pub fn input_sha256(&self) -> Result<Digest, ProtocolError> {
        self.validate()?;
        hash::input("lastdb:cloud-byte-backup:begin:v2", self)
    }
}
/// Name of a begin request. It is the `Begin` value.
pub type JobBeginRequest = Begin;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Why a job was closed.
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
    /// The job closed.
    Closed {
        /// Why the job closed.
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
    /// Whether the job is active or closed.
    pub status: JobStatus,
}
impl JobResult {
    /// Reject a bad job or oversized JSON.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
    /// Require the same job and begin digest.
    pub fn validate_for_begin(&self, begin: &Begin) -> Result<(), ProtocolError> {
        self.validate()?;
        if self.job != begin.job || self.begin_sha256 != begin.input_sha256()? {
            return Err(ProtocolError::invalid("v2.job_result.begin"));
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
