use super::{AcceptedBackup, CommitInput};
use crate::jobs::JobBeginInput;
use crate::{
    BackupJob, BackupStream, Digest, Id, PendingReason, ProtocolError, TerminalFailure, Version,
};
use serde::{Deserialize, Serialize};

pub type CommitRequest = CommitInput;

/// Status reads the exact job. No caller-side input hash is required for recovery.
/// No saved submission returns error code NotFound, not another pending state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitStatusRequest {
    pub version: Version,
    pub job: BackupJob,
}

impl CommitStatusRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        crate::limits::encoded_json_bytes(self, crate::limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum CommitResponse {
    Pending {
        version: Version,
        job: BackupJob,
        job_input_sha256: Digest,
        commit_sha256: Digest,
        reason: PendingReason,
    },
    Accepted {
        backup: Box<AcceptedBackup>,
    },
    Rejected {
        version: Version,
        job: BackupJob,
        job_input_sha256: Digest,
        commit_sha256: Digest,
        failure: TerminalFailure,
    },
}

impl CommitResponse {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::Pending { job, .. } => job.validate()?,
            Self::Accepted { backup } => backup.validate()?,
            Self::Rejected { job, failure, .. } => {
                job.validate()?;
                failure.validate()?;
            }
        }
        crate::limits::encoded_json_bytes(self, crate::limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }

    pub fn validate_for(
        &self,
        begin: &JobBeginInput,
        commit: &CommitInput,
    ) -> Result<(), ProtocolError> {
        self.validate()?;
        commit.validate_for(begin)?;
        match self {
            Self::Accepted { backup } => backup.validate_for(begin, commit),
            Self::Pending {
                job,
                job_input_sha256,
                commit_sha256,
                ..
            }
            | Self::Rejected {
                job,
                job_input_sha256,
                commit_sha256,
                ..
            } => {
                if job != &commit.job
                    || job_input_sha256 != &commit.job_input_sha256
                    || commit_sha256 != &commit.input_sha256()?
                {
                    return Err(ProtocolError::invalid("commit_response.input"));
                }
                Ok(())
            }
        }
    }

    pub fn validate_for_status(&self, request: &CommitStatusRequest) -> Result<(), ProtocolError> {
        self.validate()?;
        request.validate()?;
        let job = match self {
            Self::Pending { job, .. } | Self::Rejected { job, .. } => job,
            Self::Accepted { backup } => &backup.job,
        };
        if job != &request.job {
            return Err(ProtocolError::invalid("commit_status.job"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeadReadRequest {
    pub version: Version,
    pub stream: BackupStream,
}

impl HeadReadRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()?;
        crate::limits::encoded_json_bytes(self, crate::limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

/// A pending job never masks the last accepted backup.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeadReadResponse {
    pub version: Version,
    pub stream: BackupStream,
    pub accepted: Option<AcceptedBackup>,
    pub active_commit_id: Option<Id>,
}

impl HeadReadResponse {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()?;
        if let Some(backup) = &self.accepted {
            backup.validate()?;
            if backup.job.stream != self.stream
                || self.active_commit_id.as_ref() == Some(&backup.job.commit_id)
            {
                return Err(ProtocolError::invalid("head_read.accepted"));
            }
        }
        crate::limits::encoded_json_bytes(self, crate::limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }

    pub fn validate_for(&self, request: &HeadReadRequest) -> Result<(), ProtocolError> {
        self.validate()?;
        request.validate()?;
        if self.stream != request.stream {
            return Err(ProtocolError::invalid("head_read.stream"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum HeadReadResult {
    Ok { response: Box<HeadReadResponse> },
    Error { error: ProtocolError },
}

impl HeadReadResult {
    pub fn validate_for(&self, request: &HeadReadRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        if let Self::Ok { response } = self {
            response.validate_for(request)?;
        }
        crate::limits::encoded_json_bytes(self, crate::limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}
