use super::{ByteBackupReader, ReadProof};
use crate::byte_backup::{BackupTransport, BackupTransportError, GuestReadPermission, Result};
use cloud_backup_protocol::v2::access::{ReadGrantItemResult, ReadGrantResponse};
use cloud_backup_protocol::v2::publication::{
    CommitResponse, CommitStatusRequest, HeadReadRequest, HeadReadResult,
};
use cloud_backup_protocol::v2::{AcceptedBackup, Stream, Version};
use cloud_backup_protocol::{ErrorCode, ProtocolError};
use folddb_profile::endpoints::exemem_api_url_for;
use reqwest::header::HeaderValue;

pub(super) struct GuestAccess {
    stream: Stream,
    expires_ms: u64,
    pub(super) header: HeaderValue,
}

impl ByteBackupReader {
    /// Use a signed stream permission with no Exemem account credentials.
    pub fn guest(permission: GuestReadPermission) -> Result<Self> {
        let environment = permission.environment().registry();
        let mut header = HeaderValue::from_str(permission.authorization())
            .map_err(|_| BackupTransportError::Configuration("guest permission header"))?;
        header.set_sensitive(true);
        let (stream, expires_ms) = permission.into_read_scope();
        Ok(Self {
            transport: BackupTransport::for_guest(exemem_api_url_for(environment))?,
            proof: ReadProof::Guest(GuestAccess {
                stream,
                expires_ms,
                header,
            }),
        })
    }

    pub fn guest_stream(&self) -> Option<&Stream> {
        if let ReadProof::Guest(access) = &self.proof {
            Some(&access.stream)
        } else {
            None
        }
    }

    /// Resolve the current independent accepted point for this permission.
    pub async fn guest_head(&self) -> Result<HeadReadResult> {
        let stream = self
            .guest_stream()
            .ok_or(BackupTransportError::Configuration("guest reader"))?;
        let request = HeadReadRequest {
            version: Version::V2,
            stream: stream.clone(),
        };
        request.validate()?;
        let result: HeadReadResult = match self.post("guest/head/read", &request).await {
            Ok(response) => response,
            Err(BackupTransportError::Protocol(error))
                if error.code == ErrorCode::NotFound && error.field == "stream" =>
            {
                HeadReadResult::Error { error }
            }
            Err(error) => return Err(error),
        };
        result.validate_for(&request)?;
        if let HeadReadResult::Ok { response } = &result {
            if response.active_commit_id.is_some() {
                return Err(BackupTransportError::InvalidResponse("guest active commit"));
            }
            if let Some(accepted) = &response.accepted {
                require_independent(accepted)?;
            }
        }
        Ok(result)
    }

    pub(super) async fn guest_commit_status(
        &self,
        access: &GuestAccess,
        request: &CommitStatusRequest,
    ) -> Result<CommitResponse> {
        access.validate_stream(&request.job.stream)?;
        let backup = match self.guest_head().await? {
            HeadReadResult::Ok { response } => response
                .accepted
                .filter(|backup| backup.job() == &request.job)
                .ok_or_else(head_changed)?,
            HeadReadResult::Error { error } if error.code == ErrorCode::NotFound => {
                return Err(head_changed());
            }
            HeadReadResult::Error { error } => return Err(error.into()),
        };
        Ok(CommitResponse::Accepted {
            backup: Box::new(backup),
        })
    }
}

impl GuestAccess {
    pub(super) fn validate_stream(&self, stream: &Stream) -> Result<()> {
        if &self.stream != stream {
            return Err(protocol(ErrorCode::Unauthorized, "guest.scope"));
        }
        Ok(())
    }

    pub(super) fn validate_grant_expiry(&self, response: &ReadGrantResponse) -> Result<()> {
        if response.items.iter().any(|item| {
            matches!(item, ReadGrantItemResult::Ok { grant, .. }
                if grant.expires_ms.get() > self.expires_ms)
        }) {
            return Err(BackupTransportError::InvalidResponse("guest grant expiry"));
        }
        Ok(())
    }
}

pub(super) fn require_independent(accepted: &AcceptedBackup) -> Result<()> {
    if !accepted.begin.dependencies().is_empty() {
        return Err(protocol(
            ErrorCode::UnsupportedCapability,
            "guest.independent_snapshot",
        ));
    }
    Ok(())
}

fn head_changed() -> BackupTransportError {
    protocol(ErrorCode::RetryLater, "guest.head_changed")
}

fn protocol(code: ErrorCode, field: &'static str) -> BackupTransportError {
    ProtocolError {
        code,
        field: field.into(),
    }
    .into()
}
