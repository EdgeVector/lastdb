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
use std::sync::OnceLock;

pub(super) struct GuestAccess {
    stream: Stream,
    expires_ms: u64,
    pub(super) header: HeaderValue,
    recovery: Option<OnceLock<bool>>,
}

impl ByteBackupReader {
    /// Use a signed stream permission with no Exemem account credentials.
    pub fn guest(permission: GuestReadPermission) -> Result<Self> {
        Self::guest_with_mode(permission, None)
    }

    /// Follow the accepted recovery chain when the issuer supports Share logs.
    pub fn guest_recovery(permission: GuestReadPermission) -> Result<Self> {
        Self::guest_with_mode(permission, Some(OnceLock::new()))
    }

    fn guest_with_mode(
        permission: GuestReadPermission,
        recovery: Option<OnceLock<bool>>,
    ) -> Result<Self> {
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
                recovery,
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

    /// Resolve the accepted head for this permission and reader mode.
    pub async fn guest_head(&self) -> Result<HeadReadResult> {
        let ReadProof::Guest(access) = &self.proof else {
            return Err(BackupTransportError::Configuration("guest reader"));
        };
        let request = HeadReadRequest {
            version: Version::V2,
            stream: access.stream.clone(),
        };
        request.validate()?;
        if let Some(mode) = &access.recovery {
            match mode.get() {
                Some(true) => return self.read_guest_head(&request, true).await,
                Some(false) => {}
                None => match self.read_guest_head(&request, true).await {
                    Err(error) if recovery_route_missing(&error) => {
                        if *mode.get_or_init(|| false) {
                            return Err(error);
                        }
                    }
                    result => {
                        let result = result?;
                        if *mode.get_or_init(|| true) {
                            return Ok(result);
                        }
                    }
                },
            }
        }
        self.read_guest_head(&request, false).await
    }

    async fn read_guest_head(
        &self,
        request: &HeadReadRequest,
        recovery: bool,
    ) -> Result<HeadReadResult> {
        let operation = if recovery {
            "guest/recovery/head/read"
        } else {
            "guest/head/read"
        };
        let result: HeadReadResult = match self.post(operation, request).await {
            Ok(response) => response,
            Err(BackupTransportError::Protocol(error))
                if error.code == ErrorCode::NotFound && error.field == "stream" =>
            {
                HeadReadResult::Error { error }
            }
            Err(error) => return Err(error),
        };
        result.validate_for(request)?;
        if let HeadReadResult::Ok { response } = &result {
            if response.active_commit_id.is_some() {
                return Err(BackupTransportError::InvalidResponse("guest active commit"));
            }
            if let Some(accepted) = response.accepted.as_ref().filter(|_| !recovery) {
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
        self.ensure_guest_mode(access).await?;
        if access.use_recovery() {
            let response: CommitResponse =
                self.post("guest/recovery/commits/status", request).await?;
            response.validate_for_status(request)?;
            if !matches!(response, CommitResponse::Accepted { .. }) {
                return Err(BackupTransportError::InvalidResponse(
                    "guest accepted point",
                ));
            }
            return Ok(response);
        }
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

    pub(super) async fn ensure_guest_mode(&self, access: &GuestAccess) -> Result<()> {
        if access
            .recovery
            .as_ref()
            .is_some_and(|mode| mode.get().is_none())
        {
            self.guest_head().await?;
        }
        Ok(())
    }
}

impl GuestAccess {
    pub(super) fn use_recovery(&self) -> bool {
        self.recovery
            .as_ref()
            .is_some_and(|mode| mode.get() != Some(&false))
    }

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

fn recovery_route_missing(error: &BackupTransportError) -> bool {
    match error {
        BackupTransportError::Authentication | BackupTransportError::Http(404) => true,
        BackupTransportError::Protocol(error) => {
            error.code == ErrorCode::NotFound && error.field == "http.route"
        }
        _ => false,
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
