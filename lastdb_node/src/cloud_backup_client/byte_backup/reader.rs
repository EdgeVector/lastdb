use super::*;
use crate::profile::endpoints::exemem_api_url_for;
use cloud_backup_protocol::v2::access::{
    ReadGrant, ReadGrantItemResult, ReadGrantRequest, ReadGrantResponse,
};
use cloud_backup_protocol::v2::publication::{
    CommitResponse, CommitStatusRequest, HeadReadRequest, HeadReadResult,
};
use cloud_backup_protocol::v2::{AcceptedBackup, Stream, Version};
use cloud_backup_protocol::{ErrorCode, ProtocolError};
use reqwest::header::{HeaderMap, HeaderValue};
use std::sync::OnceLock;

/// Read accepted backups with owner or guest authority.
pub struct ByteBackupReader {
    transport: BackupTransport,
    proof: ReadProof,
}

enum ReadProof {
    Owner,
    Guest(GuestAccess),
}

struct GuestAccess {
    stream: Stream,
    expires_ms: u64,
    header: HeaderValue,
    recovery: OnceLock<bool>,
}

impl ByteBackupClient {
    pub fn reader(&self) -> ByteBackupReader {
        ByteBackupReader {
            transport: self.transport.clone(),
            proof: ReadProof::Owner,
        }
    }
}

impl ByteBackupReader {
    /// Follow the accepted recovery chain when the issuer supports Share logs.
    pub fn guest_recovery(permission: GuestReadPermission) -> Result<Self> {
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
                recovery: OnceLock::new(),
            }),
        })
    }

    pub fn canonical_endpoint(&self) -> String {
        self.transport.endpoint("cloud/backups/v2/")
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
        match access.recovery.get() {
            Some(true) => return self.read_guest_head(&request, true).await,
            Some(false) => {}
            None => match self.read_guest_head(&request, true).await {
                Err(error) if recovery_route_missing(&error) => {
                    if *access.recovery.get_or_init(|| false) {
                        return Err(error);
                    }
                }
                result => {
                    let result = result?;
                    if *access.recovery.get_or_init(|| true) {
                        return Ok(result);
                    }
                }
            },
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

    pub async fn commit_status(&self, request: &CommitStatusRequest) -> Result<CommitResponse> {
        request.validate()?;
        if let ReadProof::Guest(access) = &self.proof {
            return self.guest_commit_status(access, request).await;
        }
        let response: CommitResponse = self.post("commits/status", request).await?;
        response.validate_for_status(request)?;
        Ok(response)
    }

    async fn guest_commit_status(
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

    async fn ensure_guest_mode(&self, access: &GuestAccess) -> Result<()> {
        if access.recovery.get().is_none() {
            self.guest_head().await?;
        }
        Ok(())
    }

    pub async fn read_grants(
        &self,
        request: &ReadGrantRequest,
        accepted: &AcceptedBackup,
    ) -> Result<ReadGrantResponse> {
        request.validate()?;
        accepted.validate()?;
        if &request.job != accepted.job() {
            return Err(BackupTransportError::Configuration("accepted selection"));
        }
        let operation = match &self.proof {
            ReadProof::Owner => "objects/read",
            ReadProof::Guest(access) => {
                access.validate_stream(&request.job.stream)?;
                self.ensure_guest_mode(access).await?;
                if access.use_recovery() {
                    "guest/recovery/objects/read"
                } else {
                    require_independent(accepted)?;
                    "guest/objects/read"
                }
            }
        };
        let response: ReadGrantResponse = self.post(operation, request).await?;
        response.validate_for(request, accepted)?;
        if let ReadProof::Guest(access) = &self.proof {
            access.validate_grant_expiry(&response)?;
        }
        Ok(response)
    }

    /// Provider requests carry only the existing temporary URL.
    pub async fn open(&self, grant: &ReadGrant) -> Result<ObjectStream> {
        grant.validate()?;
        self.transport
            .open(
                StoredObject {
                    sha256: &grant.sha256,
                    bytes: grant.bytes.get(),
                },
                TemporaryGrant {
                    url: grant.url.as_str(),
                    expires_ms: grant.expires_ms.get(),
                },
            )
            .await
    }

    async fn post<Q: Serialize, R: DeserializeOwned>(
        &self,
        operation: &str,
        request: &Q,
    ) -> Result<R> {
        let path = format!("cloud/backups/v2/{operation}");
        let mut headers = HeaderMap::new();
        if let ReadProof::Guest(access) = &self.proof {
            headers.insert(reqwest::header::AUTHORIZATION, access.header.clone());
        }
        self.transport
            .post_bounded_with_headers(
                self.transport.api_endpoint(&path),
                request,
                limits::CONTROL_JSON_MAX_BYTES,
                headers,
            )
            .await
    }
}

impl GuestAccess {
    fn use_recovery(&self) -> bool {
        self.recovery.get() != Some(&false)
    }

    fn validate_stream(&self, stream: &Stream) -> Result<()> {
        if &self.stream != stream {
            return Err(protocol(ErrorCode::Unauthorized, "guest.scope"));
        }
        Ok(())
    }

    fn validate_grant_expiry(&self, response: &ReadGrantResponse) -> Result<()> {
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

fn require_independent(accepted: &AcceptedBackup) -> Result<()> {
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
