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
    guest: Option<GuestAccess>,
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
            guest: None,
        }
    }
}

impl ByteBackupReader {
    /// Follow the accepted recovery chain when the issuer supports Share logs.
    pub fn guest_recovery(permission: GuestReadPermission) -> Result<Self> {
        let env = permission.environment().registry();
        let mut header = HeaderValue::from_str(permission.authorization())
            .map_err(|_| BackupTransportError::Configuration("guest permission header"))?;
        header.set_sensitive(true);
        let (stream, expires_ms) = permission.into_read_scope();
        Ok(Self {
            transport: BackupTransport::for_guest(exemem_api_url_for(env))?,
            guest: Some(GuestAccess {
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
        self.guest.as_ref().map(|g| &g.stream)
    }

    /// Resolve the accepted head for this permission and reader mode.
    pub async fn guest_head(&self) -> Result<HeadReadResult> {
        let access = self
            .guest
            .as_ref()
            .ok_or(BackupTransportError::Configuration("guest reader"))?;
        let req = HeadReadRequest {
            version: Version::V2,
            stream: access.stream.clone(),
        };
        req.validate()?;
        if let Some(&r) = access.recovery.get() {
            return self.read_guest_head(&req, r).await;
        }
        match self.read_guest_head(&req, true).await {
            Err(e) if recovery_route_missing(&e) => {
                if *access.recovery.get_or_init(|| false) {
                    return Err(e);
                }
            }
            res => {
                let res = res?;
                if *access.recovery.get_or_init(|| true) {
                    return Ok(res);
                }
            }
        }
        self.read_guest_head(&req, false).await
    }

    async fn read_guest_head(
        &self,
        req: &HeadReadRequest,
        recovery: bool,
    ) -> Result<HeadReadResult> {
        let op = if recovery {
            "guest/recovery/head/read"
        } else {
            "guest/head/read"
        };
        let res = match self.post(op, req).await {
            Ok(resp) => resp,
            Err(BackupTransportError::Protocol(error))
                if error.code == ErrorCode::NotFound && error.field == "stream" =>
            {
                HeadReadResult::Error { error }
            }
            Err(e) => return Err(e),
        };
        res.validate_for(req)?;
        if let HeadReadResult::Ok { response } = &res {
            if response.active_commit_id.is_some() {
                return Err(BackupTransportError::InvalidResponse("guest active commit"));
            }
            if let Some(accepted) = response.accepted.as_ref().filter(|_| !recovery) {
                require_independent(accepted)?;
            }
        }
        Ok(res)
    }

    async fn prepare_guest(&self, stream: &Stream) -> Result<Option<(&GuestAccess, bool)>> {
        let Some(access) = &self.guest else {
            return Ok(None);
        };
        if &access.stream != stream {
            return Err(protocol(ErrorCode::Unauthorized, "guest.scope"));
        }
        if access.recovery.get().is_none() {
            self.guest_head().await?;
        }
        Ok(Some((access, access.recovery.get() != Some(&false))))
    }

    pub async fn commit_status(&self, request: &CommitStatusRequest) -> Result<CommitResponse> {
        request.validate()?;
        let Some((_, recovery)) = self.prepare_guest(&request.job.stream).await? else {
            let resp: CommitResponse = self.post("commits/status", request).await?;
            resp.validate_for_status(request)?;
            return Ok(resp);
        };
        if recovery {
            let resp: CommitResponse = self.post("guest/recovery/commits/status", request).await?;
            resp.validate_for_status(request)?;
            if !matches!(resp, CommitResponse::Accepted { .. }) {
                return Err(BackupTransportError::InvalidResponse(
                    "guest accepted point",
                ));
            }
            return Ok(resp);
        }
        let changed = || protocol(ErrorCode::RetryLater, "guest.head_changed");
        match self.guest_head().await? {
            HeadReadResult::Ok { response } => response
                .accepted
                .filter(|b| b.job() == &request.job)
                .map(|b| CommitResponse::Accepted {
                    backup: Box::new(b),
                })
                .ok_or_else(changed),
            HeadReadResult::Error { error } if error.code == ErrorCode::NotFound => Err(changed()),
            HeadReadResult::Error { error } => Err(error.into()),
        }
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
        let guest = self.prepare_guest(&request.job.stream).await?;
        let op = match guest {
            None => "objects/read",
            Some((_, true)) => "guest/recovery/objects/read",
            Some((_, false)) => {
                require_independent(accepted)?;
                "guest/objects/read"
            }
        };
        let resp: ReadGrantResponse = self.post(op, request).await?;
        resp.validate_for(request, accepted)?;
        if let Some((access, _)) = guest {
            let expired = resp.items.iter().any(|item| {
                matches!(item, ReadGrantItemResult::Ok { grant, .. } if grant.expires_ms.get() > access.expires_ms)
            });
            if expired {
                return Err(BackupTransportError::InvalidResponse("guest grant expiry"));
            }
        }
        Ok(resp)
    }

    /// Provider requests carry only the existing temporary URL.
    pub async fn open(&self, grant: &ReadGrant) -> Result<ObjectStream> {
        grant.validate()?;
        let obj = StoredObject {
            sha256: &grant.sha256,
            bytes: grant.bytes.get(),
        };
        let url = TemporaryGrant {
            url: grant.url.as_str(),
            expires_ms: grant.expires_ms.get(),
        };
        self.transport.open(obj, url).await
    }

    async fn post<Q: Serialize, R: DeserializeOwned>(&self, op: &str, request: &Q) -> Result<R> {
        let mut headers = HeaderMap::new();
        if let Some(access) = &self.guest {
            headers.insert(reqwest::header::AUTHORIZATION, access.header.clone());
        }
        let url = self
            .transport
            .api_endpoint(&format!("cloud/backups/v2/{op}"));
        self.transport
            .post_bounded_with_headers(url, request, limits::CONTROL_JSON_MAX_BYTES, headers)
            .await
    }
}

fn recovery_route_missing(error: &BackupTransportError) -> bool {
    matches!(
        error,
        BackupTransportError::Authentication | BackupTransportError::Http(404)
    ) || matches!(error, BackupTransportError::Protocol(e) if e.code == ErrorCode::NotFound && e.field == "http.route")
}

fn require_independent(accepted: &AcceptedBackup) -> Result<()> {
    accepted
        .begin
        .dependencies()
        .is_empty()
        .then_some(())
        .ok_or_else(|| {
            protocol(
                ErrorCode::UnsupportedCapability,
                "guest.independent_snapshot",
            )
        })
}

fn protocol(code: ErrorCode, field: &'static str) -> BackupTransportError {
    ProtocolError {
        code,
        field: field.into(),
    }
    .into()
}
