//! Read-only guest V2 backup transport reader.
use super::{BackupTransport, BackupTransportError, GuestReadPermission, ObjectStream, Result};
use crate::profile::endpoints::exemem_api_url_for;
use cloud_backup_protocol::v2::{
    access::{ReadGrant, ReadGrantItemResult, ReadGrantRequest, ReadGrantResponse},
    publication::{CommitResponse, CommitStatusRequest, HeadReadRequest, HeadReadResult},
    AcceptedBackup, Stream, Version,
};
use cloud_backup_protocol::{ErrorCode, ProtocolError};
use reqwest::header::{HeaderMap, HeaderValue};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::OnceLock;

pub struct ByteBackupReader {
    transport: BackupTransport,
    guest: GuestAccess,
}

struct GuestAccess {
    stream: Stream,
    expires_ms: u64,
    header: HeaderValue,
    recovery: OnceLock<bool>,
}

impl ByteBackupReader {
    pub fn guest_recovery(permission: GuestReadPermission) -> Result<Self> {
        let env = permission.environment().registry();
        let mut header = HeaderValue::from_str(permission.authorization())
            .map_err(|_| BackupTransportError::Configuration("guest permission header"))?;
        header.set_sensitive(true);
        let (stream, expires_ms) = permission.into_read_scope();
        Ok(Self {
            transport: BackupTransport::for_guest(exemem_api_url_for(env))?,
            guest: GuestAccess {
                stream,
                expires_ms,
                header,
                recovery: OnceLock::new(),
            },
        })
    }

    pub fn canonical_endpoint(&self) -> String {
        self.transport.endpoint("cloud/backups/v2/")
    }

    pub fn guest_stream(&self) -> &Stream {
        &self.guest.stream
    }

    pub async fn guest_head(&self) -> Result<HeadReadResult> {
        let req = HeadReadRequest {
            version: Version::V2,
            stream: self.guest.stream.clone(),
        };
        if let Some(&r) = self.guest.recovery.get() {
            return self.read_guest_head(&req, r).await;
        }
        match self.read_guest_head(&req, true).await {
            Err(e) if recovery_route_missing(&e) => {
                if *self.guest.recovery.get_or_init(|| false) {
                    return Err(e);
                }
            }
            res => {
                let res = res?;
                if *self.guest.recovery.get_or_init(|| true) {
                    return Ok(res);
                }
            }
        }
        self.read_guest_head(&req, false).await
    }

    async fn post_head(&self, op: &str, req: &HeadReadRequest) -> Result<HeadReadResult> {
        req.validate()?;
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
        Ok(res)
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
        let res = self.post_head(op, req).await?;
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

    async fn prepare_guest(&self, stream: &Stream) -> Result<bool> {
        if &self.guest.stream != stream {
            return Err(protocol(ErrorCode::Unauthorized, "guest.scope"));
        }
        if self.guest.recovery.get().is_none() {
            self.guest_head().await?;
        }
        Ok(self.guest.recovery.get() != Some(&false))
    }

    pub async fn commit_status(&self, request: &CommitStatusRequest) -> Result<CommitResponse> {
        request.validate()?;
        if self.prepare_guest(&request.job.stream).await? {
            let resp: CommitResponse = self.post("guest/recovery/commits/status", request).await?;
            resp.validate_for_status(request)?;
            return matches!(resp, CommitResponse::Accepted { .. })
                .then_some(resp)
                .ok_or(BackupTransportError::InvalidResponse(
                    "guest accepted point",
                ));
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
        let op = if self.prepare_guest(&request.job.stream).await? {
            "guest/recovery/objects/read"
        } else {
            require_independent(accepted)?;
            "guest/objects/read"
        };
        let resp: ReadGrantResponse = self.post(op, request).await?;
        resp.validate_for(request, accepted)?;
        if resp.items.iter().any(|i| {
            matches!(i, ReadGrantItemResult::Ok { grant, .. } if grant.expires_ms.get() > self.guest.expires_ms)
        }) {
            return Err(BackupTransportError::InvalidResponse("guest grant expiry"));
        }
        Ok(resp)
    }

    pub async fn open(&self, grant: &ReadGrant) -> Result<ObjectStream> {
        grant.validate()?;
        let sha = Some(grant.sha256.clone());
        self.transport
            .open_bounded(
                grant.bytes.get(),
                sha,
                grant.url.as_str(),
                grant.expires_ms.get(),
            )
            .await
    }

    async fn post<Q: Serialize, R: DeserializeOwned>(&self, op: &str, request: &Q) -> Result<R> {
        let mut headers = HeaderMap::new();
        headers.insert(reqwest::header::AUTHORIZATION, self.guest.header.clone());
        self.transport.post_v2(op, request, headers).await
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
