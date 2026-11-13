//! Separately versioned opaque byte transport and read-only guest V2 reader.
use super::{AuthClient, BackupTransport};
use crate::profile::endpoints::exemem_api_url_for;
use cloud_backup_protocol::v2::{
    access::{ReadGrant, ReadGrantItemResult, ReadGrantRequest, ReadGrantResponse},
    context::{ContextReadRequest, ContextReadResponse},
    jobs::{JobBeginRequest, JobCloseRequest, JobResult},
    limits::CONTROL_JSON_MAX_BYTES,
    publication::{CommitResponse, CommitStatusRequest, HeadReadRequest, HeadReadResult},
    transfer::{
        TransferBatchResponse, TransferIdentityRequest, TransferInput, TransferItemResult,
        TransferMode, TransferPrepareRequest, TransferResumeResponse, TransferResumeResult,
        TransferStatusRequest,
    },
    AcceptedBackup, Stream, Version,
};
use cloud_backup_protocol::{ErrorCode, ProtocolError};
use reqwest::header::{HeaderMap, HeaderValue};
use serde::{de::DeserializeOwned, Serialize};
use std::io::{Read, Write};
use std::sync::OnceLock;

mod contributions;
mod exact_upload;
mod guest_permission;

pub use super::{BackupTransportError, UploadObservation};
pub use contributions::GuestContributionWriter;
pub use exact_upload::ExactUpload;
pub use guest_permission::{GuestAppendPermission, GuestReadPermission, ShareEnvironment};
pub type Result<T> = std::result::Result<T, BackupTransportError>;

macro_rules! validated_rpc {
    ($fn:ident($req:ty) -> $resp:ty : $path:literal, $check:ident) => {
        pub async fn $fn(&self, request: &$req) -> Result<$resp> {
            request.validate()?;
            let response: $resp = self.post($path, request).await?;
            response.$check(request)?;
            Ok(response)
        }
    };
}

macro_rules! identity_rpc {
    ($fn:ident -> $resp:ty : $path:literal) => {
        pub async fn $fn(&self, input: &TransferInput) -> Result<$resp> {
            let response: $resp = self.post($path, &identity(input)?).await?;
            response.validate_for(input)?;
            Ok(response)
        }
    };
}

#[derive(Clone)]
pub struct ByteBackupClient {
    transport: BackupTransport,
}

impl ByteBackupClient {
    pub fn from_auth(auth: AuthClient) -> Result<Self> {
        Ok(Self {
            transport: BackupTransport::from_auth(auth)?,
        })
    }

    pub fn canonical_endpoint(&self) -> String {
        self.transport.api_endpoint("cloud/backups/v2/").to_string()
    }

    async fn post<Q: Serialize, R: DeserializeOwned>(&self, op: &str, request: &Q) -> Result<R> {
        let url = self
            .transport
            .api_endpoint(&format!("cloud/backups/v2/{op}"));
        self.transport
            .post_bounded_with_headers(url, request, CONTROL_JSON_MAX_BYTES, HeaderMap::new())
            .await
    }

    validated_rpc!(context(ContextReadRequest) -> ContextReadResponse : "context/read", validate_for);
    validated_rpc!(begin(JobBeginRequest) -> JobResult : "jobs/begin", validate_for_begin);
    validated_rpc!(close(JobCloseRequest) -> JobResult : "jobs/close", validate_for_close);
    identity_rpc!(resume -> TransferResumeResult : "transfers/resume");
    identity_rpc!(complete -> TransferItemResult : "transfers/complete");

    pub async fn prepare(&self, request: &TransferPrepareRequest) -> Result<TransferBatchResponse> {
        request.validate()?;
        let response: TransferBatchResponse = self.post("transfers/prepare", request).await?;
        response.validate_for(&request.items)?;
        Ok(response)
    }

    pub async fn transfer_status(
        &self,
        request: &TransferStatusRequest,
        inputs: &[TransferInput],
    ) -> Result<TransferBatchResponse> {
        request.validate_inputs(inputs)?;
        let response: TransferBatchResponse = self.post("transfers/status", request).await?;
        response.validate_for(inputs)?;
        Ok(response)
    }

    pub async fn upload(
        &self,
        input: &TransferInput,
        response: &TransferResumeResponse,
        source: Box<dyn Read + Send>,
    ) -> Result<UploadObservation> {
        response.validate_for(input)?;
        let g = &response.grant;
        let mp = g.transfer_mode == Some(TransferMode::MultipartPart);
        self.transport
            .upload(
                &input.object.sha256,
                input.object.bytes.get(),
                g.url.as_str(),
                g.expires_ms.get(),
                source,
                mp,
            )
            .await
    }
}

fn identity(input: &TransferInput) -> Result<TransferIdentityRequest> {
    input.validate()?;
    Ok(TransferIdentityRequest {
        version: Version::V2,
        job: input.job.clone(),
        request_id: input.request_id.clone(),
        input_sha256: input.input_sha256()?,
    })
}

pub struct ByteBackupReader {
    transport: BackupTransport,
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
            stream,
            expires_ms,
            header,
            recovery: OnceLock::new(),
        })
    }

    pub fn guest_stream(&self) -> &Stream {
        &self.stream
    }

    pub async fn guest_head(&self) -> Result<HeadReadResult> {
        let req = HeadReadRequest {
            version: Version::V2,
            stream: self.stream.clone(),
        };
        if let Some(&r) = self.recovery.get() {
            return self.read_guest_head(&req, r).await;
        }
        match self.read_guest_head(&req, true).await {
            Err(e) if recovery_route_missing(&e) => {
                if *self.recovery.get_or_init(|| false) {
                    return Err(e);
                }
            }
            res => {
                let res = res?;
                if *self.recovery.get_or_init(|| true) {
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
        req.validate()?;
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

    async fn prepare_guest(&self, stream: &Stream) -> Result<bool> {
        if &self.stream != stream {
            return Err(protocol(ErrorCode::Unauthorized, "guest.scope"));
        }
        if self.recovery.get().is_none() {
            self.guest_head().await?;
        }
        Ok(self.recovery.get() != Some(&false))
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
        if resp.items.iter().any(|i| matches!(i, ReadGrantItemResult::Ok { grant, .. } if grant.expires_ms.get() > self.expires_ms)) {
            return Err(BackupTransportError::InvalidResponse("guest grant expiry"));
        }
        Ok(resp)
    }

    pub async fn download_to(&self, grant: &ReadGrant, out: &mut impl Write) -> Result<()> {
        grant.validate()?;
        self.transport
            .download_to(
                grant.bytes.get(),
                &grant.sha256,
                grant.url.as_str(),
                grant.expires_ms.get(),
                out,
            )
            .await
    }

    async fn post<Q: Serialize, R: DeserializeOwned>(&self, op: &str, request: &Q) -> Result<R> {
        let mut headers = HeaderMap::new();
        headers.insert(reqwest::header::AUTHORIZATION, self.header.clone());
        let url = self
            .transport
            .api_endpoint(&format!("cloud/backups/v2/{op}"));
        self.transport
            .post_bounded_with_headers(url, request, CONTROL_JSON_MAX_BYTES, headers)
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
