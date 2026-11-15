//! Authenticated cloud storage context, job lifecycle, presigned blob transfer, and guest permissions.
use super::{AuthClient, BackupTransport, PresignedPut};
use cloud_backup_protocol::v2::{
    context::{ContextReadRequest, ContextReadResponse},
    jobs::{JobBeginRequest, JobCloseRequest, JobResult},
    limits::CONTROL_JSON_MAX_BYTES,
    transfer::{
        TransferBatchResponse, TransferIdentityRequest, TransferInput, TransferItemResult,
        TransferPrepareRequest, TransferResumeResponse, TransferResumeResult,
    },
    Version,
};
use reqwest::header::HeaderMap;
use serde::{de::DeserializeOwned, Serialize};

mod exact_upload;
mod guest_permission;

pub use super::BackupTransportError;
pub use exact_upload::ExactUpload;
pub use guest_permission::{GuestReadPermission, ShareEnvironment};
pub type Result<T> = std::result::Result<T, BackupTransportError>;

macro_rules! validated_rpc {
    ($vis:vis $fn:ident($req:ty) -> $resp:ty : $path:literal, $check:ident) => {
        $vis async fn $fn(&self, request: &$req) -> Result<$resp> {
            request.validate()?;
            let response: $resp = self.post($path, request).await?;
            response.$check(request)?;
            Ok(response)
        }
    };
}

macro_rules! identity_rpc {
    ($fn:ident -> $resp:ty : $path:literal) => {
        async fn $fn(&self, input: &TransferInput) -> Result<$resp> {
            let response: $resp = self.post($path, &identity(input)?).await?;
            response.validate_for(input)?;
            Ok(response)
        }
    };
}

#[derive(Clone)]
pub struct StorageTransferClient {
    transport: BackupTransport,
}

impl StorageTransferClient {
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

    validated_rpc!(pub context(ContextReadRequest) -> ContextReadResponse : "context/read", validate_for);
    validated_rpc!(begin(JobBeginRequest) -> JobResult : "jobs/begin", validate_for_begin);
    validated_rpc!(pub close(JobCloseRequest) -> JobResult : "jobs/close", validate_for_close);
    identity_rpc!(resume -> TransferResumeResult : "transfers/resume");
    identity_rpc!(complete -> TransferItemResult : "transfers/complete");

    async fn prepare(&self, request: &TransferPrepareRequest) -> Result<TransferBatchResponse> {
        request.validate()?;
        let response: TransferBatchResponse = self.post("transfers/prepare", request).await?;
        response.validate_for(&request.items)?;
        Ok(response)
    }

    async fn put_presigned(
        &self,
        input: &TransferInput,
        response: &TransferResumeResponse,
        data: Vec<u8>,
    ) -> Result<()> {
        response.validate_for(input)?;
        let grant = &response.grant;
        let put = PresignedPut {
            sha256: &input.object.sha256,
            bytes: input.object.bytes.get(),
            grant_url: grant.url.as_str(),
            expires_ms: grant.expires_ms.get(),
        };
        self.transport.put_presigned(&put, data).await
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
