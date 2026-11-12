//! Separately versioned opaque byte transport. No worker, route or activation.
use super::backup_transport::BackupTransport;
use super::AuthClient;
use cloud_backup_protocol::v2::{
    context::{ContextReadRequest, ContextReadResponse},
    jobs::{JobBeginRequest, JobCloseRequest, JobResult},
    transfer::{
        TransferBatchResponse, TransferIdentityRequest, TransferInput, TransferItemResult,
        TransferMode, TransferPrepareRequest, TransferResumeResponse, TransferResumeResult,
        TransferStatusRequest,
    },
    Version,
};
use reqwest::header::HeaderMap;
use serde::{de::DeserializeOwned, Serialize};
use std::io::Read;

mod contributions;
mod exact_upload;
mod guest_permission;
mod reader;

pub use super::backup_transport::{BackupTransportError, ObjectStream, UploadObservation};
pub use contributions::GuestContributionWriter;
pub use exact_upload::ExactUpload;
pub use guest_permission::{GuestAppendPermission, GuestReadPermission, ShareEnvironment};
pub use reader::ByteBackupReader;
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
        self.transport.endpoint("cloud/backups/v2/")
    }

    pub fn reader(&self) -> ByteBackupReader {
        ByteBackupReader::from_transport(self.transport.clone())
    }

    async fn post<Q: Serialize, R: DeserializeOwned>(&self, op: &str, request: &Q) -> Result<R> {
        self.transport.post_v2(op, request, HeaderMap::new()).await
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
