//! Separately versioned opaque byte transport. No worker, route or activation.
use super::backup_transport::{BackupTransport, StoredObject, TemporaryGrant};
use super::AuthClient;
use cloud_backup_protocol::v2::context::{ContextReadRequest, ContextReadResponse};
use cloud_backup_protocol::v2::jobs::{JobBeginRequest, JobCloseRequest, JobResult};
use cloud_backup_protocol::v2::transfer::{
    TransferBatchResponse, TransferIdentityRequest, TransferInput, TransferItemResult,
    TransferMode, TransferPrepareRequest, TransferResumeResponse, TransferResumeResult,
    TransferStatusRequest,
};
use cloud_backup_protocol::v2::{limits, Version};
use serde::de::DeserializeOwned;
use serde::Serialize;
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

    async fn post<Q: Serialize, R: DeserializeOwned>(
        &self,
        operation: &str,
        request: &Q,
    ) -> Result<R> {
        self.transport
            .post(
                &format!("cloud/backups/v2/{operation}"),
                request,
                limits::CONTROL_JSON_MAX_BYTES,
            )
            .await
    }

    pub async fn context(&self, request: &ContextReadRequest) -> Result<ContextReadResponse> {
        request.validate()?;
        let response: ContextReadResponse = self.post("context/read", request).await?;
        response.validate_for(request)?;
        Ok(response)
    }

    pub async fn begin(&self, request: &JobBeginRequest) -> Result<JobResult> {
        request.validate()?;
        let response: JobResult = self.post("jobs/begin", request).await?;
        response.validate_for_begin(request)?;
        Ok(response)
    }

    pub async fn close(&self, request: &JobCloseRequest) -> Result<JobResult> {
        request.validate()?;
        let response: JobResult = self.post("jobs/close", request).await?;
        response.validate_for_close(request)?;
        Ok(response)
    }

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

    pub async fn resume(&self, input: &TransferInput) -> Result<TransferResumeResult> {
        let request = identity(input)?;
        let response: TransferResumeResult = self.post("transfers/resume", &request).await?;
        response.validate_for(input)?;
        Ok(response)
    }

    pub async fn complete(&self, input: &TransferInput) -> Result<TransferItemResult> {
        let request = identity(input)?;
        let response: TransferItemResult = self.post("transfers/complete", &request).await?;
        response.validate_for(input)?;
        Ok(response)
    }

    pub async fn upload(
        &self,
        input: &TransferInput,
        response: &TransferResumeResponse,
        source: Box<dyn Read + Send>,
    ) -> Result<UploadObservation> {
        response.validate_for(input)?;
        let object = StoredObject {
            sha256: &input.object.sha256,
            bytes: input.object.bytes.get(),
        };
        let grant = TemporaryGrant {
            url: response.grant.url.as_str(),
            expires_ms: response.grant.expires_ms.get(),
        };
        self.transport
            .upload(
                object,
                grant,
                source,
                response.grant.transfer_mode == Some(TransferMode::MultipartPart),
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
