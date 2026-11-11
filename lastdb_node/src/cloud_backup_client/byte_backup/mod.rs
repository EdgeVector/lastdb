//! Separately versioned opaque byte transport. No worker, route or activation.
use super::backup_transport::{BackupTransport, StoredObject, TemporaryGrant};
use super::AuthClient;
use cloud_backup_protocol::v2::access::{
    InventoryPutRequest, InventoryPutResult, ReadGrant, ReadGrantRequest, ReadGrantResponse,
};
use cloud_backup_protocol::v2::blobs::{
    BlobReadItem, BlobReadRequest, BlobReadResponse, BlobReleaseRequest, BlobReleaseResponse,
};
use cloud_backup_protocol::v2::context::{ContextReadRequest, ContextReadResponse};
use cloud_backup_protocol::v2::discovery::{StreamsListRequest, StreamsListResponse};
use cloud_backup_protocol::v2::inventory::InventoryProof;
use cloud_backup_protocol::v2::jobs::{JobBeginRequest, JobCloseRequest, JobResult};
use cloud_backup_protocol::v2::publication::{
    Commit, CommitResponse, CommitStatusRequest, HeadReadRequest, HeadReadResult,
};
use cloud_backup_protocol::v2::transfer::{
    TransferBatchResponse, TransferIdentityRequest, TransferInput, TransferItemResult,
    TransferMode, TransferPrepareRequest, TransferResumeResponse, TransferResumeResult,
    TransferStatusRequest,
};
use cloud_backup_protocol::v2::usage::{UsageReadRequest, UsageReadResponse};
use cloud_backup_protocol::v2::{limits, AcceptedBackup, Begin, Version};
use cloud_backup_protocol::{ErrorCode, ProtocolError};
use serde::{de::DeserializeOwned, Serialize};
use std::io::Read;

mod contributions;
mod guest_permission;
mod reader;
mod share_recovery;

pub use super::backup_transport::{BackupTransportError, ObjectStream, UploadObservation};
pub use contributions::GuestContributionWriter;
pub use guest_permission::{GuestAppendPermission, GuestReadPermission, ShareEnvironment};
pub use reader::ByteBackupReader;
pub type Result<T> = std::result::Result<T, BackupTransportError>;

#[derive(Clone)]
pub struct ByteBackupClient {
    transport: BackupTransport,
}
impl ByteBackupClient {
    /// Construction reuses only configured credentials and bounded byte transport.
    /// It starts no enrollment, claim, stream or background work.
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

    pub async fn usage(&self, request: &UsageReadRequest) -> Result<UsageReadResponse> {
        request.validate()?;
        let response: UsageReadResponse = self.post("usage/read", request).await?;
        response.validate_for(request)?;
        Ok(response)
    }

    pub async fn list(&self, request: &StreamsListRequest) -> Result<StreamsListResponse> {
        request.validate()?;
        let response: StreamsListResponse = self.post("streams/list", request).await?;
        response.validate_for(request)?;
        Ok(response)
    }

    pub async fn put_inventory(&self, request: &InventoryPutRequest) -> Result<InventoryPutResult> {
        request.validate()?;
        let response: InventoryPutResult = self.post("inventory/put", request).await?;
        response.validate_for(request)?;
        Ok(response)
    }

    pub async fn read_grants(
        &self,
        request: &ReadGrantRequest,
        accepted: &AcceptedBackup,
    ) -> Result<ReadGrantResponse> {
        self.reader().read_grants(request, accepted).await
    }

    /// The caller validates accepted membership through read_grants before open.
    /// EOF authenticates exact stored byte count and SHA256, not client plaintext.
    pub async fn open(&self, grant: &ReadGrant) -> Result<ObjectStream> {
        self.reader().open(grant).await
    }

    /// Ask for GET grants for blob paths of the caller's account. An absent
    /// object is a normal [`BlobReadItem::Absent`] result.
    pub async fn blob_read(&self, request: &BlobReadRequest) -> Result<BlobReadResponse> {
        request.validate()?;
        let response: BlobReadResponse = self.post("blobs/read", request).await?;
        response.validate_for(request)?;
        Ok(response)
    }

    /// Open one granted blob object. The read requires exactly the granted
    /// length. No SHA-256 of the stored bytes exists; the caller authenticates
    /// them (record files: AEAD decryption and the record's marker).
    pub async fn open_blob(&self, grant: &BlobReadItem) -> Result<ObjectStream> {
        let BlobReadItem::Grant {
            bytes,
            expires_ms,
            url,
            ..
        } = grant
        else {
            return Err(BackupTransportError::InvalidResponse("blob grant"));
        };
        self.transport
            .open_exact_length(
                bytes.get(),
                TemporaryGrant {
                    url: url.as_str(),
                    expires_ms: expires_ms.get(),
                },
            )
            .await
    }

    /// Record blob versions that an open job's records no longer use. The
    /// service deletes them when it accepts the job. Repeating a request is
    /// harmless; a path that was never uploaded is ignored.
    pub async fn release_blobs(&self, request: &BlobReleaseRequest) -> Result<BlobReleaseResponse> {
        request.validate()?;
        let response: BlobReleaseResponse = self.post("blobs/release", request).await?;
        if response.job != request.job || response.recorded.get() > request.paths.len() as u64 {
            return Err(ProtocolError::invalid("v2.blob_release.response").into());
        }
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

    pub async fn commit(
        &self,
        begin: &Begin,
        input: &Commit,
        inventory: &InventoryProof,
    ) -> Result<CommitResponse> {
        input.validate_for(begin)?;
        input.validate_inventory(inventory)?;
        let response: CommitResponse = self.post("commits/prepare", input).await?;
        response.validate_for(begin, input)?;
        Ok(response)
    }

    pub async fn commit_status(&self, request: &CommitStatusRequest) -> Result<CommitResponse> {
        self.reader().commit_status(request).await
    }

    pub async fn head(&self, request: &HeadReadRequest) -> Result<HeadReadResult> {
        request.validate()?;
        let response: HeadReadResult = match self.post("heads/read", request).await {
            Ok(response) => response,
            Err(BackupTransportError::Protocol(error))
                if error.code == ErrorCode::NotFound && error.field.as_str() == "stream" =>
            {
                HeadReadResult::Error { error }
            }
            Err(error) => return Err(error),
        };
        response.validate_for(request)?;
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

    /// The exact validated resume response binds its grant to the frozen input.
    /// An HTTP observation never supplies verified receipt or publication authority.
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
        if response.grant.transfer_mode == Some(TransferMode::MultipartPart) {
            return self.transport.upload_part(object, grant, source).await;
        }
        self.transport.upload(object, grant, source).await
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
