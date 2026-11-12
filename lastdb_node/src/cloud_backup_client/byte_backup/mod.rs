//! Separately versioned opaque byte transport. No worker, route or activation.
use super::backup_transport::BackupTransport;
use super::AuthClient;
use cloud_backup_protocol::v2::{
    access::{InventoryPutRequest, InventoryPutResult},
    blobs::{BlobReadItem, BlobReadRequest, BlobReadResponse},
    context::{ContextReadRequest, ContextReadResponse},
    discovery::{StreamsListRequest, StreamsListResponse},
    inventory::InventoryProof,
    jobs::{JobBeginRequest, JobCloseRequest, JobResult},
    publication::{Commit, CommitResponse, CommitStatusRequest, HeadReadRequest, HeadReadResult},
    transfer::{
        TransferBatchResponse, TransferIdentityRequest, TransferInput, TransferItemResult,
        TransferMode, TransferPrepareRequest, TransferResumeResponse, TransferResumeResult,
        TransferStatusRequest,
    },
    Begin, Stream, Version,
};
use cloud_backup_protocol::ErrorCode;
use reqwest::header::HeaderMap;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
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

#[derive(Serialize)]
struct ShareRecoveryRequest<'a> {
    stream: &'a Stream,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ShareRecoveryStatus {
    stream: Stream,
    max_log_checkpoints: u8,
}

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
    validated_rpc!(list(StreamsListRequest) -> StreamsListResponse : "streams/list", validate_for);
    validated_rpc!(put_inventory(InventoryPutRequest) -> InventoryPutResult : "inventory/put", validate_for);
    validated_rpc!(blob_read(BlobReadRequest) -> BlobReadResponse : "blobs/read", validate_for);
    validated_rpc!(begin(JobBeginRequest) -> JobResult : "jobs/begin", validate_for_begin);
    validated_rpc!(close(JobCloseRequest) -> JobResult : "jobs/close", validate_for_close);
    identity_rpc!(resume -> TransferResumeResult : "transfers/resume");
    identity_rpc!(complete -> TransferItemResult : "transfers/complete");

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
            .open_bounded(bytes.get(), None, url.as_str(), expires_ms.get())
            .await
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
        self.reader().post_head("heads/read", request).await
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

    pub async fn share_recovery_limit(&self, stream: &Stream) -> Result<Option<u8>> {
        stream.validate()?;
        let req = ShareRecoveryRequest { stream };
        let r: ShareRecoveryStatus = match self.post("streams/share-recovery/status", &req).await {
            Ok(r) => r,
            Err(BackupTransportError::Http(404)) => return Ok(None),
            Err(BackupTransportError::Protocol(e))
                if (e.code == ErrorCode::NotFound && e.field == "http.route")
                    || (e.code == ErrorCode::UnsupportedCapability
                        && e.field == "guest.disabled") =>
            {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        (&r.stream == stream && (1..=8).contains(&r.max_log_checkpoints))
            .then_some(Some(r.max_log_checkpoints))
            .ok_or(BackupTransportError::InvalidResponse(
                "share recovery capability",
            ))
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
