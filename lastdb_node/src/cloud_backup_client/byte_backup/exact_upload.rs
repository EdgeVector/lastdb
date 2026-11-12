//! Upload one caller-supplied blob through the ordinary byte-backup methods.
//!
//! The caller keeps the begin value, transfer input, and sealed bytes, and
//! stores a verified receipt before it closes the job. This module does not
//! choose a stream id, read a stream head, or close the job.

use super::{BackupTransportError, ByteBackupClient, Result};
use cloud_backup_protocol::v2::jobs::{JobBeginRequest, JobResult, JobStatus};
use cloud_backup_protocol::v2::transfer::{
    ObjectReceipt, TransferBatchResponse, TransferInput, TransferItemResult,
    TransferPrepareRequest, TransferResumeResult, TransferStatus, TransferStatusRequest,
};
use cloud_backup_protocol::v2::Version;
use cloud_backup_protocol::{bytes_sha256, ErrorCode, ProtocolError};
use std::io::Cursor;

/// Outcome of one exact upload attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExactUpload {
    /// The service verified these bytes. Store this receipt before closing the job.
    Verified(ObjectReceipt),
    /// The same saved begin, input, and bytes can be sent again.
    Retryable(&'static str),
}

enum Step {
    Stop(ExactUpload),
    Continue,
}

impl ByteBackupClient {
    /// Send one saved begin, transfer input, and sealed object.
    ///
    /// A verified receipt names `input.object`. A retryable result uses the
    /// same saved values. A closed or published job, a rejected object, and a
    /// multipart grant are terminal.
    pub async fn upload_exact(
        &self,
        begin: &JobBeginRequest,
        input: &TransferInput,
        sealed: Vec<u8>,
    ) -> Result<ExactUpload> {
        ready(begin, input, &sealed)?;
        if let Some(stopped) = active_job(self.begin(begin).await)? {
            return Ok(stopped);
        }
        let prepared = match self.prepare(&one_prepare(input)).await {
            Ok(batch) => only_item(batch)?,
            Err(error) => return held("prepare", error),
        };
        match read_item("prepare", input, prepared)? {
            Step::Stop(upload) => Ok(upload),
            Step::Continue => self.put_and_complete(input, sealed).await,
        }
    }

    async fn put_and_complete(
        &self,
        input: &TransferInput,
        sealed: Vec<u8>,
    ) -> Result<ExactUpload> {
        match self.resume(input).await {
            Ok(TransferResumeResult::Grant { response }) => {
                if response.grant.transfer_mode.is_some() {
                    return Err(invalid("exact_upload.transfer_mode"));
                }
                // The service verifies the stored bytes. The provider reply is not a receipt.
                drop(
                    self.upload(input, &response, Box::new(Cursor::new(sealed)))
                        .await,
                );
            }
            Ok(TransferResumeResult::State { transfer }) => {
                return match read_state(input, transfer.state)? {
                    Step::Stop(upload) => Ok(upload),
                    Step::Continue => self.complete_once(input).await,
                };
            }
            Ok(TransferResumeResult::Error { error, .. }) => return held_protocol("resume", error),
            Err(error) => return held("resume", error),
        }
        self.complete_once(input).await
    }

    async fn complete_once(&self, input: &TransferInput) -> Result<ExactUpload> {
        let item = match self.complete(input).await {
            Ok(item) => item,
            Err(error) => return held("complete", error),
        };
        match read_item("complete", input, item)? {
            Step::Stop(upload) => Ok(upload),
            Step::Continue => self.status_once(input).await,
        }
    }

    async fn status_once(&self, input: &TransferInput) -> Result<ExactUpload> {
        let request = TransferStatusRequest {
            version: Version::V2,
            job: input.job.clone(),
            request_ids: vec![input.request_id.clone()],
        };
        let item = match self
            .transfer_status(&request, std::slice::from_ref(input))
            .await
        {
            Ok(batch) => only_item(batch)?,
            Err(error) => return held("status", error),
        };
        match read_item("status", input, item)? {
            Step::Stop(upload) => Ok(upload),
            Step::Continue => Ok(ExactUpload::Retryable("pending")),
        }
    }
}

fn active_job(result: Result<JobResult>) -> Result<Option<ExactUpload>> {
    match result {
        Ok(job) if matches!(job.status, JobStatus::Active) => Ok(None),
        Ok(_) => Err(invalid("exact_upload.job")),
        Err(error) => held("begin", error).map(Some),
    }
}

fn read_item(step: &'static str, input: &TransferInput, item: TransferItemResult) -> Result<Step> {
    match item {
        TransferItemResult::Ok { transfer } => read_state(input, transfer.state),
        TransferItemResult::Error { error, .. } => held_protocol(step, error).map(Step::Stop),
    }
}

fn read_state(input: &TransferInput, state: TransferStatus) -> Result<Step> {
    match state {
        TransferStatus::Verified { receipt } => verified(input, receipt).map(Step::Stop),
        TransferStatus::Rejected { .. } => Err(invalid("exact_upload.rejected")),
        TransferStatus::Pending { .. } => Ok(Step::Continue),
    }
}

fn verified(input: &TransferInput, receipt: ObjectReceipt) -> Result<ExactUpload> {
    receipt.validate()?;
    if receipt.scope != input.job.stream.scope || receipt.object != input.object {
        return Err(invalid("exact_upload.receipt"));
    }
    Ok(ExactUpload::Verified(receipt))
}

fn held_protocol(step: &'static str, error: ProtocolError) -> Result<ExactUpload> {
    held(step, BackupTransportError::Protocol(error))
}

fn held(step: &'static str, error: BackupTransportError) -> Result<ExactUpload> {
    if retryable(&error) {
        Ok(ExactUpload::Retryable(step))
    } else {
        Err(error)
    }
}

fn retryable(error: &BackupTransportError) -> bool {
    match error {
        BackupTransportError::Protocol(error) => matches!(
            error.code,
            ErrorCode::QuotaExceeded
                | ErrorCode::RetryLater
                | ErrorCode::ProviderPending
                | ErrorCode::OutcomeUnknown
        ),
        BackupTransportError::Timeout | BackupTransportError::Transport => true,
        BackupTransportError::Http(status) => matches!(*status, 408 | 429 | 500 | 502 | 503 | 504),
        _ => false,
    }
}

fn only_item(mut batch: TransferBatchResponse) -> Result<TransferItemResult> {
    if batch.items.len() == 1 {
        return Ok(batch.items.remove(0));
    }
    Err(BackupTransportError::InvalidResponse("exact_upload.batch"))
}

fn one_prepare(input: &TransferInput) -> TransferPrepareRequest {
    TransferPrepareRequest {
        version: Version::V2,
        items: vec![input.clone()],
    }
}

fn ready(begin: &JobBeginRequest, input: &TransferInput, sealed: &[u8]) -> Result<()> {
    begin.validate()?;
    input.validate()?;
    if input.job != begin.job {
        return Err(invalid("exact_upload.job"));
    }
    let Some(path) = &input.blob else {
        return Err(invalid("exact_upload.blob"));
    };
    if path.database != input.job.stream.stream_id {
        return Err(invalid("exact_upload.blob_scope"));
    }
    let len = u64::try_from(sealed.len()).map_err(|_| invalid("exact_upload.bytes"))?;
    if input.object.bytes.get() != len || input.object.sha256 != bytes_sha256(sealed) {
        return Err(invalid("exact_upload.bytes"));
    }
    Ok(())
}

fn invalid(field: &'static str) -> BackupTransportError {
    ProtocolError::invalid(field).into()
}
