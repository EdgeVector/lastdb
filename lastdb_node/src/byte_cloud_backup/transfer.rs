use super::{work, BackupError, Result};
use crate::cloud_backup_client::byte_backup::{BackupTransportError, ByteBackupClient};
use async_trait::async_trait;
use cloud_backup_protocol::v2::blobs::{BlobPath, BlobReadItem, BlobReadRequest};
use cloud_backup_protocol::v2::context::ContextReadRequest;
use cloud_backup_protocol::v2::transfer::*;
use cloud_backup_protocol::v2::{Destination, Version};
use cloud_backup_protocol::{AccountId, CloudScope};
use fold_db::fold_db_core::{BlobFetch, RecordBlobSource, SIGN_IN_TO_FETCH_FILE};
use std::cell::RefCell;
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::AsyncReadExt;

pub(super) async fn upload_batch(
    client: &ByteBackupClient,
    inputs: &[TransferInput],
    context: &work::Context<'_>,
    mut open_reader: impl FnMut(usize) -> Result<Box<dyn Read + Send>>,
    mut on_receipt: impl FnMut(usize, ObjectReceipt) -> Result<()>,
) -> Result<Option<BackupError>> {
    let (items, error) = prepare(client, inputs, context.pause).await?;
    if let Some(error) = error {
        for (pos, item) in items.into_iter().enumerate() {
            if let Ok(Some(receipt)) = verified(item) {
                on_receipt(pos, receipt)?;
            }
        }
        return Ok(Some(error));
    }
    let mut futures = Vec::with_capacity(inputs.len());
    for (pos, (input, item)) in inputs.iter().zip(items).enumerate() {
        futures.push(complete(client, input, item, open_reader(pos)?, context));
    }
    let results = futures::future::join_all(futures).await;
    let mut failure = None;
    for (pos, (input, result)) in inputs.iter().zip(results).enumerate() {
        match result {
            Ok(receipt) if receipt.object == input.object => on_receipt(pos, receipt)?,
            Ok(_) => work::record_error(&mut failure, BackupError::Identity),
            Err(error) => work::record_error(&mut failure, error),
        }
    }
    Ok(failure)
}

async fn prepare(
    client: &ByteBackupClient,
    inputs: &[TransferInput],
    pause: &AtomicBool,
) -> Result<(Vec<TransferItemResult>, Option<BackupError>)> {
    let request = TransferPrepareRequest {
        version: Version::V2,
        items: inputs.to_vec(),
    };
    let observed = RefCell::new(None);
    let result = work::control("transfer_prepare", pause, || async {
        let response = client.prepare(&request).await?;
        let error = batch_error(&response.items);
        let items = remember(&mut observed.borrow_mut(), response.items)?;
        error.map_or(Ok(items), Err)
    })
    .await;
    match result {
        Ok(items) => Ok((items, None)),
        Err(error) => {
            if let Some(items) = observed.into_inner() {
                Ok((items, Some(error)))
            } else {
                Err(error)
            }
        }
    }
}

fn batch_error(items: &[TransferItemResult]) -> Option<BackupError> {
    let mut errors: Vec<_> = items
        .iter()
        .filter_map(|item| match item {
            TransferItemResult::Error { error, .. } => Some(cloud_error(error.clone())),
            TransferItemResult::Ok { transfer }
                if matches!(transfer.state, TransferStatus::Rejected { .. }) =>
            {
                Some(BackupError::Source)
            }
            TransferItemResult::Ok { .. } => None,
        })
        .collect();
    let index = errors
        .iter()
        .position(|error| !work::temporary(error))
        .unwrap_or(0);
    (!errors.is_empty()).then(|| errors.swap_remove(index))
}

fn remember(
    observed: &mut Option<Vec<TransferItemResult>>,
    mut items: Vec<TransferItemResult>,
) -> Result<Vec<TransferItemResult>> {
    if let Some(previous) = observed.as_ref() {
        for (old, item) in previous.iter().zip(&mut items) {
            if matches!(old, TransferItemResult::Ok { transfer } if matches!(transfer.state, TransferStatus::Verified { .. } | TransferStatus::Rejected { .. }))
            {
                if matches!(item, TransferItemResult::Ok { transfer } if matches!(transfer.state, TransferStatus::Verified { .. } | TransferStatus::Rejected { .. }))
                    && item != old
                {
                    return Err(BackupError::Identity);
                }
                *item = old.clone();
            }
        }
    }
    *observed = Some(items.clone());
    Ok(items)
}

/// An explicit invocation makes at most one conditional PUT for this frozen input.
async fn complete(
    client: &ByteBackupClient,
    input: &TransferInput,
    prepared: TransferItemResult,
    reader: Box<dyn Read + Send>,
    context: &work::Context<'_>,
) -> Result<ObjectReceipt> {
    let result = complete_one(client, input, prepared, reader, context).await;
    context.note_result(&result);
    result
}

async fn complete_one(
    client: &ByteBackupClient,
    input: &TransferInput,
    prepared: TransferItemResult,
    reader: Box<dyn Read + Send>,
    context: &work::Context<'_>,
) -> Result<ObjectReceipt> {
    let pause = context.pause;
    check_pause(pause)?;
    if let Some(receipt) = verified(prepared)? {
        return Ok(receipt);
    }
    context.check()?;
    let resumed = work::control("transfer_resume", pause, || async {
        context.check()?;
        match client.resume(input).await? {
            TransferResumeResult::Error { error, .. } => Err(cloud_error(error)),
            result => Ok(result),
        }
    })
    .await?;
    match resumed {
        TransferResumeResult::Grant { response } => {
            context.check()?;
            context.check_upload(input, &response.grant)?;
            let _observation = client.upload(input, &response, reader).await;
        }
        TransferResumeResult::State { transfer } => {
            drop(reader);
            match transfer.state {
                TransferStatus::Verified { receipt } => return Ok(receipt),
                TransferStatus::Rejected { .. } => return Err(BackupError::Source),
                TransferStatus::Pending { .. } => {}
            }
        }
        TransferResumeResult::Error { error, .. } => return Err(cloud_error(error)),
    }
    check_pause(pause)?;
    let completed = work::control("transfer_complete", pause, || async {
        match client.complete(input).await? {
            TransferItemResult::Error { error, .. } => Err(cloud_error(error)),
            item => Ok(item),
        }
    })
    .await?;
    if let Some(receipt) = verified(completed)? {
        return Ok(receipt);
    }
    let request = TransferStatusRequest {
        version: Version::V2,
        job: input.job.clone(),
        request_ids: vec![input.request_id.clone()],
    };
    work::poll(context, "transfer_status", || async {
        let reply = client
            .transfer_status(&request, std::slice::from_ref(input))
            .await?;
        verified(reply.items.into_iter().next().ok_or(BackupError::Invalid)?)
    })
    .await
}

fn verified(value: TransferItemResult) -> Result<Option<ObjectReceipt>> {
    match value {
        TransferItemResult::Ok { transfer } => match transfer.state {
            TransferStatus::Verified { receipt } => Ok(Some(receipt)),
            TransferStatus::Pending { .. } => Ok(None),
            TransferStatus::Rejected { .. } => Err(BackupError::Source),
        },
        TransferItemResult::Error { error, .. } => Err(cloud_error(error)),
    }
}

pub(super) fn check_pause(pause: &AtomicBool) -> Result<()> {
    if pause.load(Ordering::Acquire) {
        Err(BackupError::Shutdown)
    } else {
        Ok(())
    }
}

pub(super) fn cloud_error(error: cloud_backup_protocol::ProtocolError) -> BackupError {
    BackupTransportError::Protocol(error).into()
}

/// Register the cloud source a personal home uses for non-local record blobs.
pub fn register_blob_source(host: &crate::host::Host) {
    let source: Arc<dyn RecordBlobSource> = match blob_source(host) {
        Ok(source) => Arc::new(source),
        Err(reason) => {
            if matches!(crate::cloud::load_saved_credentials(&host.home), Ok(None)) {
                Arc::new(SignInRequired)
            } else {
                tracing::debug!(
                    target: "lastdbd::blob_source",
                    %reason,
                    "record files download on request is off"
                );
                return;
            }
        }
    };
    host.db.set_record_blob_source(Some(source));
}

struct SignInRequired;

#[async_trait]
impl RecordBlobSource for SignInRequired {
    async fn fetch(&self, _path: &BlobPath, _sealed_bytes: u64) -> BlobFetch {
        BlobFetch::Unavailable(SIGN_IN_TO_FETCH_FILE.to_owned())
    }
}

fn blob_source(host: &crate::host::Host) -> std::result::Result<CloudBlobSource, String> {
    let client = super::client_from_home(&host.home)?;
    let account = AccountId::try_from(host.user_hash.clone())
        .map_err(|_| "the node account id is not a cloud account id".to_owned())?;
    Ok(CloudBlobSource {
        client,
        account,
        scope: tokio::sync::Mutex::new(None),
    })
}

struct CloudBlobSource {
    client: ByteBackupClient,
    account: AccountId,
    scope: tokio::sync::Mutex<Option<CloudScope>>,
}

#[async_trait]
impl RecordBlobSource for CloudBlobSource {
    async fn fetch(&self, path: &BlobPath, sealed_bytes: u64) -> BlobFetch {
        match self.read(path, sealed_bytes).await {
            Ok(fetch) => fetch,
            Err(reason) => BlobFetch::Unavailable(reason),
        }
    }
}

impl CloudBlobSource {
    async fn scope(&self) -> std::result::Result<CloudScope, String> {
        let mut cached = self.scope.lock().await;
        if let Some(scope) = cached.as_ref() {
            return Ok(scope.clone());
        }
        let context = self
            .client
            .context(&ContextReadRequest {
                version: Version::V2,
                account_id: self.account.clone(),
                required_destinations: vec![Destination::B2],
            })
            .await
            .map_err(|error| format!("cloud account context: {error}"))?;
        *cached = Some(context.scope.clone());
        Ok(context.scope)
    }

    async fn read(
        &self,
        path: &BlobPath,
        sealed_bytes: u64,
    ) -> std::result::Result<BlobFetch, String> {
        let request = BlobReadRequest {
            version: Version::V2,
            scope: self.scope().await?,
            paths: vec![path.clone()],
        };
        let response = match self.client.blob_read(&request).await {
            Ok(response) => response,
            Err(error) => {
                *self.scope.lock().await = None;
                return Err(format!("cloud blob read: {error}"));
            }
        };
        let item = response
            .items
            .into_iter()
            .next()
            .ok_or_else(|| "cloud blob read returned no result".to_owned())?;
        match &item {
            BlobReadItem::Absent { .. } => return Ok(BlobFetch::Absent),
            BlobReadItem::Grant { bytes, .. } if bytes.get() != sealed_bytes => {
                return Err(format!(
                    "the cloud object has {} bytes; the record's file needs {sealed_bytes}",
                    bytes.get()
                ));
            }
            BlobReadItem::Grant { .. } => {}
        }
        let mut stream = self
            .client
            .open_blob(&item)
            .await
            .map_err(|error| format!("cloud blob download: {error}"))?;
        let mut row = Vec::with_capacity(usize::try_from(sealed_bytes).unwrap_or(0));
        stream
            .read_to_end(&mut row)
            .await
            .map_err(|error| format!("cloud blob download: {error}"))?;
        Ok(BlobFetch::Bytes(row))
    }
}
