use super::{work, BackupError, Result};
use crate::cloud_backup_client::byte_backup::{BackupTransportError, ByteBackupClient};
use async_trait::async_trait;
use cloud_backup_protocol::v2::{
    blobs::{BlobPath, BlobReadItem, BlobReadRequest},
    context::ContextReadRequest,
    transfer::*,
    Destination, Version,
};
use cloud_backup_protocol::{AccountId, CloudScope, ProtocolError};
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
    let mut futs = Vec::with_capacity(inputs.len());
    for (pos, (input, item)) in inputs.iter().zip(items).enumerate() {
        let reader = open_reader(pos)?;
        futs.push(async move {
            let res = complete_one(client, input, item, reader, context).await;
            if res.is_err() {
                context.stopped.store(true, Ordering::Release);
            }
            res
        });
    }
    let results = futures::future::join_all(futs).await;
    let rank = |e: &BackupError| match e {
        BackupError::Waiting => 0,
        e if work::temporary(e) => 1,
        _ => 2,
    };
    let mut failure: Option<BackupError> = None;
    for (pos, (input, res)) in inputs.iter().zip(results).enumerate() {
        let err = match res {
            Ok(r) if r.object == input.object => {
                on_receipt(pos, r)?;
                continue;
            }
            Ok(_) => BackupError::Identity,
            Err(e) => e,
        };
        if failure.as_ref().is_none_or(|old| rank(&err) > rank(old)) {
            failure = Some(err);
        }
    }
    Ok(failure)
}

async fn prepare(
    client: &ByteBackupClient,
    inputs: &[TransferInput],
    pause: &AtomicBool,
) -> Result<(Vec<TransferItemResult>, Option<BackupError>)> {
    let req = TransferPrepareRequest {
        version: Version::V2,
        items: inputs.to_vec(),
    };
    let observed: RefCell<Option<Vec<TransferItemResult>>> = RefCell::new(None);
    let res = work::control("transfer_prepare", pause, || async {
        let mut items = client.prepare(&req).await?.items;
        let error = batch_error(&items);
        if let Some(prev) = observed.borrow().as_ref() {
            for (old, item) in prev.iter().zip(&mut items) {
                if is_settled(old) {
                    if is_settled(item) && item != old {
                        return Err(BackupError::Identity);
                    }
                    *item = old.clone();
                }
            }
        }
        *observed.borrow_mut() = Some(items.clone());
        error.map_or(Ok(items), Err)
    })
    .await;
    match res {
        Ok(items) => Ok((items, None)),
        Err(e) => {
            if let Some(items) = observed.into_inner() {
                Ok((items, Some(e)))
            } else {
                Err(e)
            }
        }
    }
}

fn is_settled(item: &TransferItemResult) -> bool {
    matches!(
        item,
        TransferItemResult::Ok { transfer }
            if matches!(transfer.state, TransferStatus::Verified { .. } | TransferStatus::Rejected { .. })
    )
}

fn batch_error(items: &[TransferItemResult]) -> Option<BackupError> {
    let mut first = None;
    for item in items {
        let err = match item {
            TransferItemResult::Error { error, .. } => cloud_error(error.clone()),
            TransferItemResult::Ok { transfer }
                if matches!(transfer.state, TransferStatus::Rejected { .. }) =>
            {
                BackupError::Source
            }
            TransferItemResult::Ok { .. } => continue,
        };
        if !work::temporary(&err) {
            return Some(err);
        }
        first.get_or_insert(err);
    }
    first
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
            res => Ok(res),
        }
    })
    .await?;
    match resumed {
        TransferResumeResult::Grant { response } => {
            context.check()?;
            let begin = context.saved.begin.as_ref().ok_or(BackupError::Invalid)?;
            if begin.job != input.job || response.grant.transfer_mode.is_some() {
                return Err(BackupError::Identity);
            }
            let _obs = client.upload(input, &response, reader).await;
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
    let req = TransferStatusRequest {
        version: Version::V2,
        job: input.job.clone(),
        request_ids: vec![input.request_id.clone()],
    };
    work::poll(context, "transfer_status", || async {
        let reply = client
            .transfer_status(&req, std::slice::from_ref(input))
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
    (!pause.load(Ordering::Acquire))
        .then_some(())
        .ok_or(BackupError::Shutdown)
}

pub(super) fn cloud_error(error: ProtocolError) -> BackupError {
    BackupTransportError::Protocol(error).into()
}

/// Register the cloud source a personal home uses for non-local record blobs.
pub fn register_blob_source(host: &crate::host::Host) {
    let has_saved = !matches!(crate::cloud::load_saved_credentials(&host.home), Ok(None));
    let acct = AccountId::try_from(host.user_hash.clone())
        .map_err(|_| "the node account id is not a cloud account id".to_owned());
    let source: Arc<dyn RecordBlobSource> = match super::client_from_home(&host.home)
        .and_then(|c| Ok((c, acct?)))
    {
        Ok((client, account)) => Arc::new(CloudBlobSource {
            client,
            account,
            scope: tokio::sync::Mutex::new(None),
        }),
        Err(reason) if has_saved => {
            tracing::debug!(target: "lastdbd::blob_source", %reason, "record files download on request is off");
            return;
        }
        Err(_) => Arc::new(SignInRequired),
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

struct CloudBlobSource {
    client: ByteBackupClient,
    account: AccountId,
    scope: tokio::sync::Mutex<Option<CloudScope>>,
}

#[async_trait]
impl RecordBlobSource for CloudBlobSource {
    async fn fetch(&self, path: &BlobPath, sealed_bytes: u64) -> BlobFetch {
        self.read(path, sealed_bytes)
            .await
            .unwrap_or_else(BlobFetch::Unavailable)
    }
}

impl CloudBlobSource {
    async fn scope(&self) -> std::result::Result<CloudScope, String> {
        let mut cached = self.scope.lock().await;
        if let Some(scope) = cached.as_ref() {
            return Ok(scope.clone());
        }
        let req = ContextReadRequest {
            version: Version::V2,
            account_id: self.account.clone(),
            required_destinations: vec![Destination::B2],
        };
        let ctx = self
            .client
            .context(&req)
            .await
            .map_err(|e| format!("cloud account context: {e}"))?;
        *cached = Some(ctx.scope.clone());
        Ok(ctx.scope)
    }

    async fn read(
        &self,
        path: &BlobPath,
        sealed_bytes: u64,
    ) -> std::result::Result<BlobFetch, String> {
        let req = BlobReadRequest {
            version: Version::V2,
            scope: self.scope().await?,
            paths: vec![path.clone()],
        };
        let resp = match self.client.blob_read(&req).await {
            Ok(r) => r,
            Err(e) => {
                *self.scope.lock().await = None;
                return Err(format!("cloud blob read: {e}"));
            }
        };
        let item = resp
            .items
            .into_iter()
            .next()
            .ok_or("cloud blob read returned no result")?;
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
            .map_err(|e| format!("cloud blob download: {e}"))?;
        let mut row = Vec::with_capacity(usize::try_from(sealed_bytes).unwrap_or(0));
        stream
            .read_to_end(&mut row)
            .await
            .map_err(|e| format!("cloud blob download: {e}"))?;
        Ok(BlobFetch::Bytes(row))
    }
}
