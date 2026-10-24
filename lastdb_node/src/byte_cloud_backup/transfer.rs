use super::{retry, wait, BackupError, Result};
use cloud_backup_protocol::v2::transfer::*;
use cloud_backup_protocol::v2::Version;
use fold_db::sync::auth::byte_backup::{ByteBackupClient, NativeBackupError};
use std::cell::RefCell;
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};

pub(super) struct PreparedBatch {
    pub items: Vec<TransferItemResult>,
    pub error: Option<BackupError>,
}

pub(super) async fn prepare(
    client: &ByteBackupClient,
    inputs: &[TransferInput],
    pause: &AtomicBool,
) -> Result<PreparedBatch> {
    let request = TransferPrepareRequest {
        version: Version::V2,
        items: inputs.to_vec(),
    };
    let observed = RefCell::new(None);
    let result = retry::control("transfer_prepare", pause, || async {
        let response = client.prepare(&request).await?;
        let error = batch_error(&response.items);
        let items = remember(&mut observed.borrow_mut(), response.items)?;
        error.map_or(Ok(items), Err)
    })
    .await;
    match result {
        Ok(items) => Ok(PreparedBatch { items, error: None }),
        Err(error) => match observed.into_inner() {
            Some(items) => Ok(PreparedBatch {
                items,
                error: Some(error),
            }),
            None => Err(error),
        },
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
        .position(|error| !retry::temporary(error))
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
/// Polls never repeat PUT or queue admission. Uncertain outcomes retain the source.
pub(super) async fn complete(
    client: &ByteBackupClient,
    input: &TransferInput,
    prepared: TransferItemResult,
    reader: Box<dyn Read + Send>,
    context: &wait::Context<'_>,
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
    context: &wait::Context<'_>,
) -> Result<ObjectReceipt> {
    let pause = context.pause;
    check_pause(pause)?;
    if let Some(receipt) = verified(prepared)? {
        return Ok(receipt);
    }
    context.check()?;
    let resumed = resume_with_retry(client, input, context).await?;
    match resumed {
        TransferResumeResult::Grant { response } => {
            context.check()?;
            // An HTTP outcome, including a timeout, never proves stored bytes.
            // Complete resolves this same transfer through service authority.
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
    let completed = retry::control("transfer_complete", pause, || async {
        let item = client.complete(input).await?;
        match item {
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
    wait::poll(context, "transfer_status", || async {
        let reply = client
            .transfer_status(&request, std::slice::from_ref(input))
            .await?;
        verified(reply.items.into_iter().next().ok_or(BackupError::Invalid)?)
    })
    .await
}

/// Grant retries reuse the exact identity and precede the single PUT opportunity.
async fn resume_with_retry(
    client: &ByteBackupClient,
    input: &TransferInput,
    context: &wait::Context<'_>,
) -> Result<TransferResumeResult> {
    retry::control("transfer_resume", context.pause, || async {
        context.check()?;
        let result = client.resume(input).await;
        match result? {
            TransferResumeResult::Error { error, .. } => Err(cloud_error(error)),
            result => Ok(result),
        }
    })
    .await
}

pub(super) fn verified(value: TransferItemResult) -> Result<Option<ObjectReceipt>> {
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
    NativeBackupError::Protocol(error).into()
}
