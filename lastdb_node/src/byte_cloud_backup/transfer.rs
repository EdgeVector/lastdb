use super::{BackupError, Result};
use cloud_backup_protocol::v2::transfer::*;
use cloud_backup_protocol::v2::Version;
use fold_db::sync::auth::byte_backup::{ByteBackupClient, NativeBackupError};
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};

pub(super) async fn prepare(
    client: &ByteBackupClient,
    inputs: &[TransferInput],
) -> Result<Vec<TransferItemResult>> {
    let request = TransferPrepareRequest {
        version: Version::V2,
        items: inputs.to_vec(),
    };
    Ok(client.prepare(&request).await?.items)
}

/// An explicit invocation makes at most one conditional PUT for this frozen input.
/// Polls never repeat PUT or queue admission. Uncertain outcomes retain the source.
pub(super) async fn complete(
    client: &ByteBackupClient,
    input: &TransferInput,
    prepared: TransferItemResult,
    reader: Box<dyn Read + Send>,
    pause: &AtomicBool,
) -> Result<ObjectReceipt> {
    check_pause(pause)?;
    if let Some(receipt) = verified(prepared)? {
        return Ok(receipt);
    }
    let resumed = client.resume(input).await?;
    match resumed {
        TransferResumeResult::Grant { response } => {
            check_pause(pause)?;
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
    if let Some(receipt) = verified(client.complete(input).await?)? {
        return Ok(receipt);
    }
    let request = TransferStatusRequest {
        version: Version::V2,
        job: input.job.clone(),
        request_ids: vec![input.request_id.clone()],
    };
    for attempt in 0..16 {
        check_pause(pause)?;
        let reply = client
            .transfer_status(&request, std::slice::from_ref(input))
            .await?;
        let item = reply.items.into_iter().next().ok_or(BackupError::Invalid)?;
        if let Some(receipt) = verified(item)? {
            return Ok(receipt);
        }
        tokio::time::sleep(std::time::Duration::from_millis(
            250 * (attempt + 1).min(12),
        ))
        .await;
    }
    Err(BackupError::Waiting)
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
    NativeBackupError::Protocol(error).into()
}
