//! Provider observations never become receipts. Exact saved inputs survive retries.
use super::{worker::Work, BackupError, Result};
use cloud_backup_protocol::transfer::*;
use cloud_backup_protocol::Version;
use std::io::Read;

pub(super) fn verified(input: &TransferInput, receipt: ObjectReceipt) -> Result<TransferView> {
    let view = TransferView {
        version: Version,
        request_id: input.request_id.clone(),
        input_sha256: input.input_sha256()?,
        state: TransferStatus::Verified { receipt },
    };
    view.validate_for(input)?;
    Ok(view)
}

pub(super) async fn batch<F>(
    work: &Work<'_>,
    inputs: Vec<TransferInput>,
    open: F,
) -> Result<Vec<TransferView>>
where
    F: Fn(usize) -> Result<Box<dyn Read + Send>>,
{
    work.check_close()?;
    let request = TransferPrepareRequest {
        version: Version,
        items: inputs,
    };
    let response = work.client.prepare(&request).await?;
    let open = &open;
    let futures = request.items.iter().zip(response.items).enumerate().map(
        |(index, (input, result))| async move {
            let transfer = match result {
                TransferItemResult::Ok { transfer } => transfer,
                TransferItemResult::Error { error, .. } => {
                    return Err(super::NativeBackupError::Protocol(error).into())
                }
            };
            settle(work, input, transfer, || open(index)).await
        },
    );
    // Settle every sibling before returning any error. A positive response is
    // still authoritative on retry, and normal failures release source readers.
    let results = futures::future::join_all(futures).await;
    let _ = super::usage::refresh(work.store, work.client, &work.source.account).await;
    results.into_iter().collect()
}

async fn settle<F>(
    work: &Work<'_>,
    input: &TransferInput,
    mut view: TransferView,
    open: F,
) -> Result<TransferView>
where
    F: Fn() -> Result<Box<dyn Read + Send>>,
{
    for attempt in 0..8u32 {
        work.check_close()?;
        view.validate_for(input)?;
        match view.state {
            TransferStatus::Verified { .. } => return Ok(view),
            TransferStatus::Rejected { .. } => return Err(BackupError::Source),
            TransferStatus::Pending { .. } => {}
        }
        let mut upload_error = None;
        match work.client.resume(input).await? {
            TransferResumeResult::Grant { response } => {
                work.check_close()?;
                upload_error = work
                    .client
                    .upload(input, &response.grant, open()?)
                    .await
                    .err();
            }
            TransferResumeResult::State { transfer } => {
                if matches!(
                    transfer.state,
                    TransferStatus::Verified { .. } | TransferStatus::Rejected { .. }
                ) {
                    view = transfer;
                    continue;
                }
            }
            TransferResumeResult::Error { error, .. } => {
                return Err(super::NativeBackupError::Protocol(error).into())
            }
        }
        // Resolve positive evidence even after a lost PUT response. A timeout
        // does not discard the input, source bytes, hold or service obligation.
        let result = work.client.complete(input).await?;
        let transfer = match result {
            TransferItemResult::Ok { transfer } => transfer,
            TransferItemResult::Error { error, .. } => {
                return Err(super::NativeBackupError::Protocol(error).into())
            }
        };
        view = transfer;
        if matches!(view.state, TransferStatus::Verified { .. }) {
            return Ok(view);
        }
        if let Some(error) = upload_error {
            return Err(error.into());
        }
        if attempt < 7 {
            pause(work, attempt).await?;
        }
    }
    Err(BackupError::Waiting)
}

pub(super) async fn pause(work: &Work<'_>, attempt: u32) -> Result<()> {
    let seconds = (1u64 << attempt.min(5)).min(30);
    // Small slices let a close intent stop new effects promptly between calls.
    for _ in 0..seconds {
        work.check_close()?;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    work.check_close()
}
