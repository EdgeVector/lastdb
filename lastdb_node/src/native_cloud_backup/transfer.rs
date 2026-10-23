//! Provider observations never become receipts. Exact saved inputs survive retries.
use super::{worker::Work, BackupError, Result};
use cloud_backup_protocol::transfer::*;
use cloud_backup_protocol::Version;
use std::io::Read;
use std::time::Duration;

const VERIFICATION_WAIT: Duration = Duration::from_secs(15 * 60);

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
            submit(work, input, transfer, || open(index)).await
        },
    );
    // Settle every sibling before returning any error. A positive response is
    // still authoritative on retry, and normal failures release source readers.
    let results = futures::future::join_all(futures).await;
    let result = match results.into_iter().collect::<Result<Vec<_>>>() {
        Ok(views) => wait(work, &request.items, views).await,
        Err(error) => Err(error),
    };
    let _ = super::usage::refresh(work.store, work.client, &work.source.account).await;
    result
}

async fn submit<F>(
    work: &Work<'_>,
    input: &TransferInput,
    view: TransferView,
    open: F,
) -> Result<TransferView>
where
    F: Fn() -> Result<Box<dyn Read + Send>>,
{
    work.check_close()?;
    view.validate_for(input)?;
    match view.state {
        TransferStatus::Verified { .. } => return Ok(view),
        TransferStatus::Rejected { .. } => return Err(BackupError::Source),
        TransferStatus::Pending { .. } => {}
    }
    match work.client.resume(input).await? {
        TransferResumeResult::Grant { response } => {
            work.check_close()?;
            // A lost PUT response is not negative evidence. Complete still
            // resolves any bytes that reached the exact create-once target.
            let _observation = work.client.upload(input, &response.grant, open()?).await;
        }
        TransferResumeResult::State { transfer } => {
            if !matches!(transfer.state, TransferStatus::Pending { .. }) {
                return Ok(transfer);
            }
        }
        TransferResumeResult::Error { error, .. } => {
            return Err(super::NativeBackupError::Protocol(error).into())
        }
    }
    work.check_close()?;
    // Once per invocation, including an explicit owner resume after a lost
    // queue send. Subsequent polls do not issue another URL, PUT or queue send.
    item(work.client.complete(input).await?)
}

async fn wait(
    work: &Work<'_>,
    inputs: &[TransferInput],
    mut views: Vec<TransferView>,
) -> Result<Vec<TransferView>> {
    let deadline = tokio::time::Instant::now() + VERIFICATION_WAIT;
    let mut attempt = 0;
    loop {
        work.check_close()?;
        let mut positions = Vec::new();
        let mut pending = Vec::new();
        for (position, (input, view)) in inputs.iter().zip(&views).enumerate() {
            view.validate_for(input)?;
            match view.state {
                TransferStatus::Verified { .. } => {}
                TransferStatus::Rejected { .. } => return Err(BackupError::Source),
                TransferStatus::Pending { .. } => {
                    positions.push(position);
                    pending.push(input.clone());
                }
            }
        }
        if pending.is_empty() {
            return Ok(views);
        }
        tokio::time::timeout_at(deadline, pause(work, attempt))
            .await
            .map_err(|_| BackupError::Waiting)??;
        let request = TransferStatusRequest {
            version: Version,
            job: pending[0].job.clone(),
            request_ids: pending
                .iter()
                .map(|input| input.request_id.clone())
                .collect(),
        };
        let response =
            tokio::time::timeout_at(deadline, work.client.transfer_status(&request, &pending))
                .await
                .map_err(|_| BackupError::Waiting)??;
        for (position, result) in positions.into_iter().zip(response.items) {
            views[position] = item(result)?;
        }
        attempt = attempt.saturating_add(1);
    }
}

fn item(result: TransferItemResult) -> Result<TransferView> {
    match result {
        TransferItemResult::Ok { transfer } => Ok(transfer),
        TransferItemResult::Error { error, .. } => {
            Err(super::NativeBackupError::Protocol(error).into())
        }
    }
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
