//! Temporary control refusals retain exact identities and successful batch slots.
use super::{worker::Work, BackupError, Result};
use cloud_backup_protocol::transfer::*;
use cloud_backup_protocol::Version;
use tokio::time::{Duration, Instant};

pub(super) const CONTROL_WAIT: Duration = Duration::from_secs(30);

pub(super) async fn prepare(
    work: &Work<'_>,
    inputs: &[TransferInput],
    deadline: Instant,
) -> Result<Vec<TransferView>> {
    let mut slots = vec![None; inputs.len()];
    let mut attempt = 0;
    loop {
        work.check_close()?;
        let positions: Vec<_> = slots
            .iter()
            .enumerate()
            .filter_map(|(i, value)| value.is_none().then_some(i))
            .collect();
        if positions.is_empty() {
            return slots
                .into_iter()
                .collect::<Option<Vec<_>>>()
                .ok_or(BackupError::Invalid);
        }
        let request = TransferPrepareRequest {
            version: Version,
            items: positions.iter().map(|i| inputs[*i].clone()).collect(),
        };
        let result = tokio::time::timeout_at(deadline, work.client.prepare(&request))
            .await
            .map_err(|_| BackupError::Waiting)?;
        match result {
            Ok(response) => {
                for (index, result) in positions.into_iter().zip(response.items) {
                    match result {
                        TransferItemResult::Ok { transfer } => slots[index] = Some(transfer),
                        TransferItemResult::Error { error, .. } => {
                            temporary(BackupError::Cloud(error.into()))?
                        }
                    }
                }
            }
            Err(error) => temporary(error.into())?,
        }
        if slots.iter().any(Option::is_none) {
            pause(work, deadline, attempt).await?;
            attempt = attempt.saturating_add(1);
        }
    }
}

pub(super) async fn resume(
    work: &Work<'_>,
    input: &TransferInput,
    deadline: Instant,
) -> Result<TransferResumeResult> {
    let mut attempt = 0;
    loop {
        work.check_close()?;
        let result = tokio::time::timeout_at(deadline, work.client.resume(input))
            .await
            .map_err(|_| BackupError::Waiting)?;
        match result {
            Ok(TransferResumeResult::Error { error, .. }) => {
                temporary(BackupError::Cloud(error.into()))?
            }
            Ok(result) => return Ok(result),
            Err(error) => temporary(error.into())?,
        }
        pause(work, deadline, attempt).await?;
        attempt = attempt.saturating_add(1);
    }
}

fn temporary(error: BackupError) -> Result<()> {
    if error.retryable() {
        Ok(())
    } else {
        Err(error)
    }
}

async fn pause(work: &Work<'_>, deadline: Instant, attempt: u32) -> Result<()> {
    let base = (100u64 << attempt.min(5)).min(3_000);
    let delay = Duration::from_millis(base + rand::random::<u64>() % base);
    let until = (Instant::now() + delay).min(deadline);
    while Instant::now() < until {
        work.check_close()?;
        tokio::time::sleep_until((Instant::now() + Duration::from_millis(100)).min(until)).await;
    }
    work.check_close()?;
    if Instant::now() >= deadline {
        Err(BackupError::Waiting)
    } else {
        Ok(())
    }
}
