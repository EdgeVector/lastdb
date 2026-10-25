//! One finite budget for an exact, validated V2 control request. Never wraps PUT.
use super::{transfer, BackupError, Result};
use cloud_backup_protocol::{ErrorCode, ProtocolError};
use fold_db::sync::auth::byte_backup::BackupTransportError;
use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use tokio::time::Instant;

pub(super) fn temporary_protocol(error: &ProtocolError) -> bool {
    matches!(
        error.code,
        ErrorCode::ProviderPending | ErrorCode::RetryLater | ErrorCode::OutcomeUnknown
    )
}

pub(super) fn temporary(error: &BackupError) -> bool {
    matches!(error, BackupError::Cloud(BackupTransportError::Protocol(error)) if temporary_protocol(error))
}

pub(super) async fn control<T, F, Fut>(
    operation: &'static str,
    pause: &AtomicBool,
    mut request: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last_error = None;
    for attempt in 0..6 {
        transfer::check_pause(pause)?;
        if Instant::now() >= deadline {
            break;
        }
        let result = tokio::time::timeout_at(deadline, request()).await;
        transfer::check_pause(pause)?;
        match result {
            Ok(Err(error)) if temporary(&error) => last_error = Some(error),
            Ok(result) => return result,
            Err(_) => break,
        }
        if attempt < 5 {
            let ceiling = 100u64 << attempt;
            let delay = Duration::from_millis(1 + rand::random::<u64>() % ceiling);
            tokio::time::sleep_until(deadline.min(Instant::now() + delay)).await;
            transfer::check_pause(pause)?;
        }
    }
    tracing::warn!(
        operation,
        error_class = "temporary_control",
        "V2 control retry budget exhausted"
    );
    Err(last_error.unwrap_or(BackupError::Waiting))
}
