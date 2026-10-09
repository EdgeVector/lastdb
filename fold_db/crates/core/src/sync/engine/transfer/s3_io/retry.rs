//! S3 retry helper.

use super::super::super::*;
use crate::sync::error::{SyncError, SyncResult};

/// Deterministic errors that must not burn the exponential-backoff schedule.
/// Auth needs a higher-level token refresh; oversize objects will fail again
/// with the same limit on every attempt. Quota/ban are permanent until the
/// operator frees space / upgrades / unbans — hot-retrying only storm Sentry.
///
/// Transient S3 failures ARE retried: transport send errors, HTTP 500,
/// HTTP 503, and AWS `SlowDown`. Those must not be treated as terminal.
fn is_non_retryable_s3_error(err: &SyncError) -> bool {
    match err {
        SyncError::Auth(_) | SyncError::Banned(_) | SyncError::QuotaExceeded(_) => true,
        e if super::is_oversize_s3_error(e) => true,
        _ => false,
    }
}

/// Base exponential delay plus a small deterministic jitter so concurrent
/// SlowDown clients do not retry in lockstep. Jitter stays well under the
/// base so existing retry tests remain bounded.
fn retry_s3_delay_ms(attempt: u32) -> u64 {
    let base_ms = 500u64.saturating_mul(2u64.saturating_pow(attempt));
    let jitter_span = (base_ms / 5).max(1);
    let jitter_ms = jitter_span.saturating_mul(u64::from(attempt % 4)) / 4;
    base_ms.saturating_add(jitter_ms)
}

impl SyncEngine {
    /// Retry an S3 operation with exponential backoff + jitter.
    /// Auth and deterministic oversize errors are NOT retried.
    /// After the attempt cap, the last error is returned unchanged — callers
    /// must not treat a failed budget as success (no silent byte drop).
    pub(crate) async fn retry_s3<F, Fut, T>(&self, label: &str, mut op: F) -> SyncResult<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = SyncResult<T>>,
    {
        let max_retries = self.config.max_retries;
        for attempt in 0..max_retries {
            match op().await {
                Ok(v) => return Ok(v),
                Err(e) if is_non_retryable_s3_error(&e) => return Err(e),
                Err(e) => {
                    let delay_ms = retry_s3_delay_ms(attempt);
                    let redacted_error = redact_sync_error_text(&e.to_string());
                    tracing::warn!(
                        "{}: attempt {}/{} failed ({}), retrying in {}ms",
                        label,
                        attempt + 1,
                        max_retries + 1,
                        redacted_error,
                        delay_ms
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }
            }
        }
        // Final attempt — no retry, just propagate
        op().await
    }
}
