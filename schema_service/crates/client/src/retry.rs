use super::*;

/// Internal error wrapper used by retryable schema-service calls.
///
/// The retry layer only retries transient failures (connect errors, timeouts,
/// 5xx responses). Permanent failures (4xx, 409 CONFLICT, deserialization
/// errors) fail fast on the first attempt.
#[derive(Debug)]
pub(crate) enum RetryError {
    Transient(FoldDbError),
    Permanent(FoldDbError),
}

impl RetryError {
    /// Wrap `err` as `Transient` when the failure is retryable
    /// (connect/timeout errors, 5xx responses), otherwise `Permanent`.
    pub(crate) fn classify(retryable: bool, err: FoldDbError) -> Self {
        if retryable {
            Self::Transient(err)
        } else {
            Self::Permanent(err)
        }
    }
}

/// Classify a `reqwest::Error` from `.send()` as transient or permanent.
///
/// Note: `reqwest::Error` from `.send()` covers connect/timeout/body-stream
/// errors but NOT HTTP status — status classification is handled separately
/// by `status_is_retryable`.
pub(crate) fn reqwest_error_is_retryable(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect() || err.is_request()
}

/// Classify an HTTP status code as retryable (5xx) or permanent (4xx).
pub(crate) fn status_is_retryable(status: StatusCode) -> bool {
    status.is_server_error()
}

/// Retry an async schema-service operation up to 3 times with exponential
/// backoff on transient failures.
///
/// The operation must distinguish transient from permanent failures by
/// returning `RetryError::Transient` or `RetryError::Permanent`. Permanent
/// errors (4xx, 409 CONFLICT, deserialization) fail fast without retry.
///
/// `MAX_ATTEMPTS` is one initial attempt plus 3 retries — "retry up to 3
/// times" — so the full backoff schedule (`backoff_delay` for attempt 0,
/// 1, 2) of 250ms, 1s, 4s is exercised before the call gives up.
pub(crate) async fn with_retries<F, Fut, T>(mut op: F) -> FoldDbResult<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, RetryError>>,
{
    const MAX_ATTEMPTS: u32 = 4;
    let mut last_err: Option<FoldDbError> = None;
    for attempt in 0..MAX_ATTEMPTS {
        match op().await {
            Ok(v) => return Ok(v),
            Err(RetryError::Permanent(e)) => return Err(e),
            Err(RetryError::Transient(e)) => {
                // Intermediate attempts are expected during fold_db_node
                // boot (folddb_server races schema_service's listen) and
                // get retried successfully — keep them at DEBUG so the
                // WARN channel stays user-actionable. Only the terminal
                // give-up below emits WARN.
                tracing::debug!(
                    attempt = attempt + 1,
                    max_attempts = MAX_ATTEMPTS,
                    error = %e,
                    "schema service call failed; will retry"
                );
                last_err = Some(e);
                if attempt + 1 < MAX_ATTEMPTS {
                    backoff_delay(attempt).await;
                }
            }
        }
    }
    let terminal = last_err.expect("retry loop must have produced at least one error");
    tracing::warn!(
        attempts = MAX_ATTEMPTS,
        error = %terminal,
        "schema service call failed after all retries"
    );
    Err(terminal)
}

/// Exponential backoff delay: 250ms, 1s, 4s, then 4s cap.
pub(crate) async fn backoff_delay(attempt: u32) {
    let ms = match attempt {
        0 => 250,
        1 => 1000,
        _ => 4000,
    };
    tokio::time::sleep(tokio::time::Duration::from_millis(ms)).await;
}

/// Drain a response body to a string for error reporting, returning
/// `"<empty>"` if the body stream itself fails.
pub(crate) async fn response_body_text(response: reqwest::Response) -> String {
    response
        .text()
        .await
        .unwrap_or_else(|_| "<empty>".to_string())
}
