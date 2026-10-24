//! Preserve initial socket admission time across an asynchronous handler handoff.
use std::time::Duration;

tokio::task_local! {
    static QUEUE_WAIT: Duration;
}

pub async fn with_socket_queue_wait<T>(
    wait: Duration,
    future: impl std::future::Future<Output = T>,
) -> T {
    QUEUE_WAIT.scope(wait, future).await
}

#[must_use]
pub fn current_socket_queue_wait() -> Option<Duration> {
    QUEUE_WAIT
        .try_with(|wait| *wait)
        .ok()
        .or_else(lastdb_uds::worker_pool::current_queue_wait)
}
