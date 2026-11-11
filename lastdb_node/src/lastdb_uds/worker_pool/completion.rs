//! One original admission survives an asynchronous handler and its response.
use super::*;

/// Admission and queue timing kept for one accepted connection after its handler returns.
pub struct ConnectionWork {
    guard: InFlightGuard,
    enqueued_at: Instant,
    queue_wait: Duration,
}

impl ConnectionWork {
    pub(super) fn new(guard: InFlightGuard, enqueued_at: Instant, queue_wait: Duration) -> Self {
        Self {
            guard,
            enqueued_at,
            queue_wait,
        }
    }

    #[must_use]
    /// How long this connection waited in the pool queue before a worker took it.
    pub fn queue_wait(&self) -> Duration {
        self.queue_wait
    }

    /// Continue an accepted request on the same bounded worker pool. The
    /// callback retains its original admission and initial arrival time.
    pub fn resume<F>(self, stream: UnixStream, work: F)
    where
        F: FnOnce(UnixStream, Self) + Send + 'static,
    {
        let dispatch = Arc::clone(&self.guard.0.dispatch);
        let job = ConnJob {
            stream,
            enqueued_at: self.enqueued_at,
            work: Box::new(work),
            in_flight_guard: self.guard,
        };
        // The guard retains PoolInner, so its Drop cannot close dispatch while
        // an accepted response remains. Admission closure still permits this.
        if dispatch.resume(job).is_err() {
            tracing::error!("accepted socket response dispatcher closed");
        }
    }
}
