//! One original admission survives an asynchronous handler and its response.
use super::*;

pub struct ConnectionWork {
    guard: InFlightGuard,
    queue_wait: Duration,
}

impl ConnectionWork {
    pub(super) fn new(guard: InFlightGuard, queue_wait: Duration) -> Self {
        Self { guard, queue_wait }
    }

    #[must_use]
    pub fn queue_wait(&self) -> Duration {
        self.queue_wait
    }

    /// Finish an accepted response on the same bounded worker pool. This is
    /// neither a new admission nor a detached blocking thread.
    pub fn resume<F>(self, stream: UnixStream, work: F)
    where
        F: FnOnce(UnixStream) + Send + 'static,
    {
        let dispatch = Arc::clone(&self.guard.0.dispatch);
        let job = ConnJob {
            stream,
            enqueued_at: Instant::now(),
            work: Box::new(move |stream, _completion| work(stream)),
            in_flight_guard: self.guard,
        };
        // The guard retains PoolInner, so its Drop cannot close dispatch while
        // an accepted response remains. Admission closure still permits this.
        if dispatch.resume(job).is_err() {
            tracing::error!("accepted socket response dispatcher closed");
        }
    }
}
