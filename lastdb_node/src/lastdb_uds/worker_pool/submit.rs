//! Admission counts queued, running, and deferred requests exactly once.
use super::*;

impl UdsWorkerPool {
    /// Transfer completion ownership when a handler awaits outside the pool.
    /// The token must live until the response finishes or the connection drops.
    pub fn try_submit_owned_connection<F>(
        &self,
        stream: UnixStream,
        work: F,
    ) -> Result<(), SubmitError>
    where
        F: FnOnce(UnixStream, ConnectionWork) + Send + 'static,
    {
        let in_flight_guard = match self.admit() {
            Ok(guard) => guard,
            Err(error) => return self.reject(stream, error),
        };
        let job = ConnJob {
            stream,
            enqueued_at: Instant::now(),
            work: Box::new(work),
            in_flight_guard,
        };
        match self.inner.dispatch.submit(job) {
            Ok(()) => {
                self.inner.submitted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(Rejected::Full(job)) => self.reject(job.stream, SubmitError::QueueFull),
            Err(Rejected::Closed(job)) => self.reject(job.stream, SubmitError::ShutDown),
        }
    }

    fn admit(&self) -> Result<InFlightGuard, SubmitError> {
        let mut state = self
            .inner
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return Err(SubmitError::ShutDown);
        }
        // A deferred handler releases its worker, not its admission. Otherwise
        // an empty dispatcher could admit unlimited futures and file handles.
        if state.in_flight >= self.inner.workers.saturating_add(self.inner.queue_capacity) {
            return Err(SubmitError::QueueFull);
        }
        state.in_flight += 1;
        Ok(InFlightGuard(Arc::clone(&self.inner)))
    }

    fn reject(&self, mut stream: UnixStream, error: SubmitError) -> Result<(), SubmitError> {
        if matches!(error, SubmitError::QueueFull) {
            self.inner
                .queue_full_rejects
                .fetch_add(1, Ordering::Relaxed);
        }
        if let Err(error) = uds_http::write_busy_and_close(&mut stream) {
            tracing::debug!(error = %error, "busy response write failed");
        }
        Err(error)
    }
}
