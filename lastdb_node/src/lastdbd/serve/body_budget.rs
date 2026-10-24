//! Deferred requests share the existing pool's maximum parse-byte allowance.
use lastdb_uds::uds_http::{self, UdsRequest};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// Include body, bounded headers, request line and container allocation slack.
const MAX_PARSE_BYTES: usize =
    uds_http::MAX_BODY_LEN + 4 * uds_http::MAX_HEADERS_TOTAL + 4 * uds_http::MAX_LINE_LEN;

#[derive(Clone)]
pub(super) struct BodyBudget(Arc<Semaphore>);

pub(super) struct BodyLease(OwnedSemaphorePermit);

impl BodyBudget {
    pub(super) fn new(workers: usize) -> Self {
        let bytes = workers
            .saturating_mul(MAX_PARSE_BYTES)
            .min(Semaphore::MAX_PERMITS);
        Self(Arc::new(Semaphore::new(bytes)))
    }

    pub(super) fn try_reserve(&self) -> Option<BodyLease> {
        let bytes = u32::try_from(MAX_PARSE_BYTES).expect("bounded HTTP parse size fits u32");
        Arc::clone(&self.0)
            .try_acquire_many_owned(bytes)
            .ok()
            .map(BodyLease)
    }

    pub(super) async fn reserve(&self) -> std::io::Result<BodyLease> {
        let bytes = u32::try_from(MAX_PARSE_BYTES).expect("bounded HTTP parse size fits u32");
        Arc::clone(&self.0)
            .acquire_many_owned(bytes)
            .await
            .map(BodyLease)
            .map_err(std::io::Error::other)
    }
}

impl BodyLease {
    /// Retain the actual frame bytes while its future awaits. Large frames
    /// consume the same allowance as the original bounded synchronous parsers.
    pub(super) fn retain_request(&mut self, request: &UdsRequest) -> bool {
        let bytes = request.headers.iter().fold(
            std::mem::size_of::<UdsRequest>()
                + request.body.capacity()
                + request.method.capacity()
                + request.target.capacity()
                + request.headers.capacity() * std::mem::size_of::<(String, String)>(),
            |total, (name, value)| {
                total
                    .saturating_add(name.capacity())
                    .saturating_add(value.capacity())
            },
        );
        if bytes > self.0.num_permits() {
            return false;
        }
        let unused = self.0.num_permits() - bytes;
        drop(self.0.split(unused));
        true
    }
}
