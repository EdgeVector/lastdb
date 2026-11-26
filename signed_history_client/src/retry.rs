//! Transient relay failure classification, jittered backoff, and pure-`std` async sleep.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use relay_protocol::ErrorAnswer;

pub(crate) const RETRIES: u32 = 3;
const RETRY_BASE: Duration = Duration::from_millis(250);
const FAST_TRANSPORT_LIMIT: Duration = Duration::from_secs(5);

pub(crate) fn retryable_transport(error: &reqwest::Error, elapsed: Duration) -> bool {
    !error.is_timeout()
        && elapsed < FAST_TRANSPORT_LIMIT
        && (error.is_connect() || error.is_request() || error.is_body())
}

/// Whether the relay indicates the same request may succeed after a brief wait:
/// gateway errors (`502..=504`), and an error answer that [`ErrorAnswer::is_transient`] calls
/// transient with the status of its code (`500 unavailable`, transient `429 rate_limited`).
pub(crate) fn is_transient(status: u16, body: &str) -> bool {
    matches!(status, 502..=504)
        || serde_json::from_str::<ErrorAnswer>(body)
            .is_ok_and(|answer| answer.code.status() == status && answer.is_transient())
}

/// Jittered wait before retry `retried` (`0..RETRIES`): half to full of `250ms * 2^retried`.
pub(crate) fn retry_wait(retried: u32) -> Duration {
    use rand::Rng as _;
    let full = RETRY_BASE * 2u32.pow(retried);
    let low = full / 2;
    rand::thread_rng().gen_range(low..=full)
}

struct SleepState {
    done: bool,
    waker: Option<Waker>,
}

struct SleepTimer {
    state: Arc<Mutex<SleepState>>,
}

impl Future for SleepTimer {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut guard = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if guard.done {
            Poll::Ready(())
        } else {
            guard.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

pub(crate) fn async_sleep(duration: Duration) -> impl Future<Output = ()> {
    let state = Arc::new(Mutex::new(SleepState {
        done: false,
        waker: None,
    }));
    let shared = Arc::clone(&state);
    std::thread::spawn(move || {
        std::thread::sleep(duration);
        let waker = {
            let mut guard = shared.lock().unwrap_or_else(PoisonError::into_inner);
            guard.done = true;
            guard.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    });
    SleepTimer { state }
}
