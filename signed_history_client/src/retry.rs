//! Transient relay failure classification, jittered backoff, and pure-`std` async sleep.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use serde_json::Value;

pub(crate) const RETRIES: u32 = 3;
const RETRY_BASE: Duration = Duration::from_millis(250);
const FAST_TRANSPORT_LIMIT: Duration = Duration::from_secs(5);

pub(crate) fn retryable_transport(error: &reqwest::Error, elapsed: Duration) -> bool {
    !error.is_timeout()
        && elapsed < FAST_TRANSPORT_LIMIT
        && (error.is_connect() || error.is_request() || error.is_body())
}

/// Whether the relay indicates the same request may succeed after a brief wait:
/// gateway errors (`502..=504`), `500 unavailable`, and transient `429 rate_limited`.
pub(crate) fn is_transient(status: u16, body: &str) -> bool {
    if matches!(status, 502..=504) {
        return true;
    }
    let reply: Value = serde_json::from_str(body).unwrap_or(Value::Null);
    let (code, field) = (
        reply["code"].as_str(),
        reply["field"].as_str().unwrap_or(""),
    );
    match (status, code) {
        (500, Some("unavailable")) => true,
        (429, Some("rate_limited")) => {
            !field.ends_with(".configuration")
                && (field.starts_with("provider.")
                    || field.starts_with("log.streams.")
                    || matches!(
                        field,
                        "mutation_log.read" | "quota.read" | "payment.read" | "payment.clock"
                    ))
        }
        _ => false,
    }
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
