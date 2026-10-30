//! The stop handle shared by the periodic background tasks.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// The join handle and the stop flag for one periodic task.
pub(crate) struct BackgroundTask {
    join: Mutex<Option<tokio::task::JoinHandle<()>>>,
    stop: Arc<AtomicBool>,
}

impl BackgroundTask {
    /// A handle that is already stopped.
    pub(crate) fn disabled() -> Self {
        Self {
            join: Mutex::new(None),
            stop: Arc::new(AtomicBool::new(true)),
        }
    }

    /// A handle that owns a running task.
    pub(crate) fn started(handle: tokio::task::JoinHandle<()>, stop: Arc<AtomicBool>) -> Self {
        Self {
            join: Mutex::new(Some(handle)),
            stop,
        }
    }

    /// Store the stop flag, then abort the task.
    pub(crate) fn stop(&self, lock_message: &'static str) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.join.lock().expect(lock_message).take() {
            handle.abort();
        }
    }
}
