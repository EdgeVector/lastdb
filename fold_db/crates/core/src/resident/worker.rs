//! Background persist worker — the T1 drain loop.
//!
//! Periodically writes the resident graph's dirty [`super::types::PersistPlan`]
//! through a [`PersistSink`], turning dirty entries clean so budget
//! enforcement can evict them. Mirrors the lifecycle discipline of
//! `fold_db_core::mutation_flush::BackgroundFlushTask`: stopped explicitly on
//! shutdown before the final durability barrier, aborted on Drop so tests do
//! not leak tasks.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Handle for the optional periodic resident-persist task.
pub struct BackgroundPersistTask {
    join: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Set when we intentionally stop; the loop checks this so abort races do
    /// not log spurious persist errors mid-shutdown.
    stop: Arc<AtomicBool>,
}

impl BackgroundPersistTask {
    /// No-op handle when the persist worker is disabled.
    pub fn disabled() -> Self {
        Self {
            join: Mutex::new(None),
            stop: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Signal the loop to exit and abort the task (best-effort).
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self
            .join
            .lock()
            .expect("background persist join lock")
            .take()
        {
            handle.abort();
        }
    }

    pub fn is_running(&self) -> bool {
        self.join
            .lock()
            .expect("background persist join lock")
            .as_ref()
            .is_some_and(|h| !h.is_finished())
    }
}

impl Drop for BackgroundPersistTask {
    fn drop(&mut self) {
        self.stop();
    }
}
