//! Periodic flushes for internal background work.
//!
//! Every successful local mutation request waits for its native disk sequence.
//! This periodic barrier does not select the request's acknowledgement policy.
//! Explicit flush and graceful shutdown also wait for accepted disk work.
//!
//! `LASTDB_BACKGROUND_FLUSH_MS` selects the periodic interval (default 500 ms).
//! A value of `0` disables the periodic task without weakening mutation receipts.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::db_operations::DbOperations;

/// Env: background flush interval in milliseconds (`0` = off).
pub const BACKGROUND_FLUSH_MS_ENV: &str = "LASTDB_BACKGROUND_FLUSH_MS";

/// Default period for the internal background flush barrier.
pub const DEFAULT_BACKGROUND_FLUSH_MS: u64 = 500;

/// Resolved periodic flush policy for this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MutationFlushPolicy {
    /// Background flusher period. `None` means no periodic flusher.
    pub background_interval: Option<Duration>,
}

impl MutationFlushPolicy {
    /// Parse from environment (and defaults) via the free `parse_*` helpers.
    pub fn from_env() -> Self {
        Self {
            background_interval: parse_background_flush_interval(
                std::env::var(BACKGROUND_FLUSH_MS_ENV).ok(),
            ),
        }
    }
}

/// `LASTDB_BACKGROUND_FLUSH_MS` → interval; unset → default 500 ms; `0` → off.
#[must_use]
pub fn parse_background_flush_interval(raw: Option<String>) -> Option<Duration> {
    let Some(raw) = raw else {
        return Some(Duration::from_millis(DEFAULT_BACKGROUND_FLUSH_MS));
    };
    let s = raw.trim();
    if s.is_empty() {
        return Some(Duration::from_millis(DEFAULT_BACKGROUND_FLUSH_MS));
    }
    match s.parse::<u64>() {
        Ok(0) => None,
        Ok(ms) => Some(Duration::from_millis(ms)),
        Err(_) => {
            tracing::warn!(
                raw = %s,
                env = BACKGROUND_FLUSH_MS_ENV,
                default_ms = DEFAULT_BACKGROUND_FLUSH_MS,
                "invalid LASTDB_BACKGROUND_FLUSH_MS; using default"
            );
            Some(Duration::from_millis(DEFAULT_BACKGROUND_FLUSH_MS))
        }
    }
}

/// Handle for the optional periodic flusher task.
///
/// Aborted and awaited by [`FoldDB::shutdown`](super::FoldDB::shutdown) before
/// the final durability flush. Drop aborts the task when an owner cannot await.
pub struct BackgroundFlushTask {
    join: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Set when we intentionally stop; the loop checks this so abort races
    /// do not log spurious flush errors mid-shutdown.
    stop: Arc<AtomicBool>,
}

impl BackgroundFlushTask {
    /// No-op handle when background flushing is disabled.
    pub fn disabled() -> Self {
        Self {
            join: Mutex::new(None),
            stop: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Spawn a periodic `db_ops.flush()` on the current Tokio runtime.
    pub fn spawn(db_ops: Arc<DbOperations>, interval: Duration) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        // lint:spawn-bare-ok process-lifetime flusher — not request-scoped.
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Skip the immediate first tick so boot is not followed by a
            // pointless empty flush before any mutation lands.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
                // `DbOperations::flush` applies `durable_through` per token.
                if let Err(e) = db_ops.flush().await {
                    if stop_flag.load(Ordering::Relaxed) {
                        break;
                    }
                    tracing::warn!(
                        target: "fold_node::database",
                        error = %e,
                        "background mutation flush failed"
                    );
                }
            }
        });
        Self {
            join: Mutex::new(Some(handle)),
            stop,
        }
    }

    /// Signal the loop to exit, abort it, and wait until it has stopped.
    pub async fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
        let handle = self.join.lock().expect("background flush join lock").take();
        if let Some(handle) = handle {
            handle.abort();
            if let Err(error) = handle.await {
                if !error.is_cancelled() {
                    tracing::warn!(
                        target: "fold_node::database",
                        error = %error,
                        "background flush task stopped with an error"
                    );
                }
            }
        }
    }

    /// Abort the loop from synchronous `Drop`. Normal shutdown uses [`Self::stop`]
    /// and awaits the task before it reaches this fallback.
    pub(crate) fn abort(&self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.join.lock().expect("background flush join lock").take() {
            handle.abort();
        }
    }
}

impl Drop for BackgroundFlushTask {
    fn drop(&mut self) {
        self.abort();
    }
}
