//! Mutation durability: memory-first writes, flush later.
//!
//! ## Why
//!
//! LastStore `put` already buffers into memory (group-commit when a shard hits
//! `max_dirty_*`). The product mutation path used to call a full
//! `db_ops.flush()` — F_FULLFSYNC across open shards on macOS — on every
//! `finalize_batch`. That matched sled-era *intent* (durability barrier at
//! commit) but not sled-era *cost*: sled's flush was cheap; LastStore's is
//! multi-millisecond to multi-second under a warm HashGroup set.
//!
//! Sled-like product latency needs the same shape sled actually used for
//! day-to-day speed: **acknowledge after memory write, durable on a short
//! background interval (and on shutdown)**.
//!
//! ## Contract
//!
//! | Path | Behavior |
//! |------|----------|
//! | Mutation `finalize_batch` | By default **skips** sync flush (read-your-writes still holds). |
//! | Background flusher | Periodic `db_ops.flush()` (default **500 ms**). |
//! | `FoldDB::shutdown` / explicit `flush` | Always full durability barrier. |
//! | LastStore group-commit | Unchanged: a single shard still fsyncs when dirty caps hit. |
//!
//! ## Env
//!
//! | Variable | Meaning |
//! |----------|---------|
//! | `LASTDB_MUTATION_SYNC_FLUSH` | `1` / `true` / `yes` / `on` → restore per-mutation flush in `finalize_batch`. |
//! | `LASTDB_BACKGROUND_FLUSH_MS` | Interval for the background flusher. Default `500`. `0` disables it. |
//!
//! Crash window with defaults: up to ~one background interval of acknowledged
//! but not-yet-F_FULLFSYNC'd mutations (plus anything still only in the
//! LastStore open buffer under the dirty caps). Graceful `lastdbd` exit
//! always flushes.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::db_operations::DbOperations;

/// Env: force the old per-mutation `finalize_batch` flush.
pub const MUTATION_SYNC_FLUSH_ENV: &str = "LASTDB_MUTATION_SYNC_FLUSH";

/// Env: background flush interval in milliseconds (`0` = off).
pub const BACKGROUND_FLUSH_MS_ENV: &str = "LASTDB_BACKGROUND_FLUSH_MS";

/// Default background flush period — sled-class group-commit cadence.
pub const DEFAULT_BACKGROUND_FLUSH_MS: u64 = 500;

/// Resolved mutation durability policy for this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MutationFlushPolicy {
    /// When true, `finalize_batch` waits on `db_ops.flush()`.
    pub sync_on_finalize: bool,
    /// Background flusher period. `None` means no periodic flusher.
    pub background_interval: Option<Duration>,
}

impl MutationFlushPolicy {
    /// Parse from environment (and defaults). Pure enough for unit tests via
    /// the free `parse_*` helpers.
    pub fn from_env() -> Self {
        Self {
            sync_on_finalize: parse_mutation_sync_flush(
                std::env::var(MUTATION_SYNC_FLUSH_ENV).ok(),
            ),
            background_interval: parse_background_flush_interval(
                std::env::var(BACKGROUND_FLUSH_MS_ENV).ok(),
            ),
        }
    }
}

/// `LASTDB_MUTATION_SYNC_FLUSH` truthy → sync finalize flush.
#[must_use]
pub fn parse_mutation_sync_flush(raw: Option<String>) -> bool {
    raw.is_some_and(|s| env_flag::truthy(&s))
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

/// Whether `finalize_batch` should block on a full storage flush.
#[must_use]
pub fn mutation_sync_flush_enabled() -> bool {
    parse_mutation_sync_flush(std::env::var(MUTATION_SYNC_FLUSH_ENV).ok())
}

/// Handle for the optional periodic flusher task.
///
/// Aborted and awaited by [`FoldDB::shutdown`](super::FoldDB::shutdown) before
/// the final durability flush. Drop uses the synchronous abort fallback so
/// tests do not leak tasks.
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

    pub fn is_running(&self) -> bool {
        self.join
            .lock()
            .expect("background flush join lock")
            .as_ref()
            .is_some_and(|h| !h.is_finished())
    }
}

impl Drop for BackgroundFlushTask {
    fn drop(&mut self) {
        self.abort();
    }
}
