//! The retention janitor for kept conflict records.
//!
//! The database keeps every conflicting write. A kept loser stays protected by
//! its conflict record. The record ends with its retention window (30 days, set
//! by `LASTDB_CONFLICT_RETENTION_SECS`) or with a purge of its key. This task
//! ends the records that reach the end of their window.
//!
//! One wake reads the queue of expiring records oldest first, in bounded pages.
//! The read of a page holds no write gate. Each page that has due records runs
//! one release batch (`AtomStore::release_due_entries`): one native write scope,
//! one batch, and the gate is free again before the next page. The janitor
//! deletes no atom. The batch names the history rows to the planner, which
//! lowers the references and deletes a body only when its count reaches zero.
//!
//! The queue is sorted by expiry, so a page whose first entry is not due ends
//! the wake. A watermark remembers the last released key, so a page read skips
//! the keys already deleted. The watermark resets every hour, in case the node
//! clock stepped back.
//!
//! | Variable | Meaning |
//! |----------|---------|
//! | `LASTDB_CONFLICT_JANITOR_MS` | Interval between wakes. Zero turns the janitor off. Default 60000. |
//! | `LASTDB_CONFLICT_RETENTION_SECS` | The retention window. Default 30 days. |

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::db_operations::DbOperations;

/// Env: interval between janitor wakes in milliseconds (`0` = off).
pub const CONFLICT_JANITOR_MS_ENV: &str = "LASTDB_CONFLICT_JANITOR_MS";
/// Default interval between wakes: one minute.
pub const DEFAULT_CONFLICT_JANITOR_MS: u64 = 60_000;
/// Queue keys that one page reads, and so the most records of one release batch.
const PAGE: usize = 64;
/// Pages of one wake, at most. A larger backlog continues at the next wake.
const PAGES_PER_WAKE: usize = 256;
/// The pause between two pages, so that a writer finds the gate free.
const PAUSE_BETWEEN_PAGES: Duration = Duration::from_millis(20);
/// The queue depth that the status counts up to.
const DEPTH_CAP: usize = 1_000;
/// A wake that reads the queue from its start, once in this many wakes.
const FULL_PASS_EVERY: u64 = 60;

/// Resolved janitor policy for this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConflictJanitorPolicy {
    /// The interval between wakes. `None` means no janitor.
    pub interval: Option<Duration>,
}

impl ConflictJanitorPolicy {
    /// Parse from the environment.
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            interval: parse_conflict_janitor_interval(
                std::env::var(CONFLICT_JANITOR_MS_ENV).ok().as_deref(),
            ),
        }
    }
}

/// `LASTDB_CONFLICT_JANITOR_MS` to an interval. Unset gives the default, zero
/// turns the janitor off, and an invalid value gives the default.
#[must_use]
pub fn parse_conflict_janitor_interval(raw: Option<&str>) -> Option<Duration> {
    let default = Some(Duration::from_millis(DEFAULT_CONFLICT_JANITOR_MS));
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return default;
    };
    match raw.parse::<u64>() {
        Ok(0) => None,
        Ok(ms) => Some(Duration::from_millis(ms)),
        Err(_) => {
            tracing::warn!(
                raw,
                env = CONFLICT_JANITOR_MS_ENV,
                default_ms = DEFAULT_CONFLICT_JANITOR_MS,
                "invalid conflict janitor interval; using the default"
            );
            default
        }
    }
}

/// Handle for the periodic janitor task.
///
/// Aborted on [`super::fold_db::FoldDB::shutdown`] before the final
/// durability flush, and on Drop.
pub struct BackgroundConflictJanitorTask {
    join: Mutex<Option<tokio::task::JoinHandle<()>>>,
    stop: Arc<AtomicBool>,
}

impl BackgroundConflictJanitorTask {
    /// No-op handle when the janitor is off.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            join: Mutex::new(None),
            stop: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Spawn the periodic janitor on the current Tokio runtime.
    pub fn spawn(db_ops: Arc<DbOperations>, interval: Duration) -> Self {
        db_ops
            .atoms()
            .retention_state()
            .set_janitor_interval_ms(u64::try_from(interval.as_millis()).unwrap_or(u64::MAX));
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        // lint:spawn-bare-ok process-lifetime janitor loop, not request-scoped.
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;
            let mut watermark: Option<String> = None;
            let mut wakes = 0u64;
            loop {
                ticker.tick().await;
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
                if wakes.is_multiple_of(FULL_PASS_EVERY) {
                    watermark = None;
                }
                wakes += 1;
                run_wake(&db_ops, &stop_flag, &mut watermark).await;
            }
        });
        Self {
            join: Mutex::new(Some(handle)),
            stop,
        }
    }

    /// Signal the loop to exit and abort the task.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.join.lock().expect("conflict janitor join lock").take() {
            handle.abort();
        }
    }
}

impl Drop for BackgroundConflictJanitorTask {
    fn drop(&mut self) {
        self.stop();
    }
}

fn now_nanos() -> u64 {
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX);
    u64::try_from(now).unwrap_or(0)
}

/// One wake: release the due records page by page, then count the queue.
async fn run_wake(db_ops: &DbOperations, stop: &AtomicBool, watermark: &mut Option<String>) {
    let atoms = db_ops.atoms();
    let mut released = 0u64;
    let mut skipped = 0u64;
    let mut error: Option<String> = None;
    for _ in 0..PAGES_PER_WAKE {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let now = now_nanos();
        let page = match atoms.read_queue_page(watermark.as_deref(), PAGE).await {
            Ok(page) => page,
            Err(failure) => {
                error = Some(failure.to_string());
                break;
            }
        };
        let due = page.due(now);
        if due.is_empty() {
            break;
        }
        let last_due = due.last().cloned();
        let more_due = due.len() == page.keys.len() && page.full;
        match atoms.release_due_entries(due, now).await {
            Ok(outcome) => {
                released += outcome.released;
                skipped += outcome.skipped;
                *watermark = last_due;
            }
            Err(failure) => {
                error = Some(failure.to_string());
                break;
            }
        }
        if !more_due {
            break;
        }
        tokio::time::sleep(PAUSE_BETWEEN_PAGES).await;
    }
    if let Some(failure) = &error {
        tracing::warn!(
            target: "fold_node::database",
            error = %failure,
            "conflict janitor stopped before the queue was empty"
        );
    } else if released > 0 || skipped > 0 {
        tracing::info!(
            target: "fold_node::database",
            released,
            skipped,
            "conflict janitor released kept conflict records"
        );
    }
    let depth = atoms.queue_depth(DEPTH_CAP).await.ok();
    atoms.retention_state().note_run(released, depth, error);
}
