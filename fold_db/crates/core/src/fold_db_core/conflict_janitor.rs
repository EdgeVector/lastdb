//! The retention janitor for kept conflict records.
//!
//! The database keeps every conflicting write. A kept loser stays protected by
//! its conflict record. The record ends with its retention window (30 days, set
//! by `LASTDB_CONFLICT_RETENTION_SECS`) or with a purge of its key. This task
//! ends the records that reach the end of their window.
//!
//! The queue is time-bucketed (`db_operations::atom_store::time_queue`). One
//! wake reads the directory of the buckets that have started (again at the start
//! of each turn), and walks them oldest first in bounded pages. The read of a
//! page holds no write gate. Each page that has due records runs one release batch
//! (`AtomStore::release_step`): one native write scope, one batch, and the gate
//! is free again before the next page. The janitor rests after each batch for
//! `took * (100 - duty) / duty` (a duty share of 10 percent by default), so the
//! flush of a release batch leaves the disk to the writers most of the time. The
//! janitor deletes no atom. The batch names the history rows to the planner,
//! which lowers the references and deletes a body only when its count reaches
//! zero.
//!
//! The janitor keeps a cursor for each bucket and for each slot drain, in
//! memory. A cursor moves over deleted keys and never goes back to the head of
//! a hash, so a long history of released records costs nothing to pass. A
//! bucket that is read to its end leaves the directory (`retire_bucket`).
//!
//! The wake does two kinds of work, and alternates them: up to
//! [`DRAIN_BATCHES_PER_TURN`] batches of slot drains, then up to
//! [`ORDINARY_BATCHES_PER_TURN`] batches of due records, and again. A long
//! drain cannot hold back the due records, and a backlog cannot hold back a
//! drain. Once every [`FULL_PASS_EVERY`] wakes the cursors of the live buckets
//! start again from the start of their buckets, in case the node clock stepped
//! back. A bucket that the directory dropped is not read again.
//!
//! A wake that ends with an error (the storage refused a write, a read failed)
//! makes the janitor wait longer before the next wake: the wait doubles with
//! each wake in a row that fails, from the interval (at least one second) up to
//! ten minutes. The status line shows the error, the count of failed wakes, and
//! the time of the next try. A good wake sets the count to zero.
//!
//! | Variable | Meaning |
//! |----------|---------|
//! | `LASTDB_CONFLICT_JANITOR_MS` | Interval between wakes. Zero turns the janitor off. Default 60000. |
//! | `LASTDB_CONFLICT_JANITOR_DUTY_PCT` | The share of wall time, in percent, that release batches may take (1 to 100). Default 10. |
//! | `LASTDB_CONFLICT_RETENTION_SECS` | The retention window. Default 30 days. |

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::db_operations::atom_store::{Lane, RetentionCursor};
use crate::db_operations::DbOperations;

/// Env: interval between janitor wakes in milliseconds (`0` = off).
pub const CONFLICT_JANITOR_MS_ENV: &str = "LASTDB_CONFLICT_JANITOR_MS";
/// Default interval between wakes: one minute.
pub const DEFAULT_CONFLICT_JANITOR_MS: u64 = 60_000;
/// Batches of one wake, at most. A larger backlog continues at the next wake.
const PAGES_PER_WAKE: usize = 256;
/// Batches of slot drains in one turn, at most. Then due records get a turn.
const DRAIN_BATCHES_PER_TURN: usize = 4;
/// Batches of due records in one turn, at most. Then the drains get a turn.
const ORDINARY_BATCHES_PER_TURN: usize = 4;
/// Env: the share of wall time, in percent, that the janitor may spend inside
/// release batches. A batch deletes many keys, and its flush shares the disk
/// with every writer. The janitor rests after each batch for
/// `took * (100 - duty) / duty`, like the TTL sweep (`ttl_sweep/pace.rs`).
pub const CONFLICT_JANITOR_DUTY_PCT_ENV: &str = "LASTDB_CONFLICT_JANITOR_DUTY_PCT";
/// Default duty share: 10 percent. In a run with 700 records due at once, 25
/// percent raised the median write latency by 9 to 46 percent while the
/// janitor released them, and 10 percent left it unchanged within the noise.
pub const DEFAULT_CONFLICT_JANITOR_DUTY_PCT: u64 = 10;
/// The shortest rest between two pages.
const MIN_PAUSE: Duration = Duration::from_millis(25);
/// The longest rest between two pages.
const MAX_PAUSE: Duration = Duration::from_secs(30);
/// The cursors of the live buckets start again, once in this many wakes.
const FULL_PASS_EVERY: u64 = 60;
/// The longest wait after a wake that failed.
const BACKOFF_MAX: Duration = Duration::from_secs(600);
/// The shortest base of the wait after a wake that failed.
const BACKOFF_BASE_MIN: Duration = Duration::from_secs(1);

/// Resolved janitor policy for this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConflictJanitorPolicy {
    /// The interval between wakes. `None` means no janitor.
    pub interval: Option<Duration>,
    /// The share of wall time, in percent, that release batches may take.
    pub duty_pct: u64,
}

impl ConflictJanitorPolicy {
    /// Parse from the environment.
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            interval: parse_conflict_janitor_interval(
                std::env::var(CONFLICT_JANITOR_MS_ENV).ok().as_deref(),
            ),
            duty_pct: parse_conflict_janitor_duty(
                std::env::var(CONFLICT_JANITOR_DUTY_PCT_ENV).ok().as_deref(),
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

/// `LASTDB_CONFLICT_JANITOR_DUTY_PCT` to a percentage from 1 to 100. Unset or
/// invalid gives the default, and a value outside the range is clamped.
#[must_use]
pub fn parse_conflict_janitor_duty(raw: Option<&str>) -> u64 {
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return DEFAULT_CONFLICT_JANITOR_DUTY_PCT;
    };
    raw.parse::<u64>().map_or_else(
        |_| {
            tracing::warn!(
                raw,
                env = CONFLICT_JANITOR_DUTY_PCT_ENV,
                default = DEFAULT_CONFLICT_JANITOR_DUTY_PCT,
                "invalid conflict janitor duty share; using the default"
            );
            DEFAULT_CONFLICT_JANITOR_DUTY_PCT
        },
        |duty| duty.clamp(1, 100),
    )
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
    pub fn spawn(db_ops: Arc<DbOperations>, interval: Duration, duty_pct: u64) -> Self {
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
            let mut cursor = db_ops.atoms().retention_cursor();
            let mut wakes = 0u64;
            let mut failures = 0u64;
            let mut retry_at: Option<std::time::Instant> = None;
            loop {
                ticker.tick().await;
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
                if retry_at.is_some_and(|at| std::time::Instant::now() < at) {
                    continue;
                }
                if wakes.is_multiple_of(FULL_PASS_EVERY) {
                    cursor.restart();
                }
                wakes += 1;
                let wake = run_wake(&db_ops, &stop_flag, &mut cursor, duty_pct).await;
                failures = if wake.error.is_some() {
                    failures + 1
                } else {
                    0
                };
                let retry_in = (failures > 0).then(|| backoff_after(failures, interval));
                retry_at = retry_in.map(|wait| std::time::Instant::now() + wait);
                db_ops.atoms().retention_state().note_run(
                    wake.released,
                    wake.error,
                    failures,
                    retry_in,
                );
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

/// The rest after a release batch that took `took`.
fn pause_after(took: Duration, duty_pct: u64) -> Duration {
    let took_ms = u64::try_from(took.as_millis()).unwrap_or(u64::MAX);
    Duration::from_millis(took_ms.saturating_mul(100 - duty_pct) / duty_pct)
        .clamp(MIN_PAUSE, MAX_PAUSE)
}

/// The wait before the next wake after `failures` wakes in a row that failed.
fn backoff_after(failures: u64, interval: Duration) -> Duration {
    let base = interval.max(BACKOFF_BASE_MIN);
    let shift = u32::try_from(failures.min(10)).unwrap_or(10);
    base.saturating_mul(1 << shift).min(BACKOFF_MAX)
}

/// What one wake did.
#[derive(Default)]
struct Wake {
    released: u64,
    skipped: u64,
    /// The first error of the wake.
    error: Option<String>,
}

impl Wake {
    fn fail(&mut self, failure: &crate::schema::SchemaError) {
        if self.error.is_none() {
            self.error = Some(failure.to_string());
        }
    }
}

/// One wake: alternate slot drains and due records, batch by batch, and read the
/// directory of the queue at the start of each turn. A lane that fails ends for
/// this wake. The other lane goes on.
async fn run_wake(
    db_ops: &DbOperations,
    stop: &AtomicBool,
    cursor: &mut RetentionCursor,
    duty_pct: u64,
) -> Wake {
    let atoms = db_ops.atoms();
    let mut wake = Wake::default();
    let mut ordinary_ok = true;
    let mut drain_ok = true;
    let mut batches = 0usize;
    'wake: while batches < PAGES_PER_WAKE && (ordinary_ok || drain_ok) {
        let mut progressed = false;
        // Read the directory at the start of each turn, not only of the wake: a
        // bucket that starts while a long drain runs must get its turn too.
        if ordinary_ok {
            let now = atoms.retention_state().clock_nanos();
            if let Err(failure) = atoms.refresh_queue(cursor, now).await {
                wake.fail(&failure);
                ordinary_ok = false;
            }
        }
        for lane in [Lane::Drain, Lane::Ordinary] {
            let (turn, lane_ok) = match lane {
                Lane::Drain => (DRAIN_BATCHES_PER_TURN, &mut drain_ok),
                _ => (ORDINARY_BATCHES_PER_TURN, &mut ordinary_ok),
            };
            for _ in 0..turn {
                if stop.load(Ordering::Relaxed) {
                    break 'wake;
                }
                if !*lane_ok {
                    break;
                }
                let started = std::time::Instant::now();
                let step = if lane == Lane::Drain {
                    atoms.drain_step(cursor).await
                } else {
                    let now = atoms.retention_state().clock_nanos();
                    atoms.release_step(cursor, now).await
                };
                let outcome = match step {
                    Ok(Some(outcome)) => outcome,
                    Ok(None) => break,
                    Err(failure) => {
                        wake.fail(&failure);
                        *lane_ok = false;
                        break;
                    }
                };
                progressed = true;
                batches += 1;
                atoms.retention_state().note_released(outcome.released);
                atoms
                    .retention_state()
                    .set_pending_drains(cursor.pending_drains() as u64);
                wake.released += outcome.released;
                wake.skipped += outcome.skipped;
                tracing::debug!(
                    target: "fold_node::database",
                    lane = ?outcome.lane,
                    released = outcome.released,
                    skipped = outcome.skipped,
                    finished = outcome.finished,
                    retired = outcome.retired,
                    took_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    "conflict janitor batch"
                );
                tokio::time::sleep(pause_after(started.elapsed(), duty_pct)).await;
            }
        }
        if !progressed {
            break;
        }
    }
    atoms
        .retention_state()
        .set_pending_drains(cursor.pending_drains() as u64);
    if let Some(failure) = &wake.error {
        tracing::warn!(
            target: "fold_node::database",
            error = %failure,
            "conflict janitor stopped before its work was done"
        );
    } else if wake.released > 0 || wake.skipped > 0 {
        tracing::info!(
            target: "fold_node::database",
            released = wake.released,
            skipped = wake.skipped,
            buckets = cursor.buckets(),
            pending_drains = cursor.pending_drains(),
            "conflict janitor released kept conflict records"
        );
    }
    wake
}
