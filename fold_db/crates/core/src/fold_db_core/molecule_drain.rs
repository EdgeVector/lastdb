//! The paced task that drains dead molecules.
//!
//! A molecule that lost its last binding is a candidate (`atom_store::
//! molecule_drain`). This task reads the candidate list, and for each candidate
//! runs the steps of the drain, one batch at a time: a bounded page of tip
//! records through the atom counter planner, then bounded pages of the rows that
//! carry no count, then the final batch. Each batch is one native write scope, and
//! it decides from the counters it reads inside the gate. The page read holds no
//! gate. The pacing is the one of the retention janitor and the blob dwell pass
//! (`conflict_janitor_pace.rs`, `WriterWatch`): the task rests after each batch for
//! `blocked * (100 - duty) / duty`, where `blocked` is the time the batch kept writers
//! waiting (the hold of the write gate and the wait for the flush that seals the
//! queue), not the wall time of the step. While another task has opened a write scope
//! within the quiet period (`LASTDB_MAINTENANCE_QUIET_SECS`, 60 s by default) the rest is
//! the full one. A node with no other writer for that period has nobody to protect, and
//! the task rests the shortest time (25 ms). A rest ends early when the quiet period has
//! passed.
//!
//! The task wakes at its interval and at once when a batch lowers a `bindings`
//! counter to zero (a schema drop, a catalog removal). It reads the candidate
//! list from its start at each wake. A wake that ends with an error makes the task
//! wait longer before the next wake, as the conflict janitor does. A wake signal (a
//! new drop, or a molecule named by hand) ends that wait, because it brings new work.
//!
//! | Variable | Meaning |
//! |----------|---------|
//! | `LASTDB_MOLECULE_DRAIN_MS` | Interval between wakes. Zero turns the task off. Default 30000. |
//! | `LASTDB_MOLECULE_DRAIN_DUTY_PCT` | The share of wall time, in percent, that batches may take (1 to 100). Default 10. |
//! | `LASTDB_MOLECULE_DRAIN_TIPS` | Tip records one batch erases, at most (1 to 512). Default 256. A batch that hits the read limit halves the size, and the size doubles back after 5 wakes that did work and had no read limit. |
//! | `LASTDB_MOLECULE_DRAIN_BATCHES` | Batches one wake runs, at most. Default 1024. |

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::background_task::BackgroundTask;
use std::time::Duration;

use super::conflict_janitor_pace::{rest, rest_after_cost};
use crate::db_operations::atom_store::{
    AtomStore, DrainCursor, DrainLimits, DrainOutcomeKind as OutcomeKind, WriterWatch,
};
use crate::db_operations::DbOperations;

/// Env: interval between wakes in milliseconds (`0` = off).
pub const MOLECULE_DRAIN_MS_ENV: &str = "LASTDB_MOLECULE_DRAIN_MS";
/// Default interval between wakes: 30 seconds.
pub const DEFAULT_MOLECULE_DRAIN_MS: u64 = 30_000;
/// Env: the share of time, in percent, that batches may keep writers waiting.
pub const MOLECULE_DRAIN_DUTY_PCT_ENV: &str = "LASTDB_MOLECULE_DRAIN_DUTY_PCT";
/// Default duty share: 10 percent. A write that shares time with a drain batch waits
/// longer, so the default keeps the share of such writes small. The drain is not urgent:
/// the schema is already gone, and only the disk space waits.
pub const DEFAULT_MOLECULE_DRAIN_DUTY_PCT: u64 = 10;
/// Env: tip records one batch erases, at most.
pub const MOLECULE_DRAIN_TIPS_ENV: &str = "LASTDB_MOLECULE_DRAIN_TIPS";
/// Default tip records in a batch.
pub const DEFAULT_MOLECULE_DRAIN_TIPS: usize = 256;
/// Env: batches one wake runs, at most.
pub const MOLECULE_DRAIN_BATCHES_ENV: &str = "LASTDB_MOLECULE_DRAIN_BATCHES";
/// Default batches in a wake.
pub const DEFAULT_MOLECULE_DRAIN_BATCHES: usize = 1_024;

/// The longest wait after a wake that failed.
const BACKOFF_MAX: Duration = Duration::from_secs(600);
/// The shortest base of the wait after a wake that failed.
const BACKOFF_BASE_MIN: Duration = Duration::from_secs(1);

/// Resolved policy for this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MoleculeDrainPolicy {
    /// The interval between wakes. `None` means no drain task.
    pub interval: Option<Duration>,
    pub duty_pct: u64,
    pub tips: usize,
    pub batches: usize,
}

impl MoleculeDrainPolicy {
    /// Parse from the environment.
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            interval: parse_interval(std::env::var(MOLECULE_DRAIN_MS_ENV).ok().as_deref()),
            duty_pct: parse_number(
                std::env::var(MOLECULE_DRAIN_DUTY_PCT_ENV).ok().as_deref(),
                MOLECULE_DRAIN_DUTY_PCT_ENV,
                DEFAULT_MOLECULE_DRAIN_DUTY_PCT,
            )
            .clamp(1, 100),
            tips: usize::try_from(parse_number(
                std::env::var(MOLECULE_DRAIN_TIPS_ENV).ok().as_deref(),
                MOLECULE_DRAIN_TIPS_ENV,
                DEFAULT_MOLECULE_DRAIN_TIPS as u64,
            ))
            .unwrap_or(DEFAULT_MOLECULE_DRAIN_TIPS)
            .clamp(1, 512),
            batches: usize::try_from(parse_number(
                std::env::var(MOLECULE_DRAIN_BATCHES_ENV).ok().as_deref(),
                MOLECULE_DRAIN_BATCHES_ENV,
                DEFAULT_MOLECULE_DRAIN_BATCHES as u64,
            ))
            .unwrap_or(DEFAULT_MOLECULE_DRAIN_BATCHES)
            .max(1),
        }
    }
}

/// `LASTDB_MOLECULE_DRAIN_MS` to an interval. Unset gives the default, zero turns
/// the task off, and an invalid value gives the default.
fn parse_interval(raw: Option<&str>) -> Option<Duration> {
    let default = Some(Duration::from_millis(DEFAULT_MOLECULE_DRAIN_MS));
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return default;
    };
    match raw.parse::<u64>() {
        Ok(0) => None,
        Ok(ms) => Some(Duration::from_millis(ms)),
        Err(_) => {
            tracing::warn!(
                raw,
                env = MOLECULE_DRAIN_MS_ENV,
                "invalid molecule drain interval; using the default"
            );
            default
        }
    }
}

fn parse_number(raw: Option<&str>, env: &str, default: u64) -> u64 {
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return default;
    };
    raw.parse::<u64>().unwrap_or_else(|_| {
        tracing::warn!(
            raw,
            env,
            default,
            "invalid molecule drain setting; using the default"
        );
        default
    })
}

/// Handle for the periodic drain task. Aborted on shutdown and on Drop.
pub struct BackgroundMoleculeDrainTask {
    task: BackgroundTask,
}

impl BackgroundMoleculeDrainTask {
    /// No-op handle when the task is off.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            task: BackgroundTask::disabled(),
        }
    }

    /// Spawn the task on the current Tokio runtime.
    pub fn spawn(
        db_ops: Arc<DbOperations>,
        policy: MoleculeDrainPolicy,
        interval: Duration,
    ) -> Self {
        db_ops
            .atoms()
            .drain_state()
            .set_interval_ms(u64::try_from(interval.as_millis()).unwrap_or(u64::MAX));
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let wake = db_ops.atoms().drain_wake_signal();
        // lint:spawn-bare-ok process-lifetime drain loop, not request-scoped.
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;
            let mut cursor = DrainCursor::new();
            // The watch lives as long as the task: it remembers the last write scope
            // of another task across the wakes.
            let mut watch = WriterWatch::new();
            // The batch size shrinks after a read limit and grows back after
            // `REGROW_AFTER_WAKES` wakes that did work and had no read limit.
            let mut tips = policy.tips;
            let mut clean_wakes = 0u32;
            let mut failures = 0u64;
            let mut retry_at: Option<std::time::Instant> = None;
            loop {
                let woken = tokio::select! {
                    _ = ticker.tick() => false,
                    () = wake.notified() => true,
                };
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
                if woken {
                    // A wake signal means new work: a drop, or a molecule named by
                    // hand. The wait that a failed wake set does not apply to it.
                    failures = 0;
                    retry_at = None;
                }
                if retry_at.is_some_and(|at| std::time::Instant::now() < at) {
                    continue;
                }
                cursor.restart();
                let wake_result = run_wake(
                    db_ops.atoms(),
                    &stop_flag,
                    &mut cursor,
                    (policy, &mut tips),
                    &mut watch,
                )
                .await;
                regrow_after(&wake_result, &mut clean_wakes, &mut tips, policy.tips);
                let error = wake_result.error;
                failures = if error.is_some() { failures + 1 } else { 0 };
                let retry_in = (failures > 0).then(|| backoff_after(failures, interval));
                retry_at = retry_in.map(|wait| std::time::Instant::now() + wait);
                db_ops.atoms().drain_state().note_run(
                    error,
                    failures,
                    retry_in.map(|wait| u64::try_from(wait.as_millis()).unwrap_or(u64::MAX)),
                );
            }
        });
        Self {
            task: BackgroundTask::started(handle, stop),
        }
    }

    /// Signal the loop to exit and abort the task.
    pub fn stop(&self) {
        self.task.stop("molecule drain join lock");
    }
}

impl Drop for BackgroundMoleculeDrainTask {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The smallest batch the drain shrinks to after read limits.
const MIN_TIPS: usize = 8;

/// A read call spent its budget of pages or keys.
fn is_read_limit(failure: &crate::schema::SchemaError) -> bool {
    matches!(
        failure,
        crate::schema::SchemaError::ReadResourceLimit { .. }
    ) || failure
        .to_string()
        .to_ascii_lowercase()
        .contains("read resource limit")
}

/// The wait before the next wake after `failures` wakes in a row that failed.
fn backoff_after(failures: u64, interval: Duration) -> Duration {
    let base = interval.max(BACKOFF_BASE_MIN);
    let shift = u32::try_from(failures.min(10)).unwrap_or(10);
    base.saturating_mul(1 << shift).min(BACKOFF_MAX)
}

/// What one wake did.
struct WakeResult {
    /// The error of the step that failed.
    error: Option<String>,
    /// Steps that wrote a batch.
    batches: u64,
    /// A batch hit the read limit and the batch size went down.
    shrank: bool,
}

/// Wakes that did work, and had no read limit, before the batch size doubles again.
const REGROW_AFTER_WAKES: u32 = 5;

/// Count a clean wake. After `REGROW_AFTER_WAKES` of them the batch size doubles, up
/// to the size the policy sets. A wake with an error or a read limit starts the count
/// again. A wake that did no work says nothing about the read limit and is not counted.
fn regrow_after(result: &WakeResult, clean_wakes: &mut u32, tips: &mut usize, max_tips: usize) {
    if result.error.is_some() || result.shrank {
        *clean_wakes = 0;
        return;
    }
    if result.batches == 0 || *tips >= max_tips {
        return;
    }
    *clean_wakes += 1;
    if *clean_wakes >= REGROW_AFTER_WAKES {
        *clean_wakes = 0;
        *tips = (*tips * 2).min(max_tips);
        tracing::info!(
            target: "fold_node::database",
            tips = *tips,
            "molecule drain batch is larger again after clean wakes"
        );
    }
}

/// One wake: batches until the list is done, the batch budget is spent, or a step
/// fails.
async fn run_wake(
    atoms: &AtomStore,
    stop: &AtomicBool,
    cursor: &mut DrainCursor,
    (policy, tips): (MoleculeDrainPolicy, &mut usize),
    watch: &mut WriterWatch,
) -> WakeResult {
    let mut result = WakeResult {
        error: None,
        batches: 0,
        shrank: false,
    };
    for _ in 0..policy.batches {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let started = std::time::Instant::now();
        match atoms
            .molecule_drain_step(cursor, DrainLimits::new(*tips))
            .await
        {
            Ok(Some(outcome)) => {
                let wrote = note_outcome(&outcome.candidate.molecule, &outcome.kind, started);
                publish(atoms, cursor);
                // The watch takes the scopes of this step off its count after every
                // step, also after one that wrote no batch.
                let pause =
                    rest_after_cost(watch, (outcome.blocked, outcome.scopes), policy.duty_pct);
                if wrote {
                    result.batches += 1;
                    rest(pause, watch).await;
                }
            }
            Ok(None) => break,
            Err(failure) if *tips > MIN_TIPS && is_read_limit(&failure) => {
                // The batch read more pages than one call may spend: halve it. The
                // failed molecule went to the back of the line.
                *tips = (*tips / 2).max(MIN_TIPS);
                result.shrank = true;
                tracing::warn!(
                    target: "fold_node::database",
                    tips = *tips,
                    "molecule drain batch hit the read limit; the batch is smaller now"
                );
            }
            Err(failure) => {
                tracing::warn!(
                    target: "fold_node::database",
                    error = %failure,
                    "molecule drain stopped before its work was done"
                );
                result.error = Some(failure.to_string());
                break;
            }
        }
    }
    publish(atoms, cursor);
    result
}

/// Put the gauges and the refused molecules of the cursor in the report.
fn publish(atoms: &AtomStore, cursor: &DrainCursor) {
    let (pending, waiting, refused) = cursor.gauges();
    atoms.drain_state().set_gauges(pending, waiting, refused);
    atoms.drain_state().set_refused_sample(cursor.refused());
}

/// Log one step. Returns true when the step wrote a batch.
fn note_outcome(molecule: &str, kind: &OutcomeKind, started: std::time::Instant) -> bool {
    let took_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    match kind {
        OutcomeKind::Waiting | OutcomeKind::Refused(_) => {
            tracing::info!(
                target: "fold_node::database",
                molecule,
                outcome = ?kind,
                "molecule drain left a molecule"
            );
            false
        }
        _ => {
            tracing::debug!(
                target: "fold_node::database",
                molecule,
                outcome = ?kind,
                took_ms,
                "molecule drain batch"
            );
            true
        }
    }
}
