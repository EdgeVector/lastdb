//! The retention window, and what the janitor and the purge report.

use super::keys::{QUEUE_DIRECTORY, QUEUE_ENTRIES};
use crate::db_operations::atom_store::time_queue::TimeQueue;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

/// Env: how long a kept conflict record stays, in seconds.
pub const CONFLICT_RETENTION_SECS_ENV: &str = "LASTDB_CONFLICT_RETENTION_SECS";
/// The default window: 30 days.
pub const DEFAULT_CONFLICT_RETENTION_SECS: u64 = 30 * 24 * 60 * 60;

/// The window in seconds. Unset, empty, zero or invalid gives the default.
pub(crate) fn parse_retention_secs(raw: Option<&str>) -> u64 {
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return DEFAULT_CONFLICT_RETENTION_SECS;
    };
    match raw.parse::<u64>() {
        Ok(secs) if secs > 0 => secs,
        _ => {
            tracing::warn!(
                raw,
                env = CONFLICT_RETENTION_SECS_ENV,
                default_secs = DEFAULT_CONFLICT_RETENTION_SECS,
                "invalid conflict retention window; using the default"
            );
            DEFAULT_CONFLICT_RETENTION_SECS
        }
    }
}

/// A forward excursion of the wall clock beyond this is ignored.
const CLOCK_TOLERANCE_NANOS: u64 = 24 * 60 * 60 * 1_000_000_000;

fn wall_clock_nanos() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX)).unwrap_or(0)
}

/// The window and the totals of one store, since the node started.
#[derive(Debug)]
pub(crate) struct RetentionState {
    window_secs: u64,
    /// The layout of the queue. The slice of a bucket follows the window.
    queue: TimeQueue,
    /// The wall clock when the store was made, and the monotonic clock then.
    /// They give an estimate of the time that a clock excursion cannot move.
    anchor_wall_nanos: u64,
    anchor: std::time::Instant,
    janitor_interval_ms: AtomicU64,
    runs: AtomicU64,
    last_run_unix_ms: AtomicU64,
    last_run_released: AtomicU64,
    released_by_janitor: AtomicU64,
    released_by_purge: AtomicU64,
    undecodable: AtomicU64,
    purge_capped_slots: AtomicU64,
    pending_drains: AtomicU64,
    consecutive_failures: AtomicU64,
    next_retry_unix_ms: AtomicU64,
    last_error: Mutex<Option<String>>,
}

impl Default for RetentionState {
    fn default() -> Self {
        Self::with_window(parse_retention_secs(
            std::env::var(CONFLICT_RETENTION_SECS_ENV).ok().as_deref(),
        ))
    }
}

impl RetentionState {
    pub(crate) fn with_window(window_secs: u64) -> Self {
        Self {
            window_secs,
            queue: TimeQueue::for_window(
                QUEUE_ENTRIES,
                QUEUE_DIRECTORY,
                window_secs.saturating_mul(1_000_000_000),
            ),
            anchor_wall_nanos: wall_clock_nanos(),
            anchor: std::time::Instant::now(),
            janitor_interval_ms: AtomicU64::new(0),
            runs: AtomicU64::new(0),
            last_run_unix_ms: AtomicU64::new(0),
            last_run_released: AtomicU64::new(0),
            released_by_janitor: AtomicU64::new(0),
            released_by_purge: AtomicU64::new(0),
            undecodable: AtomicU64::new(0),
            purge_capped_slots: AtomicU64::new(0),
            pending_drains: AtomicU64::new(0),
            consecutive_failures: AtomicU64::new(0),
            next_retry_unix_ms: AtomicU64::new(0),
            last_error: Mutex::new(None),
        }
    }

    /// The layout of the retention queue.
    pub(crate) fn queue(&self) -> TimeQueue {
        self.queue
    }

    pub(crate) fn window_nanos(&self) -> u64 {
        self.window_secs.saturating_mul(1_000_000_000)
    }

    /// The clock of the queue: the wall clock, but never more than a day ahead
    /// of the time that the monotonic clock gives since the store was made.
    /// The written expiry and the due test of the janitor both read it. A
    /// forward excursion of the wall clock longer than a day cannot give a
    /// record a far-future expiry, and cannot make the janitor release a
    /// record at once. A smaller forward jump moves the clock, and the
    /// janitor then releases records early by at most the size of the jump.
    pub(crate) fn clock_nanos(&self) -> u64 {
        let elapsed = u64::try_from(self.anchor.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let estimate = self.anchor_wall_nanos.saturating_add(elapsed);
        wall_clock_nanos().min(estimate.saturating_add(CLOCK_TOLERANCE_NANOS))
    }

    /// The janitor says how often it runs (zero: it is off).
    pub(crate) fn set_janitor_interval_ms(&self, interval_ms: u64) {
        self.janitor_interval_ms
            .store(interval_ms, Ordering::Relaxed);
    }

    pub(crate) fn note_undecodable(&self) {
        self.undecodable.fetch_add(1, Ordering::Relaxed);
    }

    /// An erase released records with the keys it removed.
    pub(crate) fn note_purge(&self, released: u64) {
        self.released_by_purge
            .fetch_add(released, Ordering::Relaxed);
    }

    /// An erase left records of some slots to the window, because the slots
    /// held more than one request releases.
    pub(crate) fn note_capped_slots(&self, slots: u64) {
        self.purge_capped_slots.fetch_add(slots, Ordering::Relaxed);
    }

    /// One release batch of the janitor ended. The total counts at once, so a
    /// wake that rests between many pages shows its progress.
    pub(crate) fn note_released(&self, released: u64) {
        self.released_by_janitor
            .fetch_add(released, Ordering::Relaxed);
    }

    /// The janitor holds this many slot drains that are not finished.
    pub(crate) fn set_pending_drains(&self, pending: u64) {
        self.pending_drains.store(pending, Ordering::Relaxed);
    }

    /// One janitor wake ended: how many records it released, and why it
    /// stopped early. `failures` is the number of wakes in a row that ended
    /// with an error (zero after a good wake), and `retry_in` is the wait that
    /// the janitor now keeps before its next wake.
    pub(crate) fn note_run(
        &self,
        released: u64,
        error: Option<String>,
        failures: u64,
        retry_in: Option<Duration>,
    ) {
        self.runs.fetch_add(1, Ordering::Relaxed);
        self.last_run_released.store(released, Ordering::Relaxed);
        let now = chrono::Utc::now().timestamp_millis();
        let now = u64::try_from(now).unwrap_or(0);
        self.last_run_unix_ms.store(now, Ordering::Relaxed);
        self.consecutive_failures.store(failures, Ordering::Relaxed);
        let retry_at = retry_in.map_or(0, |wait| {
            now.saturating_add(u64::try_from(wait.as_millis()).unwrap_or(u64::MAX))
        });
        self.next_retry_unix_ms.store(retry_at, Ordering::Relaxed);
        *self
            .last_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = error;
    }

    pub(crate) fn snapshot(&self) -> ConflictRetentionReport {
        ConflictRetentionReport {
            window_secs: self.window_secs,
            janitor_interval_ms: self.janitor_interval_ms.load(Ordering::Relaxed),
            runs: self.runs.load(Ordering::Relaxed),
            last_run_unix_ms: self.last_run_unix_ms.load(Ordering::Relaxed),
            last_run_released: self.last_run_released.load(Ordering::Relaxed),
            released_by_janitor: self.released_by_janitor.load(Ordering::Relaxed),
            released_by_purge: self.released_by_purge.load(Ordering::Relaxed),
            undecodable_rows: self.undecodable.load(Ordering::Relaxed),
            purge_capped_slots: self.purge_capped_slots.load(Ordering::Relaxed),
            queue_depth: None,
            queue_depth_capped: false,
            queue_buckets: None,
            pending_drains: self.pending_drains.load(Ordering::Relaxed),
            consecutive_failures: self.consecutive_failures.load(Ordering::Relaxed),
            next_retry_unix_ms: self.next_retry_unix_ms.load(Ordering::Relaxed),
            last_error: self
                .last_error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        }
    }
}

/// The retention report in `lastdb status`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConflictRetentionReport {
    /// How long a kept conflict record stays, in seconds.
    pub window_secs: u64,
    /// How often the janitor wakes, in milliseconds. Zero means it is off.
    pub janitor_interval_ms: u64,
    /// Janitor wakes since the node started.
    pub runs: u64,
    /// Unix milliseconds of the last janitor wake. Zero means none yet.
    pub last_run_unix_ms: u64,
    /// Records the last wake released.
    pub last_run_released: u64,
    pub released_by_janitor: u64,
    /// Records released with the key by a purge, a TTL sweep or a delete.
    pub released_by_purge: u64,
    /// History rows that did not decode. They keep their references.
    pub undecodable_rows: u64,
    /// Slots (of an erase) that held more kept records than one request
    /// releases. The window ends the records that the erase left.
    pub purge_capped_slots: u64,
    /// Entries in the queue when the report was built, counted up to a limit.
    /// `None` means that the directory of the queue could not be read.
    pub queue_depth: Option<u64>,
    /// True when the queue holds at least `queue_depth` entries (the count
    /// stopped at its limit, or at a bucket that it could not read).
    pub queue_depth_capped: bool,
    /// Rows in the bucket directory of the queue: buckets that hold, or held,
    /// an entry. The count reads the directory only.
    #[serde(default)]
    pub queue_buckets: Option<u64>,
    /// Slot drains the janitor has started and not finished.
    #[serde(default)]
    pub pending_drains: u64,
    /// Janitor wakes in a row that ended with an error. The janitor waits
    /// longer after each one, and a good wake sets the count to zero.
    #[serde(default)]
    pub consecutive_failures: u64,
    /// Unix milliseconds of the next janitor wake after an error. Zero means
    /// the janitor keeps its ordinary interval.
    #[serde(default)]
    pub next_retry_unix_ms: u64,
    pub last_error: Option<String>,
}
