//! The retention window, and what the janitor and the purge report.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

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

/// The window and the totals of one store, since the node started.
#[derive(Debug)]
pub(crate) struct RetentionState {
    window_secs: u64,
    janitor_interval_ms: AtomicU64,
    runs: AtomicU64,
    last_run_unix_ms: AtomicU64,
    last_run_released: AtomicU64,
    released_by_janitor: AtomicU64,
    released_by_purge: AtomicU64,
    undecodable: AtomicU64,
    purge_leftover: AtomicU64,
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
            janitor_interval_ms: AtomicU64::new(0),
            runs: AtomicU64::new(0),
            last_run_unix_ms: AtomicU64::new(0),
            last_run_released: AtomicU64::new(0),
            released_by_janitor: AtomicU64::new(0),
            released_by_purge: AtomicU64::new(0),
            undecodable: AtomicU64::new(0),
            purge_leftover: AtomicU64::new(0),
            last_error: Mutex::new(None),
        }
    }

    pub(crate) fn window_nanos(&self) -> u64 {
        self.window_secs.saturating_mul(1_000_000_000)
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

    /// An erase left records to the window, because the key held too many.
    pub(crate) fn note_leftover(&self, leftover: u64) {
        self.purge_leftover.fetch_add(leftover, Ordering::Relaxed);
    }

    /// One release batch of the janitor ended. The total counts at once, so a
    /// wake that rests between many pages shows its progress.
    pub(crate) fn note_released(&self, released: u64) {
        self.released_by_janitor
            .fetch_add(released, Ordering::Relaxed);
    }

    /// One janitor wake ended: how many records it released, and why it
    /// stopped early.
    pub(crate) fn note_run(&self, released: u64, error: Option<String>) {
        self.runs.fetch_add(1, Ordering::Relaxed);
        self.last_run_released.store(released, Ordering::Relaxed);
        let now = chrono::Utc::now().timestamp_millis();
        self.last_run_unix_ms
            .store(u64::try_from(now).unwrap_or(0), Ordering::Relaxed);
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
            purge_leftover: self.purge_leftover.load(Ordering::Relaxed),
            queue_depth: None,
            queue_depth_capped: false,
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
    /// Records a purge left for the janitor, because the key held more than
    /// the limit of one request.
    pub purge_leftover: u64,
    /// Entries in the queue when the report was built, counted up to a limit.
    /// `None` means the count failed.
    pub queue_depth: Option<u64>,
    /// True when the queue holds at least `queue_depth` entries (the count
    /// stopped at its limit).
    pub queue_depth_capped: bool,
    pub last_error: Option<String>,
}
