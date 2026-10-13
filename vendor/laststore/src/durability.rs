//! Platform durability barriers.
//!
//! Every data, sidecar and directory barrier of the store goes through this
//! module, so one set of process counters says how many barriers a workload
//! pays and how long each takes. The counters are process-wide: a node runs
//! one store.

use crate::Result;
use std::fs::File;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static DATA_SYNCS: AtomicU64 = AtomicU64::new(0);
static DATA_SYNC_MICROS: AtomicU64 = AtomicU64::new(0);
static SIDECAR_SYNCS: AtomicU64 = AtomicU64::new(0);
static SIDECAR_SYNC_MICROS: AtomicU64 = AtomicU64::new(0);
static DIR_SYNCS: AtomicU64 = AtomicU64::new(0);
static DIR_SYNC_MICROS: AtomicU64 = AtomicU64::new(0);
static SCOPE_FLUSHES_SERIAL: AtomicU64 = AtomicU64::new(0);
static SCOPE_FLUSHES_PARALLEL: AtomicU64 = AtomicU64::new(0);
static WHOLE_FLUSHES_SERIAL: AtomicU64 = AtomicU64::new(0);
static WHOLE_FLUSHES_PARALLEL: AtomicU64 = AtomicU64::new(0);

/// Process lifetime counts of durability barriers, by kind.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SyncStats {
    /// Group data files made durable (`F_FULLFSYNC` on Apple).
    pub data_syncs: u64,
    /// Total time spent in those barriers, in microseconds.
    pub data_sync_micros: u64,
    /// Key sidecars made durable when a clean group was reaped.
    pub sidecar_syncs: u64,
    /// Total time spent in sidecar barriers, in microseconds.
    pub sidecar_sync_micros: u64,
    /// Directories made durable.
    pub dir_syncs: u64,
    /// Total time spent in directory barriers, in microseconds.
    pub dir_sync_micros: u64,
    /// Scoped flushes (one per transaction barrier), walked one group at a time.
    pub scope_flushes_serial: u64,
    /// Scoped flushes that synced their groups with parallel workers.
    pub scope_flushes_parallel: u64,
    /// Whole-store flushes (the 500 ms tick, shutdown), walked serially.
    pub whole_flushes_serial: u64,
    /// Whole-store flushes that synced their groups with parallel workers.
    pub whole_flushes_parallel: u64,
}

/// Read the process counters.
#[must_use]
pub fn sync_stats() -> SyncStats {
    SyncStats {
        data_syncs: DATA_SYNCS.load(Ordering::Relaxed),
        data_sync_micros: DATA_SYNC_MICROS.load(Ordering::Relaxed),
        sidecar_syncs: SIDECAR_SYNCS.load(Ordering::Relaxed),
        sidecar_sync_micros: SIDECAR_SYNC_MICROS.load(Ordering::Relaxed),
        dir_syncs: DIR_SYNCS.load(Ordering::Relaxed),
        dir_sync_micros: DIR_SYNC_MICROS.load(Ordering::Relaxed),
        scope_flushes_serial: SCOPE_FLUSHES_SERIAL.load(Ordering::Relaxed),
        scope_flushes_parallel: SCOPE_FLUSHES_PARALLEL.load(Ordering::Relaxed),
        whole_flushes_serial: WHOLE_FLUSHES_SERIAL.load(Ordering::Relaxed),
        whole_flushes_parallel: WHOLE_FLUSHES_PARALLEL.load(Ordering::Relaxed),
    }
}

pub(crate) fn note_scope_flush(parallel: bool) {
    let counter = if parallel {
        &SCOPE_FLUSHES_PARALLEL
    } else {
        &SCOPE_FLUSHES_SERIAL
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn note_whole_flush(parallel: bool) {
    let counter = if parallel {
        &WHOLE_FLUSHES_PARALLEL
    } else {
        &WHOLE_FLUSHES_SERIAL
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

fn timed(
    calls: &AtomicU64,
    micros: &AtomicU64,
    barrier: impl FnOnce() -> Result<()>,
) -> Result<()> {
    let started = Instant::now();
    let result = barrier();
    calls.fetch_add(1, Ordering::Relaxed);
    micros.fetch_add(
        u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    result
}

#[cfg(target_os = "macos")]
fn barrier(file: &File) -> Result<()> {
    // Rust's sync_data also issues F_FULLFSYNC on Apple. Use the explicit
    // strongest barrier once and propagate errors; never fall back to fsync.
    rustix::fs::fcntl_fullfsync(file).map_err(std::io::Error::from)?;
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn barrier(file: &File) -> Result<()> {
    file.sync_data()?;
    Ok(())
}

/// Make a group data file durable.
pub(crate) fn sync_dirty_file(file: &File) -> Result<()> {
    timed(&DATA_SYNCS, &DATA_SYNC_MICROS, || barrier(file))
}

/// Make a key sidecar durable before it replaces the previous one.
pub(crate) fn sync_sidecar_file(file: &File) -> Result<()> {
    timed(&SIDECAR_SYNCS, &SIDECAR_SYNC_MICROS, || barrier(file))
}

/// Make a directory entry durable.
pub(crate) fn sync_directory(dir: &File) -> Result<()> {
    timed(&DIR_SYNCS, &DIR_SYNC_MICROS, || {
        dir.sync_all()?;
        Ok(())
    })
}
