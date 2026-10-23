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

#[cfg(target_os = "macos")]
mod ordered;

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
static FULL_SYNCS: AtomicU64 = AtomicU64::new(0);
static FULL_SYNC_MICROS: AtomicU64 = AtomicU64::new(0);
static ORDERED_SYNCS: AtomicU64 = AtomicU64::new(0);
static ORDERED_SYNC_MICROS: AtomicU64 = AtomicU64::new(0);
static FILE_SET_SYNCS: AtomicU64 = AtomicU64::new(0);
static FILE_SET_SYNC_MICROS: AtomicU64 = AtomicU64::new(0);

/// Process lifetime counts of durability barriers, by kind.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SyncStats {
    /// Apple device-cache drains, including final complete-commit barriers.
    pub full_syncs: u64,
    /// Total time in Apple device-cache drains, in microseconds.
    pub full_sync_micros: u64,
    /// Ordered Apple file/directory barriers inside complete native commits.
    pub ordered_syncs: u64,
    /// Total time in those ordered barriers, in microseconds.
    pub ordered_sync_micros: u64,
    /// Bounded native file sets flushed before a publication or final device drain.
    pub file_set_syncs: u64,
    /// Wall time for those file sets, including any scoped worker setup.
    pub file_set_sync_micros: u64,
    /// Group data files made durable (`F_FULLFSYNC` on Apple).
    pub data_syncs: u64,
    /// Total time spent in those barriers, in microseconds.
    pub data_sync_micros: u64,
    /// Key sidecars flushed before their rename (big groups only).
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
        full_syncs: FULL_SYNCS.load(Ordering::Relaxed),
        full_sync_micros: FULL_SYNC_MICROS.load(Ordering::Relaxed),
        ordered_syncs: ORDERED_SYNCS.load(Ordering::Relaxed),
        ordered_sync_micros: ORDERED_SYNC_MICROS.load(Ordering::Relaxed),
        file_set_syncs: FILE_SET_SYNCS.load(Ordering::Relaxed),
        file_set_sync_micros: FILE_SET_SYNC_MICROS.load(Ordering::Relaxed),
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
    if ordered::sync_if_active(file)? {
        return Ok(());
    }
    // Rust's sync_data also issues F_FULLFSYNC on Apple. Use the explicit
    // strongest barrier once and propagate errors; never fall back to fsync.
    full_barrier(file)
}

#[cfg(target_os = "macos")]
fn full_barrier(file: &File) -> Result<()> {
    let _timing = crate::commit_timing::stage("device_flush_us");
    crate::commit_timing::count("device_flushes", 1);
    timed(&FULL_SYNCS, &FULL_SYNC_MICROS, || {
        rustix::fs::fcntl_fullfsync(file).map_err(std::io::Error::from)?;
        Ok(())
    })
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

/// Order every private dependency and this file before its visible rename.
/// The enclosing native commit still requires a final full device sync.
pub(crate) fn sync_publication_file(file: &File) -> Result<()> {
    timed(&DATA_SYNCS, &DATA_SYNC_MICROS, || {
        #[cfg(target_os = "macos")]
        if ordered::publication_if_active(file)? {
            return Ok(());
        }
        barrier(file)
    })
}

/// Make a key sidecar durable before it replaces the previous one. Only a
/// group at or above the sidecar flush floor calls this.
pub(crate) fn sync_sidecar_file(file: &File) -> Result<()> {
    timed(&SIDECAR_SYNCS, &SIDECAR_SYNC_MICROS, || barrier(file))
}

/// Make a directory entry durable.
pub(crate) fn sync_directory(dir: &File) -> Result<()> {
    timed(&DIR_SYNCS, &DIR_SYNC_MICROS, || {
        #[cfg(target_os = "macos")]
        return barrier(dir);
        #[cfg(not(target_os = "macos"))]
        {
            dir.sync_all()?;
            Ok(())
        }
    })
}

/// Keep intermediate native publications ordered, then drain the device once.
/// Other platforms retain their existing per-file durability barriers.
pub(crate) fn complete_native_commit<T>(
    root: &std::path::Path,
    commit: impl FnOnce() -> Result<T>,
) -> Result<T> {
    #[cfg(target_os = "macos")]
    return ordered::complete(root, commit);
    #[cfg(not(target_os = "macos"))]
    {
        let _ = root;
        commit()
    }
}
