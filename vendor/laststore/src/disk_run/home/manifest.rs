//! CURRENT: the one sealed file that names the readable runs.
//!
//! CURRENT is one [`crate::frame`] frame under the home key. Each publish
//! uses a fresh random frame id, so a retried generation after a crash never
//! reuses a nonce. The payload names the home, the generation, the frontier
//! (complete commits), the runs of each tiered level (newest first), the runs
//! of a base merge in progress with its cut, and the runs of the base.

use super::super::codec::put_varint;
use super::super::RUN_SUFFIX;
use crate::Result;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use uuid::Uuid;

pub(super) const MAGIC: &[u8; 8] = b"LDRCUR2\0";
/// File name of the sealed list of runs and the frontier.
pub const CURRENT: &str = "CURRENT";
/// Directory name of the immutable run files.
pub const RUNS: &str = "runs";
/// Most levels one CURRENT may name.
pub(super) const MAX_LEVELS: usize = 32;
/// Most runs one level, the merging runs, or the base may hold.
pub(super) const MAX_LEVEL_RUNS: usize = 4096;
/// Largest CURRENT file a reader accepts.
pub(super) const MAX_CURRENT_BYTES: usize = 64 * 1024 * 1024;

/// What CURRENT records about one run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RunMeta {
    pub(super) run_id: Uuid,
    pub(super) records: u64,
    /// Tombstones among the records.
    pub(super) tombstones: u64,
    /// Key and value bytes before compression; a tombstone counts its key.
    pub(super) record_bytes: u64,
    /// Key bytes of the tombstones.
    pub(super) tombstone_bytes: u64,
    pub(super) file_bytes: u64,
    pub(super) first_key: Vec<u8>,
    pub(super) last_key: Vec<u8>,
}

impl RunMeta {
    /// True when `key` can be in this run.
    pub(super) fn covers(&self, key: &[u8]) -> bool {
        self.first_key.as_slice() <= key && key <= self.last_key.as_slice()
    }

    /// True when the key ranges of the two runs meet.
    pub(super) fn meets(&self, other: &RunMeta) -> bool {
        self.first_key <= other.last_key && other.first_key <= self.last_key
    }

    /// True when a run may contain a key in the base merge's `(after, through]`.
    pub(super) fn meets_merge_interval(
        &self,
        after: Option<&[u8]>,
        through: Option<&[u8]>,
    ) -> bool {
        after.is_none_or(|after| self.last_key.as_slice() > after)
            && through.is_none_or(|through| self.first_key.as_slice() <= through)
    }

    /// True when the run can hold a key in `[lower, upper)`.
    pub(super) fn overlaps(&self, lower: &[u8], upper: Option<&[u8]>) -> bool {
        self.last_key.as_slice() >= lower
            && upper.is_none_or(|upper| self.first_key.as_slice() < upper)
    }
}

/// One run file that a CURRENT names. The file stays while any held
/// manifest names it. A merge marks its inputs obsolete after it publishes
/// the replacement; the last holder then deletes the file.
#[derive(Debug)]
pub(super) struct RunFile {
    pub(super) meta: RunMeta,
    pub(super) path: PathBuf,
    obsolete: AtomicBool,
    /// Record bytes of the older versions that this run's records hide,
    /// plus one; zero while not yet counted. Only tiered runs need it.
    pub(super) hidden: AtomicU64,
}

impl RunFile {
    pub(super) fn new(runs: &Path, meta: RunMeta) -> Arc<Self> {
        let path = runs.join(format!("{}{RUN_SUFFIX}", meta.run_id));
        Self::from_parts(meta, path, 0)
    }

    pub(in crate::disk_run::home) fn from_parts(
        meta: RunMeta,
        path: PathBuf,
        hidden: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            path,
            meta,
            obsolete: AtomicBool::new(false),
            hidden: AtomicU64::new(hidden),
        })
    }

    /// Delete the file when the last holder drops it.
    pub(super) fn retire(&self) {
        self.obsolete.store(true, Ordering::Release);
    }

    /// Record bytes of the older versions this run hides, once counted.
    pub(super) fn hidden(&self) -> Option<u64> {
        self.hidden.load(Ordering::Acquire).checked_sub(1)
    }

    pub(super) fn set_hidden(&self, bytes: u64) {
        self.hidden
            .store(bytes.saturating_add(1), Ordering::Release);
    }
}

impl Drop for RunFile {
    fn drop(&mut self) {
        if self.obsolete.load(Ordering::Acquire) {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// One complete published state of a home.
#[derive(Debug, Clone)]
pub(super) struct Manifest {
    pub(super) home_id: Uuid,
    pub(super) generation: u64,
    pub(super) frontier: u64,
    /// Tiered levels, level 0 first; inside a level, the newest run first.
    pub(super) levels: Vec<Vec<Arc<RunFile>>>,
    /// Runs that a base merge in progress moves into the base, newest first.
    pub(super) merging: Vec<Arc<RunFile>>,
    /// Key through which the base holds the merging runs.
    pub(super) merge_cut: Option<Vec<u8>>,
    /// The base: non-overlapping runs in key order with no tombstones.
    pub(super) base: Vec<Arc<RunFile>>,
}

impl Manifest {
    pub(super) fn empty(home_id: Uuid) -> Self {
        Self {
            home_id,
            generation: 1,
            frontier: 0,
            levels: Vec::new(),
            merging: Vec::new(),
            merge_cut: None,
            base: Vec::new(),
        }
    }

    /// Every run, newest first: the order a read must search.
    pub(super) fn newest_first(&self) -> impl Iterator<Item = &Arc<RunFile>> {
        self.levels
            .iter()
            .flatten()
            .chain(&self.merging)
            .chain(&self.base)
    }

    /// The tiered runs, newest first.
    pub(super) fn tiered(&self) -> impl Iterator<Item = &Arc<RunFile>> {
        self.levels.iter().flatten()
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(self.home_id.as_bytes());
        put_varint(&mut out, self.generation);
        put_varint(&mut out, self.frontier);
        put_varint(&mut out, self.levels.len() as u64);
        for level in &self.levels {
            put_runs(&mut out, level);
        }
        put_runs(&mut out, &self.merging);
        match &self.merge_cut {
            Some(cut) => {
                put_varint(&mut out, cut.len() as u64 + 1);
                out.extend_from_slice(cut);
            }
            None => put_varint(&mut out, 0),
        }
        put_runs(&mut out, &self.base);
        out
    }

    /// Read and authenticate CURRENT, and check that it names `home_id`.
    pub(super) fn load(root: &Path, key: &[u8; 32], home_id: Uuid) -> Result<Self> {
        Self::load_paths(&root.join(CURRENT), &root.join(RUNS), key, home_id)
    }
}

fn put_runs(out: &mut Vec<u8>, runs: &[Arc<RunFile>]) {
    put_varint(out, runs.len() as u64);
    for run in runs {
        let meta = &run.meta;
        out.extend_from_slice(meta.run_id.as_bytes());
        for value in [
            meta.records,
            meta.tombstones,
            meta.record_bytes,
            meta.tombstone_bytes,
            meta.file_bytes,
            run.hidden.load(Ordering::Acquire),
        ] {
            put_varint(out, value);
        }
        for key in [&meta.first_key, &meta.last_key] {
            put_varint(out, key.len() as u64);
            out.extend_from_slice(key);
        }
    }
}
