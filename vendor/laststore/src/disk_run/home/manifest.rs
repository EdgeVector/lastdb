//! CURRENT: the one sealed file that names the readable runs and its bounded loaders.

use crate::disk_run::codec::{corrupt, get_len, get_slice, get_varint, put_varint};
use crate::disk_run::{MAX_RUN_KEY_BYTES, RUN_SUFFIX};
use crate::{Error, Result};
use std::fs;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use uuid::Uuid;

pub(super) const MAGIC: &[u8; 8] = b"LDRCUR2\0";
/// File name of the sealed list of runs and the frontier.
pub const CURRENT: &str = "CURRENT";
/// Directory name of the immutable run files.
pub const RUNS: &str = "runs";
pub(super) const MAX_LEVELS: usize = 32;
pub(super) const MAX_LEVEL_RUNS: usize = 4096;
pub(super) const MAX_CURRENT_BYTES: usize = 64 * 1024 * 1024;
const ARC_HEADER: usize = 2 * size_of::<usize>();

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RunMeta {
    pub(super) run_id: Uuid,
    pub(super) records: u64,
    pub(super) tombstones: u64,
    pub(super) record_bytes: u64,
    pub(super) tombstone_bytes: u64,
    pub(super) file_bytes: u64,
    pub(super) first_key: Vec<u8>,
    pub(super) last_key: Vec<u8>,
}

impl RunMeta {
    pub(super) fn covers(&self, key: &[u8]) -> bool {
        self.first_key.as_slice() <= key && key <= self.last_key.as_slice()
    }

    pub(super) fn meets(&self, other: &RunMeta) -> bool {
        self.first_key <= other.last_key && other.first_key <= self.last_key
    }

    pub(super) fn meets_merge_interval(
        &self,
        after: Option<&[u8]>,
        through: Option<&[u8]>,
    ) -> bool {
        after.is_none_or(|a| self.last_key.as_slice() > a)
            && through.is_none_or(|t| self.first_key.as_slice() <= t)
    }

    pub(super) fn overlaps(&self, lower: &[u8], upper: Option<&[u8]>) -> bool {
        self.last_key.as_slice() >= lower && upper.is_none_or(|u| self.first_key.as_slice() < u)
    }
}

#[derive(Debug)]
pub(super) struct RunFile {
    pub(super) meta: RunMeta,
    pub(super) path: PathBuf,
    obsolete: AtomicBool,
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

    pub(super) fn retire(&self) {
        self.obsolete.store(true, Ordering::Release);
    }

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

#[derive(Debug, Clone)]
pub(super) struct Manifest {
    pub(super) home_id: Uuid,
    pub(super) generation: u64,
    pub(super) frontier: u64,
    pub(super) levels: Vec<Vec<Arc<RunFile>>>,
    pub(super) merging: Vec<Arc<RunFile>>,
    pub(super) merge_cut: Option<Vec<u8>>,
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

    pub(super) fn newest_first(&self) -> impl Iterator<Item = &Arc<RunFile>> {
        self.levels
            .iter()
            .flatten()
            .chain(&self.merging)
            .chain(&self.base)
    }

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

    pub(in crate::disk_run::home) fn decode(bytes: &[u8], runs: &Path) -> Result<Self> {
        decode_with(bytes, runs, &mut Admission::unbounded())
    }

    pub(in crate::disk_run::home) fn decode_bounded(
        bytes: &[u8],
        cap: usize,
        runs: &Path,
        maximum: u64,
    ) -> Result<Self> {
        let mut budget = Admission { maximum, used: 0 };
        budget.add(cap as u64 + (size_of::<Self>() + ARC_HEADER) as u64)?;
        decode_with(bytes, runs, &mut budget)
    }

    pub(super) fn check_shape(&self) -> Result<()> {
        check_shape_with(self, &mut Admission::unbounded())
    }

    pub(in crate::disk_run::home) fn retained_bytes(&self) -> u64 {
        let mut bytes = (size_of::<Self>()
            + ARC_HEADER
            + self.levels.capacity() * size_of::<Vec<Arc<RunFile>>>()
            + self.merge_cut.as_ref().map_or(0, Vec::capacity)) as u64;
        for list in self.levels.iter().chain([&self.merging, &self.base]) {
            bytes = bytes.saturating_add((list.capacity() * size_of::<Arc<RunFile>>()) as u64);
            for run in list {
                let per_run = size_of::<RunFile>()
                    + ARC_HEADER
                    + run.path.capacity()
                    + run.meta.first_key.capacity()
                    + run.meta.last_key.capacity();
                bytes = bytes.saturating_add(per_run as u64);
            }
        }
        bytes
    }
}

fn put_runs(out: &mut Vec<u8>, runs: &[Arc<RunFile>]) {
    put_varint(out, runs.len() as u64);
    for run in runs {
        let (m, h) = (&run.meta, run.hidden.load(Ordering::Acquire));
        out.extend_from_slice(m.run_id.as_bytes());
        for v in [
            m.records,
            m.tombstones,
            m.record_bytes,
            m.tombstone_bytes,
            m.file_bytes,
            h,
        ] {
            put_varint(out, v);
        }
        for key in [&m.first_key, &m.last_key] {
            put_varint(out, key.len() as u64);
            out.extend_from_slice(key);
        }
    }
}

fn decode_with(bytes: &[u8], runs: &Path, budget: &mut Admission) -> Result<Manifest> {
    if bytes.get(..8) != Some(MAGIC.as_slice()) || bytes.len() < 24 {
        return Err(corrupt("bad CURRENT"));
    }
    let home_id = Uuid::from_slice(&bytes[8..24]).map_err(|_| corrupt("bad CURRENT"))?;
    let mut at = 24;
    let (generation, frontier) = (get_varint(bytes, &mut at)?, get_varint(bytes, &mut at)?);
    let count = get_len(bytes, &mut at, MAX_LEVELS)?;
    let mut levels = budget.array(count)?;
    for _ in 0..count {
        levels.push(run_list(bytes, &mut at, runs, budget)?);
    }
    let merging = run_list(bytes, &mut at, runs, budget)?;
    let cut = get_len(bytes, &mut at, MAX_RUN_KEY_BYTES + 1)?;
    let merge_cut = cut
        .checked_sub(1)
        .map(|len| budget.copy(get_slice(bytes, &mut at, len)?))
        .transpose()?;
    let base = run_list(bytes, &mut at, runs, budget)?;
    if at != bytes.len() || generation == 0 {
        return Err(corrupt("bad CURRENT"));
    }
    let manifest = Manifest {
        home_id,
        generation,
        frontier,
        levels,
        merging,
        merge_cut,
        base,
    };
    check_shape_with(&manifest, budget)?;
    Ok(manifest)
}

fn run_list(
    bytes: &[u8],
    at: &mut usize,
    directory: &Path,
    budget: &mut Admission,
) -> Result<Vec<Arc<RunFile>>> {
    let count = get_len(bytes, at, MAX_LEVEL_RUNS)?;
    let mut runs = budget.array(count)?;
    for _ in 0..count {
        let run_id = Uuid::from_slice(get_slice(bytes, at, 16)?)
            .map_err(|_| corrupt("bad run entry in CURRENT"))?;
        let mut v = [0u64; 6];
        for slot in &mut v {
            *slot = get_varint(bytes, at)?;
        }
        let [records, tombstones, record_bytes, tombstone_bytes, file_bytes, hidden] = v;
        let first_len = get_len(bytes, at, MAX_RUN_KEY_BYTES)?;
        let first_key = budget.copy(get_slice(bytes, at, first_len)?)?;
        let last_len = get_len(bytes, at, MAX_RUN_KEY_BYTES)?;
        let last_key = budget.copy(get_slice(bytes, at, last_len)?)?;
        if run_id.is_nil()
            || records == 0
            || tombstones > records
            || tombstone_bytes > record_bytes
            || first_key > last_key
        {
            return Err(corrupt("bad run entry in CURRENT"));
        }
        budget.add((size_of::<RunFile>() + ARC_HEADER) as u64)?;
        let path = budget.path(directory, run_id)?;
        runs.push(RunFile::from_parts(
            RunMeta {
                run_id,
                records,
                tombstones,
                record_bytes,
                tombstone_bytes,
                file_bytes,
                first_key,
                last_key,
            },
            path,
            hidden,
        ));
    }
    Ok(runs)
}

fn check_shape_with(manifest: &Manifest, budget: &mut Admission) -> Result<()> {
    let mut ids = budget.array(manifest.newest_first().count())?;
    ids.extend(manifest.newest_first().map(|run| run.meta.run_id));
    ids.sort_unstable();
    if ids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(corrupt("CURRENT names a run twice"));
    }
    if manifest.merge_cut.is_some() && manifest.merging.is_empty() {
        return Err(corrupt("CURRENT has a base merge cut but no merging runs"));
    }
    if !manifest
        .base
        .windows(2)
        .all(|p| p[0].meta.last_key < p[1].meta.first_key)
        || manifest.base.iter().any(|run| run.meta.tombstones > 0)
    {
        return Err(corrupt("CURRENT has an unordered base"));
    }
    Ok(())
}

struct Admission {
    maximum: u64,
    used: u64,
}

impl Admission {
    fn unbounded() -> Self {
        Self {
            maximum: u64::MAX,
            used: 0,
        }
    }
    fn add(&mut self, bytes: u64) -> Result<()> {
        self.used = self
            .used
            .checked_add(bytes)
            .filter(|n| *n <= self.maximum)
            .ok_or(Error::ReadBudgetExceeded)?;
        Ok(())
    }
    fn array<T>(&mut self, count: usize) -> Result<Vec<T>> {
        let bytes = count
            .checked_mul(size_of::<T>())
            .ok_or(Error::ReadBudgetExceeded)?;
        self.add(bytes as u64)?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(count)
            .map_err(|_| Error::ReadBudgetExceeded)?;
        self.add((values.capacity().saturating_sub(count) * size_of::<T>()) as u64)?;
        Ok(values)
    }
    fn copy(&mut self, bytes: &[u8]) -> Result<Vec<u8>> {
        let mut value = self.array(bytes.len())?;
        value.extend_from_slice(bytes);
        Ok(value)
    }
    fn path(&mut self, directory: &Path, id: Uuid) -> Result<PathBuf> {
        let mut name = [0; 42];
        let _ = id.hyphenated().encode_lower(&mut name[..36]);
        name[36..].copy_from_slice(RUN_SUFFIX.as_bytes());
        let name = std::str::from_utf8(&name).map_err(|_| corrupt("bad run path"))?;
        let size = directory
            .as_os_str()
            .len()
            .checked_add(1 + name.len())
            .ok_or(Error::ReadBudgetExceeded)?;
        self.add(size as u64)?;
        let mut path = PathBuf::new();
        path.try_reserve_exact(size)
            .map_err(|_| Error::ReadBudgetExceeded)?;
        self.add(path.capacity().saturating_sub(size) as u64)?;
        path.push(directory);
        path.push(name);
        Ok(path)
    }
}
