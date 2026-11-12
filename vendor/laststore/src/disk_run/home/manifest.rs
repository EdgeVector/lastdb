//! CURRENT: the one sealed file that names the readable runs and its bounded loaders.

use super::write_private_file;
use crate::disk_run::codec::{corrupt, get_len, get_slice, get_varint, put_varint};
use crate::disk_run::{MergeSpace, RunMeta, MAX_RUN_KEY_BYTES, RUN_SUFFIX};
use crate::durability::sync_directory;
use crate::frame::{encode_frame, MIN_ENCODED_LEN};
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

#[derive(Debug)]
pub(super) struct RunFile {
    pub(super) meta: RunMeta,
    pub(super) path: PathBuf,
    obsolete: AtomicBool,
    pub(super) hidden: AtomicU64,
}

impl RunFile {
    pub(super) fn new(runs: &Path, meta: RunMeta) -> Arc<Self> {
        Arc::new(Self {
            path: runs.join(format!("{}{RUN_SUFFIX}", meta.run_id)),
            meta,
            obsolete: AtomicBool::new(false),
            hidden: AtomicU64::new(0),
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

    fn lists(&self) -> impl Iterator<Item = &Vec<Arc<RunFile>>> {
        self.levels.iter().chain([&self.merging, &self.base])
    }

    pub(super) fn newest_first(&self) -> impl Iterator<Item = &Arc<RunFile>> {
        self.lists().flatten()
    }

    pub(super) fn tiered(&self) -> impl Iterator<Item = &Arc<RunFile>> {
        self.levels.iter().flatten()
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut out = [MAGIC.as_slice(), self.home_id.as_bytes()].concat();
        for v in [self.generation, self.frontier, self.levels.len() as u64] {
            put_varint(&mut out, v);
        }
        self.levels.iter().for_each(|l| put_runs(&mut out, l));
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

    pub(super) fn stage(
        &self,
        root: &Path,
        key: &[u8; 32],
        space: Option<&MergeSpace>,
    ) -> Result<PathBuf> {
        let mut unbounded = Admission {
            maximum: u64::MAX,
            used: 0,
        };
        check_shape_with(self, &mut unbounded)?;
        let payload = self.encode();
        if self.levels.len() > MAX_LEVELS
            || self.lists().any(|l| l.len() > MAX_LEVEL_RUNS)
            || payload.len() > MAX_CURRENT_BYTES - MIN_ENCODED_LEN
        {
            return Err(Error::WriteCapacity(
                "disk run home: CURRENT would exceed its open limits; merge first".into(),
            ));
        }
        let frame = encode_frame(key, Uuid::new_v4(), 0, &payload)?;
        if let Some(s) = space {
            s.admit_current(frame.len() as u64)?;
        }
        let tmp = root.join(format!(".{CURRENT}.{}.tmp", self.generation));
        write_private_file(&tmp, &frame).map(|()| tmp)
    }

    pub(super) fn replace(root: &Path, staged: &Path) -> Result<()> {
        fs::rename(staged, root.join(CURRENT))?;
        sync_directory(root)
    }

    pub(super) fn decode_bounded(
        bytes: &[u8],
        cap: usize,
        runs: &Path,
        maximum: u64,
    ) -> Result<Self> {
        let mut budget = Admission { maximum, used: 0 };
        budget.add((cap + size_of::<Self>() + ARC_HEADER) as u64)?;
        if bytes.get(..8) != Some(MAGIC.as_slice()) || bytes.len() < 24 {
            return Err(corrupt("bad CURRENT"));
        }
        let home_id = Uuid::from_slice(&bytes[8..24]).map_err(|_| corrupt("bad CURRENT"))?;
        let mut at = 24;
        let (generation, frontier) = (get_varint(bytes, &mut at)?, get_varint(bytes, &mut at)?);
        let count = get_len(bytes, &mut at, MAX_LEVELS)?;
        let mut levels = budget.array(count)?;
        for _ in 0..count {
            levels.push(run_list(bytes, &mut at, runs, &mut budget)?);
        }
        let merging = run_list(bytes, &mut at, runs, &mut budget)?;
        let merge_cut = get_len(bytes, &mut at, MAX_RUN_KEY_BYTES + 1)?
            .checked_sub(1)
            .map(|len| budget.copy(get_slice(bytes, &mut at, len)?))
            .transpose()?;
        let base = run_list(bytes, &mut at, runs, &mut budget)?;
        if at != bytes.len() || generation == 0 {
            return Err(corrupt("bad CURRENT"));
        }
        let manifest = Self {
            home_id,
            generation,
            frontier,
            levels,
            merging,
            merge_cut,
            base,
        };
        check_shape_with(&manifest, &mut budget)?;
        Ok(manifest)
    }

    pub(super) fn retained_bytes(&self) -> u64 {
        let base = size_of::<Self>()
            + ARC_HEADER
            + self.levels.capacity() * size_of::<Vec<Arc<RunFile>>>()
            + self.merge_cut.as_ref().map_or(0, Vec::capacity);
        let run_mem = |r: &Arc<RunFile>| {
            size_of::<RunFile>()
                + ARC_HEADER
                + r.path.capacity()
                + r.meta.first_key.capacity()
                + r.meta.last_key.capacity()
        };
        self.lists().fold(base as u64, |acc, list| {
            let mem = list.capacity() * size_of::<Arc<RunFile>>()
                + list.iter().map(run_mem).sum::<usize>();
            acc.saturating_add(mem as u64)
        })
    }
}

fn put_runs(out: &mut Vec<u8>, runs: &[Arc<RunFile>]) {
    put_varint(out, runs.len() as u64);
    for r in runs {
        let m = &r.meta;
        out.extend_from_slice(m.run_id.as_bytes());
        for v in [
            m.records,
            m.tombstones,
            m.record_bytes,
            m.tombstone_bytes,
            m.file_bytes,
            r.hidden.load(Ordering::Acquire),
        ] {
            put_varint(out, v);
        }
        for key in [&m.first_key, &m.last_key] {
            put_varint(out, key.len() as u64);
            out.extend_from_slice(key);
        }
    }
}

fn run_list(
    bytes: &[u8],
    at: &mut usize,
    dir: &Path,
    budget: &mut Admission,
) -> Result<Vec<Arc<RunFile>>> {
    let count = get_len(bytes, at, MAX_LEVEL_RUNS)?;
    let mut runs = budget.array(count)?;
    let mut read_key = |at: &mut usize, adm: &mut Admission| -> Result<Vec<u8>> {
        adm.copy(get_slice(
            bytes,
            at,
            get_len(bytes, at, MAX_RUN_KEY_BYTES)?,
        )?)
    };
    for _ in 0..count {
        let run_id = Uuid::from_slice(get_slice(bytes, at, 16)?)
            .map_err(|_| corrupt("bad run entry in CURRENT"))?;
        let mut fields = [0u64; 6];
        for slot in &mut fields {
            *slot = get_varint(bytes, at)?;
        }
        let [records, tombstones, record_bytes, tombstone_bytes, file_bytes, hidden] = fields;
        let (first_key, last_key) = (read_key(at, budget)?, read_key(at, budget)?);
        if run_id.is_nil()
            || records == 0
            || tombstones > records
            || tombstone_bytes > record_bytes
            || first_key > last_key
        {
            return Err(corrupt("bad run entry in CURRENT"));
        }
        budget.add((size_of::<RunFile>() + ARC_HEADER) as u64)?;
        runs.push(Arc::new(RunFile {
            path: budget.path(dir, run_id)?,
            meta: RunMeta {
                run_id,
                records,
                tombstones,
                record_bytes,
                tombstone_bytes,
                file_bytes,
                first_key,
                last_key,
            },
            obsolete: AtomicBool::new(false),
            hidden: AtomicU64::new(hidden),
        }));
    }
    Ok(runs)
}

fn check_shape_with(manifest: &Manifest, budget: &mut Admission) -> Result<()> {
    let mut ids = budget.array(manifest.newest_first().count())?;
    ids.extend(manifest.newest_first().map(|r| r.meta.run_id));
    ids.sort_unstable();
    if ids.windows(2).any(|p| p[0] == p[1]) {
        return Err(corrupt("CURRENT names a run twice"));
    }
    if manifest.merge_cut.is_some() && manifest.merging.is_empty() {
        return Err(corrupt("CURRENT has a base merge cut but no merging runs"));
    }
    if !manifest
        .base
        .windows(2)
        .all(|p| p[0].meta.last_key < p[1].meta.first_key)
        || manifest.base.iter().any(|r| r.meta.tombstones > 0)
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
    fn add(&mut self, bytes: u64) -> Result<()> {
        self.used = self
            .used
            .checked_add(bytes)
            .filter(|&n| n <= self.maximum)
            .ok_or(Error::ReadBudgetExceeded)?;
        Ok(())
    }

    fn array<T>(&mut self, count: usize) -> Result<Vec<T>> {
        let elem = size_of::<T>();
        self.add(count.checked_mul(elem).ok_or(Error::ReadBudgetExceeded)? as u64)?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(count)
            .map_err(|_| Error::ReadBudgetExceeded)?;
        self.add((values.capacity().saturating_sub(count) * elem) as u64)?;
        Ok(values)
    }

    fn copy(&mut self, bytes: &[u8]) -> Result<Vec<u8>> {
        let mut v = self.array(bytes.len())?;
        v.extend_from_slice(bytes);
        Ok(v)
    }

    fn path(&mut self, dir: &Path, id: Uuid) -> Result<PathBuf> {
        let name = format!("{id}{RUN_SUFFIX}");
        let sz = dir
            .as_os_str()
            .len()
            .checked_add(1 + name.len())
            .ok_or(Error::ReadBudgetExceeded)?;
        self.add(sz as u64)?;
        let mut p = PathBuf::new();
        p.try_reserve_exact(sz)
            .map_err(|_| Error::ReadBudgetExceeded)?;
        self.add(p.capacity().saturating_sub(sz) as u64)?;
        p.extend([dir, Path::new(&name)]);
        Ok(p)
    }
}
