//! CURRENT: the sealed file naming readable runs and bounded manifest loaders.

use super::{open_nofollow, write_private_file};
use crate::disk_run::codec::{corrupt, get_bytes, get_len, get_slice, get_varint, put_varint};
use crate::disk_run::{MergeSpace, RunMeta, MAX_RUN_KEY_BYTES, RUN_SUFFIX};
use crate::durability::sync_directory;
use crate::frame::{decode_frame, encode_frame, VerifiedFrameReader, MIN_ENCODED_LEN};
use crate::{Error, Result};
use std::fs;
use std::io::Read;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use uuid::Uuid;
use zeroize::Zeroizing;

const MAGIC: &[u8; 8] = b"LDRCUR2\0";
/// File name of the sealed list of runs and the frontier.
pub const CURRENT: &str = "CURRENT";
/// Directory name of the immutable run files.
pub const RUNS: &str = "runs";
pub(super) const MAX_LEVELS: usize = 32;
const MAX_LEVEL_RUNS: usize = 4096;
const MAX_CURRENT_BYTES: usize = 64 << 20;
const ARC_HDR: usize = 2 * size_of::<usize>();

#[derive(Debug)]
pub(super) struct RunFile(
    pub(super) RunMeta,
    pub(super) PathBuf,
    AtomicBool,
    AtomicU64,
);

impl RunFile {
    pub(super) fn new(runs: &Path, meta: RunMeta) -> Arc<Self> {
        let path = runs.join(format!("{}{RUN_SUFFIX}", meta.0));
        Arc::new(Self(meta, path, AtomicBool::new(false), AtomicU64::new(0)))
    }
    pub(super) fn retire(&self) {
        self.2.store(true, Ordering::Release);
    }
    pub(super) fn hidden(&self) -> Option<u64> {
        self.3.load(Ordering::Acquire).checked_sub(1)
    }
    pub(super) fn set_hidden(&self, bytes: u64) {
        self.3.store(bytes.saturating_add(1), Ordering::Release);
    }
}

impl Drop for RunFile {
    fn drop(&mut self) {
        if self.2.load(Ordering::Acquire) {
            let _ = fs::remove_file(&self.1);
        }
    }
}

#[derive(Debug, Clone, Default)]
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
    fn lists(&self) -> impl Iterator<Item = &Vec<Arc<RunFile>>> {
        self.levels.iter().chain([&self.merging, &self.base])
    }
    pub(super) fn newest_first(&self) -> impl Iterator<Item = &Arc<RunFile>> {
        self.lists().flatten()
    }
    pub(super) fn tiered(&self) -> impl Iterator<Item = &Arc<RunFile>> {
        self.levels.iter().flatten()
    }

    pub(super) fn load(root: &Path, key: &[u8; 32], id: Uuid, max: Option<u64>) -> Result<Self> {
        let mut f = open_nofollow(&root.join(CURRENT))?;
        let len = f.metadata()?.len();
        if !(MIN_ENCODED_LEN as u64..=MAX_CURRENT_BYTES as u64).contains(&len) {
            return Err(corrupt("CURRENT length"));
        }
        let runs = root.join(RUNS);
        let decoded = if let Some(max) = max {
            let pol = &lastdb_model::storage_policy::BACKUP_RESOURCES;
            let max = max.min(pol.working_memory_bytes);
            if max < 2 * pol.stream_buffer_bytes + 16 {
                return Err(Error::ReadBudgetExceeded);
            }
            let mut r = VerifiedFrameReader::open(&f, 0, len, key)?;
            let n = usize::try_from(r.3 .0)
                .ok()
                .filter(|_| r.3 .0.saturating_add(r.3 .1) <= max)
                .ok_or(Error::ReadBudgetExceeded)?;
            let mut buf = Zeroizing::new(Vec::new());
            buf.try_reserve_exact(n)
                .map_err(|_| Error::ReadBudgetExceeded)?;
            if (buf.capacity() as u64).saturating_add(r.3 .1) > max {
                return Err(Error::ReadBudgetExceeded);
            }
            buf.resize(n, 0);
            r.read_exact(&mut buf)?;
            if r.read(&mut [0])? != 0 {
                return Err(corrupt("CURRENT length"));
            }
            drop(r);
            Self::decode_bounded(&buf, buf.capacity(), &runs, max)?
        } else {
            let mut frame = Vec::with_capacity(len as usize);
            f.read_to_end(&mut frame)?;
            let raw = decode_frame(key, &frame, MAX_CURRENT_BYTES, (None, None))?.2;
            Self::decode_bounded(&raw, 0, &runs, u64::MAX)?
        };
        (decoded.home_id == id)
            .then_some(decoded)
            .ok_or_else(|| corrupt("CURRENT names another home"))
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut out = [MAGIC.as_slice(), self.home_id.as_bytes()].concat();
        [self.generation, self.frontier, self.levels.len() as u64]
            .into_iter()
            .for_each(|v| put_varint(&mut out, v));
        self.levels.iter().for_each(|l| put_runs(&mut out, l));
        put_runs(&mut out, &self.merging);
        put_varint(
            &mut out,
            self.merge_cut.as_ref().map_or(0, |c| c.len() as u64 + 1),
        );
        if let Some(cut) = &self.merge_cut {
            out.extend_from_slice(cut);
        }
        put_runs(&mut out, &self.base);
        out
    }

    pub(super) fn stage(
        &self,
        root: &Path,
        key: &[u8; 32],
        sp: Option<&MergeSpace>,
    ) -> Result<PathBuf> {
        check_shape(self, &mut Admission(u64::MAX, 0))?;
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
        if let Some(s) = sp {
            s.admit(frame.len() as u64, false)?;
        }
        let tmp = root.join(format!(".{CURRENT}.{}.tmp", self.generation));
        write_private_file(&tmp, &frame).map(|()| tmp)
    }

    pub(super) fn replace(root: &Path, staged: &Path) -> Result<()> {
        fs::rename(staged, root.join(CURRENT))?;
        sync_directory(root)
    }

    pub(super) fn verify_files(&self, msg: &'static str) -> Result<()> {
        for run in self.newest_first() {
            let m = fs::symlink_metadata(&run.1)?;
            if !m.is_file() || m.len() != run.0 .1[4] {
                return Err(Error::Corrupt(msg.into()));
            }
        }
        Ok(())
    }

    fn decode_bounded(bytes: &[u8], cap: usize, runs: &Path, maximum: u64) -> Result<Self> {
        let mut b = Admission(maximum, 0);
        b.add((cap + size_of::<Self>() + ARC_HDR) as u64)?;
        if bytes.get(..8) != Some(MAGIC.as_slice()) || bytes.len() < 24 {
            return Err(corrupt("bad CURRENT"));
        }
        let home_id = Uuid::from_slice(&bytes[8..24]).map_err(|_| corrupt("bad CURRENT"))?;
        let mut at = 24;
        let (generation, frontier) = (get_varint(bytes, &mut at)?, get_varint(bytes, &mut at)?);
        let count = get_len(bytes, &mut at, MAX_LEVELS)?;
        let mut levels = b.array(count)?;
        for _ in 0..count {
            levels.push(run_list(bytes, &mut at, runs, &mut b)?);
        }
        let merging = run_list(bytes, &mut at, runs, &mut b)?;
        let merge_cut = get_len(bytes, &mut at, MAX_RUN_KEY_BYTES + 1)?
            .checked_sub(1)
            .map(|len| b.copy(get_slice(bytes, &mut at, len)?))
            .transpose()?;
        let base = run_list(bytes, &mut at, runs, &mut b)?;
        if at != bytes.len() || generation == 0 {
            return Err(corrupt("bad CURRENT"));
        }
        let m = Self {
            home_id,
            generation,
            frontier,
            levels,
            merging,
            merge_cut,
            base,
        };
        check_shape(&m, &mut b).map(|()| m)
    }

    pub(super) fn retained_bytes(&self) -> u64 {
        let base = size_of::<Self>()
            + ARC_HDR
            + self.levels.capacity() * size_of::<Vec<Arc<RunFile>>>()
            + self.merge_cut.as_ref().map_or(0, Vec::capacity);
        let rmem = |r: &Arc<RunFile>| {
            size_of::<RunFile>() + ARC_HDR + r.1.capacity() + r.0 .2.capacity() + r.0 .3.capacity()
        };
        let lists: usize = self
            .lists()
            .map(|l| l.capacity() * size_of::<Arc<RunFile>>() + l.iter().map(rmem).sum::<usize>())
            .sum();
        (base + lists) as u64
    }
}

fn put_runs(out: &mut Vec<u8>, runs: &[Arc<RunFile>]) {
    put_varint(out, runs.len() as u64);
    for r in runs {
        let m = &r.0;
        out.extend_from_slice(m.0.as_bytes());
        m.1.into_iter().for_each(|v| put_varint(out, v));
        put_varint(out, r.3.load(Ordering::Acquire));
        for k in [&m.2, &m.3] {
            put_varint(out, k.len() as u64);
            out.extend_from_slice(k);
        }
    }
}

fn run_list(
    b: &[u8],
    at: &mut usize,
    dir: &Path,
    adm: &mut Admission,
) -> Result<Vec<Arc<RunFile>>> {
    let count = get_len(b, at, MAX_LEVEL_RUNS)?;
    let mut runs = adm.array(count)?;
    for _ in 0..count {
        let run_id =
            Uuid::from_slice(get_slice(b, at, 16)?).map_err(|_| corrupt("bad run entry"))?;
        let mut c = [0u64; 5];
        for slot in &mut c {
            *slot = get_varint(b, at)?;
        }
        let hidden = get_varint(b, at)?;
        let first = adm.copy(get_bytes(b, at, MAX_RUN_KEY_BYTES)?)?;
        let last = adm.copy(get_bytes(b, at, MAX_RUN_KEY_BYTES)?)?;
        if run_id.is_nil() || c[0] == 0 || c[1] > c[0] || c[3] > c[2] || first > last {
            return Err(corrupt("bad run entry in CURRENT"));
        }
        adm.add((size_of::<RunFile>() + ARC_HDR) as u64)?;
        let path = adm.path(dir, run_id)?;
        runs.push(Arc::new(RunFile(
            RunMeta(run_id, c, first, last),
            path,
            AtomicBool::new(false),
            AtomicU64::new(hidden),
        )));
    }
    Ok(runs)
}

fn check_shape(m: &Manifest, b: &mut Admission) -> Result<()> {
    let mut ids = b.array(m.newest_first().count())?;
    ids.extend(m.newest_first().map(|r| r.0 .0));
    ids.sort_unstable();
    if ids.windows(2).any(|p| p[0] == p[1]) {
        return Err(corrupt("CURRENT names a run twice"));
    }
    if m.merge_cut.is_some() && m.merging.is_empty() {
        return Err(corrupt("CURRENT has a base merge cut but no merging runs"));
    }
    (m.base.windows(2).all(|p| p[0].0 .3 < p[1].0 .2) && m.base.iter().all(|r| r.0 .1[1] == 0))
        .then_some(())
        .ok_or_else(|| corrupt("CURRENT has an unordered base"))
}

struct Admission(u64, u64);

impl Admission {
    fn add(&mut self, bytes: u64) -> Result<()> {
        self.1 = self
            .1
            .checked_add(bytes)
            .filter(|&n| n <= self.0)
            .ok_or(Error::ReadBudgetExceeded)?;
        Ok(())
    }

    fn array<T>(&mut self, count: usize) -> Result<Vec<T>> {
        let elem = size_of::<T>();
        self.add(count.checked_mul(elem).ok_or(Error::ReadBudgetExceeded)? as u64)?;
        let mut v = Vec::new();
        v.try_reserve_exact(count)
            .map_err(|_| Error::ReadBudgetExceeded)?;
        self.add((v.capacity().saturating_sub(count) * elem) as u64)
            .map(|()| v)
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
