//! Binary wire encoding, bounded decoding, and shape checks for [`Manifest`].

use super::{Manifest, RunFile, ARC_HDR, MAX_LEVELS, MAX_LEVEL_RUNS};
use crate::disk_run::codec::{corrupt, get_bytes, get_len, get_slice, get_varint, put_varint};
use crate::disk_run::{RunMeta, MAX_RUN_KEY_BYTES, RUN_SUFFIX};
use crate::{Error, Result};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use uuid::Uuid;

const MAGIC: &[u8; 8] = b"LDRCUR2\0";

impl Manifest {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut out = [MAGIC.as_slice(), self.home_id.as_bytes()].concat();
        for val in [self.generation, self.frontier, self.levels.len() as u64] {
            put_varint(&mut out, val);
        }
        for level in &self.levels {
            put_runs(&mut out, level);
        }
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

    pub(super) fn decode_bounded(
        bytes: &[u8],
        cap: usize,
        runs: &Path,
        maximum: u64,
    ) -> Result<Self> {
        let mut adm = Admission::new(maximum);
        adm.add((cap + size_of::<Self>() + ARC_HDR) as u64)?;
        if bytes.get(..8) != Some(MAGIC.as_slice()) || bytes.len() < 24 {
            return Err(corrupt("bad CURRENT"));
        }
        let home_id = Uuid::from_slice(&bytes[8..24]).map_err(|_| corrupt("bad CURRENT"))?;
        let mut at = 24;
        let (generation, frontier) = (get_varint(bytes, &mut at)?, get_varint(bytes, &mut at)?);
        let count = get_len(bytes, &mut at, MAX_LEVELS)?;
        let mut levels = adm.array(count)?;
        for _ in 0..count {
            levels.push(run_list(bytes, &mut at, runs, &mut adm)?);
        }
        let merging = run_list(bytes, &mut at, runs, &mut adm)?;
        let merge_cut = get_len(bytes, &mut at, MAX_RUN_KEY_BYTES + 1)?
            .checked_sub(1)
            .map(|len| adm.copy(get_slice(bytes, &mut at, len)?))
            .transpose()?;
        let base = run_list(bytes, &mut at, runs, &mut adm)?;
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
        check_shape(&manifest, &mut adm).map(|()| manifest)
    }
}

fn put_runs(out: &mut Vec<u8>, runs: &[Arc<RunFile>]) {
    put_varint(out, runs.len() as u64);
    for run in runs {
        let meta = &run.meta;
        out.extend_from_slice(meta.id.as_bytes());
        for val in [
            meta.records,
            meta.tombstones,
            meta.logical_bytes,
            meta.tombstone_key_bytes,
            meta.file_bytes,
        ] {
            put_varint(out, val);
        }
        put_varint(out, run.hidden_plus_one.load(Ordering::Acquire));
        for key in [&meta.first_key, &meta.last_key] {
            put_varint(out, key.len() as u64);
            out.extend_from_slice(key);
        }
    }
}

fn run_list(
    bytes: &[u8],
    at: &mut usize,
    dir: &Path,
    adm: &mut Admission,
) -> Result<Vec<Arc<RunFile>>> {
    let count = get_len(bytes, at, MAX_LEVEL_RUNS)?;
    let mut runs = adm.array(count)?;
    for _ in 0..count {
        let id =
            Uuid::from_slice(get_slice(bytes, at, 16)?).map_err(|_| corrupt("bad run entry"))?;
        let records = get_varint(bytes, at)?;
        let tombstones = get_varint(bytes, at)?;
        let logical_bytes = get_varint(bytes, at)?;
        let tombstone_key_bytes = get_varint(bytes, at)?;
        let file_bytes = get_varint(bytes, at)?;
        let hidden = get_varint(bytes, at)?;
        let first_key = adm.copy(get_bytes(bytes, at, MAX_RUN_KEY_BYTES)?)?;
        let last_key = adm.copy(get_bytes(bytes, at, MAX_RUN_KEY_BYTES)?)?;
        if id.is_nil()
            || records == 0
            || tombstones > records
            || tombstone_key_bytes > logical_bytes
            || first_key > last_key
        {
            return Err(corrupt("bad run entry in CURRENT"));
        }
        adm.add((size_of::<RunFile>() + ARC_HDR) as u64)?;
        let path = adm.path(dir, id)?;
        runs.push(Arc::new(RunFile {
            meta: RunMeta {
                id,
                records,
                tombstones,
                logical_bytes,
                tombstone_key_bytes,
                file_bytes,
                first_key,
                last_key,
            },
            path,
            retired: AtomicBool::new(false),
            hidden_plus_one: AtomicU64::new(hidden),
        }));
    }
    Ok(runs)
}

pub(super) fn check_shape(manifest: &Manifest, adm: &mut Admission) -> Result<()> {
    let mut ids = adm.array(manifest.newest_first().count())?;
    ids.extend(manifest.newest_first().map(|r| r.meta.id));
    ids.sort_unstable();
    if ids.windows(2).any(|p| p[0] == p[1]) {
        return Err(corrupt("CURRENT names a run twice"));
    }
    if manifest.merge_cut.is_some() && manifest.merging.is_empty() {
        return Err(corrupt("CURRENT has a base merge cut but no merging runs"));
    }
    let ordered = manifest
        .base
        .windows(2)
        .all(|p| p[0].meta.last_key < p[1].meta.first_key)
        && manifest.base.iter().all(|r| r.meta.tombstones == 0);
    ordered
        .then_some(())
        .ok_or_else(|| corrupt("CURRENT has an unordered base"))
}

pub(super) struct Admission {
    limit: u64,
    used: u64,
}

impl Admission {
    pub(super) fn new(limit: u64) -> Self {
        Self { limit, used: 0 }
    }

    pub(super) fn add(&mut self, bytes: u64) -> Result<()> {
        self.used = self
            .used
            .checked_add(bytes)
            .filter(|&n| n <= self.limit)
            .ok_or(Error::ReadBudgetExceeded)?;
        Ok(())
    }

    pub(super) fn array<T>(&mut self, count: usize) -> Result<Vec<T>> {
        let elem = size_of::<T>();
        self.add(count.checked_mul(elem).ok_or(Error::ReadBudgetExceeded)? as u64)?;
        let mut out = Vec::new();
        out.try_reserve_exact(count)
            .map_err(|_| Error::ReadBudgetExceeded)?;
        self.add((out.capacity().saturating_sub(count) * elem) as u64)
            .map(|()| out)
    }

    pub(super) fn copy(&mut self, bytes: &[u8]) -> Result<Vec<u8>> {
        let mut out = self.array(bytes.len())?;
        out.extend_from_slice(bytes);
        Ok(out)
    }

    pub(super) fn path(&mut self, dir: &Path, id: Uuid) -> Result<PathBuf> {
        let name = format!("{id}{RUN_SUFFIX}");
        let sz = dir
            .as_os_str()
            .len()
            .checked_add(1 + name.len())
            .ok_or(Error::ReadBudgetExceeded)?;
        self.add(sz as u64)?;
        let mut path = PathBuf::new();
        path.try_reserve_exact(sz)
            .map_err(|_| Error::ReadBudgetExceeded)?;
        self.add(path.capacity().saturating_sub(sz) as u64)?;
        path.extend([dir, Path::new(&name)]);
        Ok(path)
    }
}
