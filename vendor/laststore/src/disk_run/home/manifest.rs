//! CURRENT: sealed manifest model, binary wire codec, file staging, and run-file lifecycle.

use super::{open_nofollow, write_private_file};
use crate::disk_run::codec::{corrupt, get_bytes, get_len, get_slice, get_varint, put_varint};
use crate::disk_run::{MergeSpace, RunMeta, MAX_RUN_KEY_BYTES, RUN_SUFFIX};
use crate::durability::sync_directory;
use crate::frame::{decode_frame, encode_frame, MIN_ENCODED_LEN};
use crate::{Error, Result};
use std::fs;
use std::io::Read;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use uuid::Uuid;

pub(super) const CURRENT: &str = "CURRENT";
/// Directory name of the immutable run files.
pub const RUNS: &str = "runs";
pub(super) const MAX_LEVELS: usize = 32;
const MAX_LEVEL_RUNS: usize = 4096;
const MAX_CURRENT_BYTES: usize = 64 << 20;
const ARC_HDR: usize = 2 * size_of::<usize>();
const MAGIC: &[u8; 8] = b"LDRCUR2\0";

#[derive(Debug)]
pub(super) struct RunFile {
    pub(super) meta: RunMeta,
    pub(super) path: PathBuf,
    retired: AtomicBool,
    hidden_plus_one: AtomicU64,
}

impl RunFile {
    pub(super) fn new(runs: &Path, meta: RunMeta) -> Arc<Self> {
        let path = runs.join(format!("{}{RUN_SUFFIX}", meta.id));
        Arc::new(Self {
            meta,
            path,
            retired: AtomicBool::new(false),
            hidden_plus_one: AtomicU64::new(0),
        })
    }

    pub(super) fn retire(&self) {
        self.retired.store(true, Ordering::Release);
    }

    pub(super) fn is_retired(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }

    pub(super) fn hidden(&self) -> Option<u64> {
        self.hidden_plus_one.load(Ordering::Acquire).checked_sub(1)
    }

    pub(super) fn set_hidden(&self, bytes: u64) {
        self.hidden_plus_one
            .store(bytes.saturating_add(1), Ordering::Release);
    }
}

impl Drop for RunFile {
    fn drop(&mut self) {
        if self.retired.load(Ordering::Acquire) {
            let _ = fs::remove_file(&self.path);
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

    pub(super) fn load(root: &Path, key: &[u8; 32], id: Uuid) -> Result<Self> {
        let mut file = open_nofollow(&root.join(CURRENT))?;
        let len = file.metadata()?.len();
        if !(MIN_ENCODED_LEN as u64..=MAX_CURRENT_BYTES as u64).contains(&len) {
            return Err(corrupt("CURRENT length"));
        }
        let mut frame = Vec::with_capacity(len as usize);
        file.read_to_end(&mut frame)?;
        let raw = decode_frame(key, &frame, MAX_CURRENT_BYTES, (None, None))?.2;
        let decoded = Self::decode(&raw, &root.join(RUNS))?;
        (decoded.home_id == id)
            .then_some(decoded)
            .ok_or_else(|| corrupt("CURRENT names another home"))
    }

    pub(super) fn stage(
        &self,
        root: &Path,
        key: &[u8; 32],
        space: Option<&MergeSpace>,
    ) -> Result<PathBuf> {
        check_shape(self)?;
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
        if let Some(sp) = space {
            sp.admit(frame.len() as u64, false)?;
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
            let meta = fs::symlink_metadata(&run.path)?;
            if !meta.is_file() || meta.len() != run.meta.file_bytes {
                return Err(Error::Corrupt(msg.into()));
            }
        }
        Ok(())
    }

    pub(super) fn retained_bytes(&self) -> u64 {
        let base = size_of::<Self>()
            + ARC_HDR
            + self.levels.capacity() * size_of::<Vec<Arc<RunFile>>>()
            + self.merge_cut.as_ref().map_or(0, Vec::capacity);
        let run_mem = |r: &Arc<RunFile>| {
            size_of::<RunFile>()
                + ARC_HDR
                + r.path.capacity()
                + r.meta.first_key.capacity()
                + r.meta.last_key.capacity()
        };
        let lists: usize = self
            .lists()
            .map(|l| {
                l.capacity() * size_of::<Arc<RunFile>>() + l.iter().map(run_mem).sum::<usize>()
            })
            .sum();
        (base + lists) as u64
    }

    fn encode(&self) -> Vec<u8> {
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

    fn decode(bytes: &[u8], runs: &Path) -> Result<Self> {
        if bytes.get(..8) != Some(MAGIC.as_slice()) || bytes.len() < 24 {
            return Err(corrupt("bad CURRENT"));
        }
        let home_id = Uuid::from_slice(&bytes[8..24]).map_err(|_| corrupt("bad CURRENT"))?;
        let mut at = 24;
        let (generation, frontier) = (get_varint(bytes, &mut at)?, get_varint(bytes, &mut at)?);
        let count = get_len(bytes, &mut at, MAX_LEVELS)?;
        let mut levels = Vec::with_capacity(count);
        for _ in 0..count {
            levels.push(run_list(bytes, &mut at, runs)?);
        }
        let merging = run_list(bytes, &mut at, runs)?;
        let merge_cut = get_len(bytes, &mut at, MAX_RUN_KEY_BYTES + 1)?
            .checked_sub(1)
            .map(|len| get_slice(bytes, &mut at, len).map(<[u8]>::to_vec))
            .transpose()?;
        let base = run_list(bytes, &mut at, runs)?;
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
        check_shape(&manifest).map(|()| manifest)
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

fn run_list(bytes: &[u8], at: &mut usize, dir: &Path) -> Result<Vec<Arc<RunFile>>> {
    let count = get_len(bytes, at, MAX_LEVEL_RUNS)?;
    let mut runs = Vec::with_capacity(count);
    for _ in 0..count {
        let id =
            Uuid::from_slice(get_slice(bytes, at, 16)?).map_err(|_| corrupt("bad run entry"))?;
        let records = get_varint(bytes, at)?;
        let tombstones = get_varint(bytes, at)?;
        let logical_bytes = get_varint(bytes, at)?;
        let tombstone_key_bytes = get_varint(bytes, at)?;
        let file_bytes = get_varint(bytes, at)?;
        let hidden = get_varint(bytes, at)?;
        let first_key = get_bytes(bytes, at, MAX_RUN_KEY_BYTES)?.to_vec();
        let last_key = get_bytes(bytes, at, MAX_RUN_KEY_BYTES)?.to_vec();
        if id.is_nil()
            || records == 0
            || tombstones > records
            || tombstone_key_bytes > logical_bytes
            || first_key > last_key
        {
            return Err(corrupt("bad run entry in CURRENT"));
        }
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
            path: dir.join(format!("{id}{RUN_SUFFIX}")),
            retired: AtomicBool::new(false),
            hidden_plus_one: AtomicU64::new(hidden),
        }));
    }
    Ok(runs)
}

fn check_shape(manifest: &Manifest) -> Result<()> {
    let mut ids: Vec<Uuid> = manifest.newest_first().map(|r| r.meta.id).collect();
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
