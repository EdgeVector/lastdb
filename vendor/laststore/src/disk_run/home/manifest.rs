//! CURRENT: the one sealed file that names the readable runs.
//!
//! CURRENT is one [`crate::frame`] frame under the home key. Each publish
//! uses a fresh random frame id, so a retried generation after a crash never
//! reuses a nonce. The payload names the home, the generation, the frontier
//! (complete commits), the runs of each tiered level (newest first), the runs
//! of a base merge in progress with its cut, and the runs of the base. A
//! CURRENT from before the base (`LDRCUR1`) still opens: all of its runs are
//! tiered, and the next merges build the base.

use super::super::codec::{corrupt, get_len, get_slice, get_varint, put_varint};
use super::super::{MAX_RUN_KEY_BYTES, RUN_SUFFIX};
use crate::durability::{sync_directory, sync_dirty_file};
use crate::frame::{decode_frame, encode_frame, min_encoded_len, FrameHeader};
use crate::{Error, Result};
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use uuid::Uuid;

const MAGIC_V1: &[u8; 8] = b"LDRCUR1\0";
const MAGIC: &[u8; 8] = b"LDRCUR2\0";
pub(super) const CURRENT: &str = "CURRENT";
pub(super) const RUNS: &str = "runs";
/// Most levels one CURRENT may name.
pub(super) const MAX_LEVELS: usize = 32;
/// Most runs one level, the merging runs, or the base may hold. Merges keep
/// them far below this.
pub(super) const MAX_LEVEL_RUNS: usize = 4096;
/// Largest CURRENT file a reader accepts.
const MAX_CURRENT_BYTES: usize = 64 * 1024 * 1024;

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

    /// True when the run can hold a key in `[lower, upper)`.
    pub(super) fn overlaps(&self, lower: &[u8], upper: Option<&[u8]>) -> bool {
        let below_upper = match upper {
            Some(upper) => self.first_key.as_slice() < upper,
            None => true,
        };
        self.last_key.as_slice() >= lower && below_upper
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
    hidden: AtomicU64,
}

impl RunFile {
    pub(super) fn new(runs: &Path, meta: RunMeta) -> Arc<Self> {
        Self::counted(runs, meta, 0)
    }

    fn counted(runs: &Path, meta: RunMeta, hidden: u64) -> Arc<Self> {
        Arc::new(Self {
            path: runs.join(format!("{}{RUN_SUFFIX}", meta.run_id)),
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
            // A failed delete leaves an orphan; the next open removes it.
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
    /// Runs in one level can overlap.
    pub(super) levels: Vec<Vec<Arc<RunFile>>>,
    /// The runs that a base merge in progress moves into the base, newest
    /// first: older than every tiered run and newer than the base. Empty
    /// when no base merge runs.
    pub(super) merging: Vec<Arc<RunFile>>,
    /// The key through which the base holds the merging runs: base runs up
    /// to it are base merge outputs, later base runs are its inputs. `None`
    /// before the first step.
    pub(super) merge_cut: Option<Vec<u8>>,
    /// The base: the oldest data, below every other run. Its runs are in
    /// key order and do not overlap, and it holds no tombstone.
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

    /// Every run, newest first: the order a read must search. The base runs
    /// do not overlap, so their order inside the base does not matter.
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

    fn encode(&self) -> Vec<u8> {
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

    fn decode(bytes: &[u8], runs: &Path) -> Result<Self> {
        let with_base = match bytes.get(..8) {
            Some(magic) if magic == MAGIC.as_slice() => true,
            Some(magic) if magic == MAGIC_V1.as_slice() => false,
            _ => return Err(corrupt("bad CURRENT")),
        };
        if bytes.len() < 24 {
            return Err(corrupt("bad CURRENT"));
        }
        let home_id = Uuid::from_slice(&bytes[8..24]).expect("16 bytes");
        let mut at = 24;
        let generation = get_varint(bytes, &mut at)?;
        let frontier = get_varint(bytes, &mut at)?;
        let mut levels = Vec::new();
        for _ in 0..get_len(bytes, &mut at, MAX_LEVELS)? {
            levels.push(get_runs(bytes, &mut at, runs, with_base)?);
        }
        let (mut merging, mut merge_cut, mut base) = (Vec::new(), None, Vec::new());
        if with_base {
            merging = get_runs(bytes, &mut at, runs, true)?;
            let cut = get_len(bytes, &mut at, MAX_RUN_KEY_BYTES + 1)?;
            if let Some(len) = cut.checked_sub(1) {
                merge_cut = Some(get_slice(bytes, &mut at, len)?.to_vec());
            }
            base = get_runs(bytes, &mut at, runs, true)?;
        }
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
        manifest.check_shape()?;
        Ok(manifest)
    }

    /// The facts reads and merges rely on: each run named once, a cut only
    /// during a base merge, and base runs in key order with no overlap and
    /// no tombstone.
    fn check_shape(&self) -> Result<()> {
        let mut seen = HashSet::new();
        if !self.newest_first().all(|run| seen.insert(run.meta.run_id)) {
            return Err(corrupt("CURRENT names a run twice"));
        }
        if self.merge_cut.is_some() && self.merging.is_empty() {
            return Err(corrupt("CURRENT has a base merge cut but no merging runs"));
        }
        let ordered = self
            .base
            .windows(2)
            .all(|pair| pair[0].meta.last_key < pair[1].meta.first_key);
        if !ordered || self.base.iter().any(|run| run.meta.tombstones > 0) {
            return Err(corrupt("CURRENT has an unordered base"));
        }
        Ok(())
    }

    /// Seal this manifest into a synced temporary file in `root`.
    ///
    /// It refuses a manifest that [`Manifest::load`] would refuse, so a
    /// published CURRENT always opens. An error leaves the old CURRENT.
    pub(super) fn stage(&self, root: &Path, key: &[u8; 32]) -> Result<PathBuf> {
        self.check_shape()?;
        let payload = self.encode();
        if self.levels.len() > MAX_LEVELS
            || self.levels.iter().any(|level| level.len() > MAX_LEVEL_RUNS)
            || self.merging.len() > MAX_LEVEL_RUNS
            || self.base.len() > MAX_LEVEL_RUNS
            || payload.len() > MAX_CURRENT_BYTES - min_encoded_len()
        {
            return Err(Error::WriteCapacity(
                "disk run home: CURRENT would exceed its open limits; merge first".into(),
            ));
        }
        let header = FrameHeader {
            chunk_uuid: Uuid::new_v4(),
            shard: 0,
            start_csn: 0,
            counter: 0,
        };
        let frame = encode_frame(key, header, &payload)?;
        // Named by generation, not by the frame id: one owner stages each
        // generation once, and open removes any leftover.
        let tmp = root.join(format!(".{CURRENT}.{}.tmp", self.generation));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)?;
        let written = file
            .write_all(&frame)
            .map_err(Error::from)
            .and_then(|()| sync_dirty_file(&file));
        if let Err(error) = written {
            let _ = fs::remove_file(&tmp);
            return Err(error);
        }
        Ok(tmp)
    }

    /// Rename a staged CURRENT into place and sync the directory. After an
    /// error here the durable CURRENT is unknown; the caller must stop.
    pub(super) fn replace(root: &Path, staged: &Path) -> Result<()> {
        fs::rename(staged, root.join(CURRENT))?;
        sync_directory(&File::open(root)?)
    }

    /// Read and authenticate CURRENT, and check that it names `home_id`.
    pub(super) fn load(root: &Path, key: &[u8; 32], home_id: Uuid) -> Result<Self> {
        let mut file = File::open(root.join(CURRENT))?;
        let len = file.metadata()?.len();
        if len < min_encoded_len() as u64 || len > MAX_CURRENT_BYTES as u64 {
            return Err(corrupt("CURRENT length"));
        }
        let mut frame = Vec::with_capacity(len as usize);
        file.read_to_end(&mut frame)?;
        let decoded = decode_frame(key, &frame)?;
        let manifest = Self::decode(&decoded.payload, &root.join(RUNS))?;
        if manifest.home_id != home_id {
            return Err(corrupt("CURRENT names another home"));
        }
        Ok(manifest)
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

/// One list of runs. A CURRENT from before the base (`counts` false) holds
/// no tombstone, record byte, or hidden byte counts; the record bytes then
/// stand at the file size, and the hidden bytes are counted again.
fn get_runs(bytes: &[u8], at: &mut usize, dir: &Path, counts: bool) -> Result<Vec<Arc<RunFile>>> {
    let count = get_len(bytes, at, MAX_LEVEL_RUNS)?;
    let mut runs = Vec::with_capacity(count);
    for _ in 0..count {
        let id = get_slice(bytes, at, 16)?;
        let run_id = Uuid::from_slice(id).expect("16 bytes");
        let records = get_varint(bytes, at)?;
        let (tombstones, record_bytes, tombstone_bytes, file_bytes, hidden) = if counts {
            let mut values = [0u64; 5];
            for value in &mut values {
                *value = get_varint(bytes, at)?;
            }
            (values[0], values[1], values[2], values[3], values[4])
        } else {
            let file_bytes = get_varint(bytes, at)?;
            (0, file_bytes, 0, file_bytes, 0)
        };
        let first_len = get_len(bytes, at, MAX_RUN_KEY_BYTES)?;
        let first_key = get_slice(bytes, at, first_len)?.to_vec();
        let last_len = get_len(bytes, at, MAX_RUN_KEY_BYTES)?;
        let last_key = get_slice(bytes, at, last_len)?.to_vec();
        if run_id.is_nil()
            || records == 0
            || tombstones > records
            || tombstone_bytes > record_bytes
            || first_key > last_key
        {
            return Err(corrupt("bad run entry in CURRENT"));
        }
        let meta = RunMeta {
            run_id,
            records,
            tombstones,
            record_bytes,
            tombstone_bytes,
            file_bytes,
            first_key,
            last_key,
        };
        runs.push(RunFile::counted(dir, meta, hidden));
    }
    Ok(runs)
}
