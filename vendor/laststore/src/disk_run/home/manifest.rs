//! CURRENT: the one sealed file that names the readable runs.
//!
//! CURRENT is one [`crate::frame`] frame under the home key. Each publish
//! uses a fresh random frame id, so a retried generation after a crash never
//! reuses a nonce. The payload names the home, the generation, the frontier
//! (complete commits), and the runs of each level, newest first.

use super::super::codec::{corrupt, get_len, get_slice, get_varint, put_varint};
use super::super::{MAX_RUN_KEY_BYTES, RUN_SUFFIX};
use crate::durability::{sync_directory, sync_dirty_file};
use crate::frame::{decode_frame, encode_frame, min_encoded_len, FrameHeader};
use crate::{Error, Result};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use uuid::Uuid;

const MAGIC: &[u8; 8] = b"LDRCUR1\0";
pub(super) const CURRENT: &str = "CURRENT";
pub(super) const RUNS: &str = "runs";
/// Most levels one CURRENT may name.
pub(super) const MAX_LEVELS: usize = 32;
/// Most runs one level may hold. Merges keep levels far below this.
const MAX_LEVEL_RUNS: usize = 4096;
/// Largest CURRENT file a reader accepts.
const MAX_CURRENT_BYTES: usize = 64 * 1024 * 1024;

/// What CURRENT records about one run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RunMeta {
    pub(super) run_id: Uuid,
    pub(super) records: u64,
    pub(super) file_bytes: u64,
    pub(super) first_key: Vec<u8>,
    pub(super) last_key: Vec<u8>,
}

impl RunMeta {
    /// True when `key` can be in this run.
    pub(super) fn covers(&self, key: &[u8]) -> bool {
        self.first_key.as_slice() <= key && key <= self.last_key.as_slice()
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
}

impl RunFile {
    pub(super) fn new(runs: &Path, meta: RunMeta) -> Arc<Self> {
        Arc::new(Self {
            path: runs.join(format!("{}{RUN_SUFFIX}", meta.run_id)),
            meta,
            obsolete: AtomicBool::new(false),
        })
    }

    /// Delete the file when the last holder drops it.
    pub(super) fn retire(&self) {
        self.obsolete.store(true, Ordering::Release);
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
    /// Level 0 first; inside a level, the newest run first.
    pub(super) levels: Vec<Vec<Arc<RunFile>>>,
}

impl Manifest {
    pub(super) fn empty(home_id: Uuid) -> Self {
        Self {
            home_id,
            generation: 1,
            frontier: 0,
            levels: Vec::new(),
        }
    }

    /// Every run, newest first: the order a read must search.
    pub(super) fn newest_first(&self) -> impl Iterator<Item = &Arc<RunFile>> {
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
            put_varint(&mut out, level.len() as u64);
            for run in level {
                let meta = &run.meta;
                out.extend_from_slice(meta.run_id.as_bytes());
                put_varint(&mut out, meta.records);
                put_varint(&mut out, meta.file_bytes);
                for key in [&meta.first_key, &meta.last_key] {
                    put_varint(&mut out, key.len() as u64);
                    out.extend_from_slice(key);
                }
            }
        }
        out
    }

    fn decode(bytes: &[u8], runs: &Path) -> Result<Self> {
        if bytes.len() < 24 || &bytes[..8] != MAGIC {
            return Err(corrupt("bad CURRENT"));
        }
        let home_id = Uuid::from_slice(&bytes[8..24]).expect("16 bytes");
        let mut at = 24;
        let generation = get_varint(bytes, &mut at)?;
        let frontier = get_varint(bytes, &mut at)?;
        let mut levels = Vec::new();
        for _ in 0..get_len(bytes, &mut at, MAX_LEVELS)? {
            let count = get_len(bytes, &mut at, MAX_LEVEL_RUNS)?;
            let mut level = Vec::with_capacity(count);
            for _ in 0..count {
                let id = get_slice(bytes, &mut at, 16)?;
                let run_id = Uuid::from_slice(id).expect("16 bytes");
                let records = get_varint(bytes, &mut at)?;
                let file_bytes = get_varint(bytes, &mut at)?;
                let first_len = get_len(bytes, &mut at, MAX_RUN_KEY_BYTES)?;
                let first_key = get_slice(bytes, &mut at, first_len)?.to_vec();
                let last_len = get_len(bytes, &mut at, MAX_RUN_KEY_BYTES)?;
                let last_key = get_slice(bytes, &mut at, last_len)?.to_vec();
                if run_id.is_nil() || records == 0 || first_key > last_key {
                    return Err(corrupt("bad run entry in CURRENT"));
                }
                level.push(RunFile::new(
                    runs,
                    RunMeta {
                        run_id,
                        records,
                        file_bytes,
                        first_key,
                        last_key,
                    },
                ));
            }
            levels.push(level);
        }
        if at != bytes.len() || generation == 0 {
            return Err(corrupt("bad CURRENT"));
        }
        Ok(Self {
            home_id,
            generation,
            frontier,
            levels,
        })
    }

    /// Write this manifest as CURRENT: a temporary file, a file sync, a
    /// rename, then a directory sync.
    ///
    /// An error before the rename leaves the old CURRENT. An error after it
    /// leaves the new state unknown; the caller must stop writing.
    pub(super) fn publish(&self, root: &Path, key: &[u8; 32]) -> Result<()> {
        let header = FrameHeader {
            chunk_uuid: Uuid::new_v4(),
            shard: 0,
            start_csn: 0,
            counter: 0,
        };
        let frame = encode_frame(key, header, &self.encode())?;
        let tmp = root.join(format!(".{CURRENT}.{}.tmp", header.chunk_uuid));
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
        fs::rename(&tmp, root.join(CURRENT))?;
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
