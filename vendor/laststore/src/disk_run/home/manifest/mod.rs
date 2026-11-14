//! CURRENT: sealed manifest model, file staging, and run-file lifecycle.

mod codec;

use super::{open_nofollow, write_private_file};
use crate::disk_run::codec::corrupt;
use crate::disk_run::{MergeSpace, RunMeta, RUN_SUFFIX};
use crate::durability::sync_directory;
use crate::frame::{decode_frame, encode_frame, VerifiedFrameReader, MIN_ENCODED_LEN};
use crate::{Error, Result};
use codec::{check_shape, Admission};
use std::fs;
use std::io::Read;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use uuid::Uuid;
use zeroize::Zeroizing;

/// File name of the sealed list of runs and the frontier.
pub const CURRENT: &str = "CURRENT";
/// Directory name of the immutable run files.
pub const RUNS: &str = "runs";
pub(in crate::disk_run::home) const MAX_LEVELS: usize = 32;
const MAX_LEVEL_RUNS: usize = 4096;
const MAX_CURRENT_BYTES: usize = 64 << 20;
const ARC_HDR: usize = 2 * size_of::<usize>();

#[derive(Debug)]
pub(in crate::disk_run::home) struct RunFile {
    pub(in crate::disk_run::home) meta: RunMeta,
    pub(in crate::disk_run::home) path: PathBuf,
    retired: AtomicBool,
    hidden_plus_one: AtomicU64,
}

impl RunFile {
    pub(in crate::disk_run::home) fn new(runs: &Path, meta: RunMeta) -> Arc<Self> {
        let path = runs.join(format!("{}{RUN_SUFFIX}", meta.id));
        Arc::new(Self {
            meta,
            path,
            retired: AtomicBool::new(false),
            hidden_plus_one: AtomicU64::new(0),
        })
    }

    pub(in crate::disk_run::home) fn retire(&self) {
        self.retired.store(true, Ordering::Release);
    }

    pub(in crate::disk_run::home) fn hidden(&self) -> Option<u64> {
        self.hidden_plus_one.load(Ordering::Acquire).checked_sub(1)
    }

    pub(in crate::disk_run::home) fn set_hidden(&self, bytes: u64) {
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
pub(in crate::disk_run::home) struct Manifest {
    pub(in crate::disk_run::home) home_id: Uuid,
    pub(in crate::disk_run::home) generation: u64,
    pub(in crate::disk_run::home) frontier: u64,
    pub(in crate::disk_run::home) levels: Vec<Vec<Arc<RunFile>>>,
    pub(in crate::disk_run::home) merging: Vec<Arc<RunFile>>,
    pub(in crate::disk_run::home) merge_cut: Option<Vec<u8>>,
    pub(in crate::disk_run::home) base: Vec<Arc<RunFile>>,
}

impl Manifest {
    fn lists(&self) -> impl Iterator<Item = &Vec<Arc<RunFile>>> {
        self.levels.iter().chain([&self.merging, &self.base])
    }

    pub(in crate::disk_run::home) fn newest_first(&self) -> impl Iterator<Item = &Arc<RunFile>> {
        self.lists().flatten()
    }

    pub(in crate::disk_run::home) fn tiered(&self) -> impl Iterator<Item = &Arc<RunFile>> {
        self.levels.iter().flatten()
    }

    pub(in crate::disk_run::home) fn load(
        root: &Path,
        key: &[u8; 32],
        id: Uuid,
        max: Option<u64>,
    ) -> Result<Self> {
        let mut file = open_nofollow(&root.join(CURRENT))?;
        let len = file.metadata()?.len();
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
            let mut reader = VerifiedFrameReader::open(&file, 0, len, key)?;
            let plain_len = usize::try_from(reader.plain_len)
                .ok()
                .filter(|_| {
                    reader
                        .plain_len
                        .saturating_add(reader.reserved_memory_bytes)
                        <= max
                })
                .ok_or(Error::ReadBudgetExceeded)?;
            let mut buf = Zeroizing::new(Vec::new());
            buf.try_reserve_exact(plain_len)
                .map_err(|_| Error::ReadBudgetExceeded)?;
            if (buf.capacity() as u64).saturating_add(reader.reserved_memory_bytes) > max {
                return Err(Error::ReadBudgetExceeded);
            }
            buf.resize(plain_len, 0);
            reader.read_exact(&mut buf)?;
            if reader.read(&mut [0])? != 0 {
                return Err(corrupt("CURRENT length"));
            }
            drop(reader);
            Self::decode_bounded(&buf, buf.capacity(), &runs, max)?
        } else {
            let mut frame = Vec::with_capacity(len as usize);
            file.read_to_end(&mut frame)?;
            let raw = decode_frame(key, &frame, MAX_CURRENT_BYTES, (None, None))?.2;
            Self::decode_bounded(&raw, 0, &runs, u64::MAX)?
        };
        (decoded.home_id == id)
            .then_some(decoded)
            .ok_or_else(|| corrupt("CURRENT names another home"))
    }

    pub(in crate::disk_run::home) fn stage(
        &self,
        root: &Path,
        key: &[u8; 32],
        space: Option<&MergeSpace>,
    ) -> Result<PathBuf> {
        check_shape(self, &mut Admission::new(u64::MAX))?;
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

    pub(in crate::disk_run::home) fn replace(root: &Path, staged: &Path) -> Result<()> {
        fs::rename(staged, root.join(CURRENT))?;
        sync_directory(root)
    }

    pub(in crate::disk_run::home) fn verify_files(&self, msg: &'static str) -> Result<()> {
        for run in self.newest_first() {
            let meta = fs::symlink_metadata(&run.path)?;
            if !meta.is_file() || meta.len() != run.meta.file_bytes {
                return Err(Error::Corrupt(msg.into()));
            }
        }
        Ok(())
    }

    pub(in crate::disk_run::home) fn retained_bytes(&self) -> u64 {
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
}
