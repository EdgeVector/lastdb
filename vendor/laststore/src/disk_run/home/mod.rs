//! A disk format v2 home: sorted run files and one CURRENT.
//!
//! ```text
//! <root>/laststore-run-home-v2   64 B clear descriptor: magic, home id, CRC
//! <root>/LOCK                    exclusive owner lock (flock)
//! <root>/CURRENT                 sealed list of runs + frontier
//! <root>/runs/<run id>.ldrun     immutable run files
//! <root>/NODE_CLEANUP            optional node-cleanup checkpoint
//! <root>/CAS_DELETE              optional cross-home deletion intent
//! <root>/mutation-log-v1/        the owner's mutation log
//! <root>/cloud-byte-v2/          the owner's backup source and run holds
//! ```
//!
//! A commit writes one level-0 run with all of its records, makes the run
//! durable, then publishes a new CURRENT by rename. A read takes one
//! CURRENT for its whole call and searches the runs newest first, so it
//! sees one complete commit boundary and never a part of a commit.

mod capture;
mod cleanup;
mod commit;
mod diagnostic;
mod diagnostic_inventory;
mod maintenance;
mod manifest;
mod manifest_bounded;
mod open;
mod plan;
mod publication;
mod range;
mod read;
mod stream;

pub use capture::{CapturedRunFile, CapturedRunHome, CapturedRunKind, SnapshotFile};
pub use cleanup::{RunCleanupFiles, MAX_CAS_DELETE_CHECKPOINT_BYTES};
pub use commit::{HomeMutation, RunCommit};
pub use diagnostic::{
    CollectionInventory, FileDiagnostics, LogicalInventory, RunDiagnosticCheckpoint,
    RunHomeDiagnostics, RunPartDiagnostics,
};
pub use manifest::{CURRENT, RUNS};
pub use range::{RangeBounds, RunHashRange, RunRangePage};
pub use read::{ReadBudget, RunRead, RunReadCosts, RunRoot};

use super::codec::corrupt;
use super::RunOptions;
use crate::space::sync_directory;
use crate::{Error, Result};
use manifest::{Manifest, RunFile};
use open::build_empty;
use std::fs::{self, File};
use std::io::Read;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use uuid::Uuid;

/// File name of the exclusive owner lock (flock).
pub const LOCK: &str = "LOCK";
/// File name of the authenticated node-cleanup checkpoint.
pub(super) const NODE_CLEANUP: &str = "NODE_CLEANUP";
/// File name of the authenticated cross-home deletion intent.
pub(super) const CAS_DELETE: &str = "CAS_DELETE";
/// Directory of the home owner's mutation log, beside `runs/`.
pub const MUTATION_LOG_DIR: &str = "mutation-log-v1";
/// Directory of the home owner's cloud backup: the capture source and the
/// run holds. A hold hard-links run files, so it stays beside `runs/`.
pub const BACKUP_DIR: &str = "cloud-byte-v2";
/// Directory of the home owner's shared snapshot holds beside `runs/`.
pub const SHARED_BACKUP_DIR: &str = "cloud-shared";
/// File name of the clear home descriptor.
pub const RUN_HOME_MARKER: &str = "laststore-run-home-v2";
/// `LDRHOME4`: every value is stored as given.
pub(super) const DESCRIPTOR_MAGIC: &[u8; 8] = b"LDRHOME4";
pub(super) const DESCRIPTOR_BYTES: usize = 64;

/// Largest value that a commit accepts.
pub const MAX_VALUE_BYTES: usize = super::MAX_RUN_VALUE_BYTES;

/// Level and size facts of the published CURRENT.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunHomeStatus {
    /// CURRENT generation.
    pub generation: u64,
    /// Complete commits.
    pub frontier: u64,
    /// For each tiered level: runs, records (tombstones included), file bytes.
    pub levels: Vec<(usize, u64, u64)>,
    /// The runs of a base merge in progress: runs, records, file bytes.
    pub merging: (usize, u64, u64),
    /// The base: runs, records, file bytes.
    pub base: (usize, u64, u64),
    /// Key and value bytes of every run before compression.
    pub record_bytes: u64,
    /// Older versions that newer tiered records hide, and the tiered
    /// tombstones, in record bytes; `None` while a new run is not counted.
    pub stale_bytes: Option<u64>,
}

impl RunHomeStatus {
    fn parts(&self) -> impl Iterator<Item = &(usize, u64, u64)> {
        self.levels.iter().chain([&self.merging, &self.base])
    }

    /// Runs that CURRENT names.
    pub fn runs(&self) -> usize {
        self.parts().map(|part| part.0).sum()
    }

    /// Records of every run, tombstones and hidden versions included.
    pub fn records(&self) -> u64 {
        self.parts().map(|part| part.1).sum()
    }

    /// File bytes of every run.
    pub fn file_bytes(&self) -> u64 {
        self.parts().map(|part| part.2).sum()
    }
}

fn totals(runs: &[Arc<RunFile>]) -> (usize, u64, u64) {
    (
        runs.len(),
        runs.iter().map(|run| run.meta.records).sum(),
        runs.iter().map(|run| run.meta.file_bytes).sum(),
    )
}

/// An open v2 home. One process owns it; reads and writes share it.
pub struct RunHome {
    root: PathBuf,
    runs: PathBuf,
    key: [u8; 32],
    identity: Uuid,
    options: RunOptions,
    current: RwLock<Arc<Manifest>>,
    commit: Mutex<()>,
    publish: Mutex<()>,
    merge: Mutex<()>,
    retired: Mutex<Vec<Weak<RunFile>>>,
    poisoned: AtomicBool,
    lock: Arc<File>,
}

impl RunHome {
    /// Create a new empty home in `root`, which must be absent or empty.
    pub fn create(root: &Path, key: &[u8; 32]) -> Result<Self> {
        if root.try_exists()? && fs::read_dir(root)?.next().is_some() {
            return Err(Error::Config(
                "disk run home: create needs an absent or empty directory".into(),
            ));
        }
        let name = root
            .file_name()
            .ok_or_else(|| Error::Config("disk run home: root has no name".into()))?;
        let parent = match root.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent,
            _ => Path::new("."),
        };
        let staging = parent.join(format!(
            ".{}.create-{}",
            name.to_string_lossy(),
            Uuid::new_v4()
        ));
        fs::DirBuilder::new().mode(0o700).create(&staging)?;
        let built = build_empty(&staging, key).and_then(|()| {
            fs::rename(&staging, root)?;
            sync_directory(&File::open(parent)?)
        });
        if let Err(error) = built {
            let _ = fs::remove_dir_all(&staging);
            return Err(error);
        }
        Self::open(root, key)
    }

    /// The home's directory.
    pub fn path(&self) -> &Path {
        &self.root
    }

    /// The home identity from its descriptor.
    pub fn identity(&self) -> Uuid {
        self.identity
    }

    /// True after a failed publish or a poisoned commit or publication lock.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    /// Level and size facts of the published CURRENT.
    pub fn status(&self) -> RunHomeStatus {
        let current = self.held();
        RunHomeStatus {
            generation: current.generation,
            frontier: current.frontier,
            levels: current.levels.iter().map(|level| totals(level)).collect(),
            merging: totals(&current.merging),
            base: totals(&current.base),
            record_bytes: current
                .newest_first()
                .map(|run| run.meta.record_bytes)
                .sum(),
            stale_bytes: current
                .tiered()
                .map(|run| {
                    run.hidden()
                        .map(|hidden| hidden.saturating_add(run.meta.tombstone_bytes))
                })
                .sum(),
        }
    }

    /// File bytes of the runs that merges replaced and that a held root or
    /// a read still keeps on disk.
    pub fn retired_bytes(&self) -> u64 {
        let mut retired = self.retired.lock().unwrap_or_else(|e| e.into_inner());
        retired.retain(|run| run.strong_count() > 0);
        retired
            .iter()
            .filter_map(Weak::upgrade)
            .map(|run| run.meta.file_bytes)
            .sum()
    }

    fn note_retired(&self, runs: &[Arc<RunFile>]) {
        let mut retired = self.retired.lock().unwrap_or_else(|e| e.into_inner());
        retired.retain(|run| run.strong_count() > 0);
        retired.extend(runs.iter().map(Arc::downgrade));
    }

    fn held(&self) -> Arc<Manifest> {
        Arc::clone(&self.current.read().unwrap_or_else(|e| e.into_inner()))
    }

    fn refuse_if_poisoned(&self) -> Result<()> {
        if self.is_poisoned() {
            return Err(Error::WriteCapacity(
                "disk run home: an earlier publish failed; restart the process to reopen the home"
                    .into(),
            ));
        }
        Ok(())
    }
}

pub(super) fn read_descriptor(root: &Path) -> Result<Uuid> {
    read_descriptor_file(&root.join(RUN_HOME_MARKER))
}

pub(super) fn read_descriptor_file(path: &Path) -> Result<Uuid> {
    let mut file = File::open(path)?;
    if file.metadata()?.len() != DESCRIPTOR_BYTES as u64 {
        return Err(corrupt("home descriptor length"));
    }
    let mut bytes = [0u8; DESCRIPTOR_BYTES];
    file.read_exact(&mut bytes)?;
    let crc = u32::from_le_bytes(bytes[60..].try_into().expect("4 bytes"));
    if &bytes[..8] != DESCRIPTOR_MAGIC
        || bytes[8..12] != 1u32.to_le_bytes()
        || crc != crc32fast::hash(&bytes[..60])
    {
        return Err(corrupt("unsupported home descriptor"));
    }
    let identity = Uuid::from_slice(&bytes[16..32]).expect("16 bytes");
    if identity.is_nil() {
        return Err(corrupt("nil home identity"));
    }
    Ok(identity)
}
