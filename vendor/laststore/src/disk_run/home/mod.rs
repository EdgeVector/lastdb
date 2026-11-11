//! A disk format v2 home: sorted run files and one CURRENT.

mod capture;
mod cleanup;
mod commit;
mod diagnostic;
mod manifest;
mod open;
mod publication;
mod range;
mod read;
mod stream;

pub use capture::{CapturedRunFile, CapturedRunHome, CapturedRunKind, SnapshotFile};
pub use cleanup::{CheckpointKind, RunCleanupFiles, MAX_CAS_DELETE_CHECKPOINT_BYTES};
pub use commit::{HomeMutation, RunCommit};
pub use diagnostic::{
    CollectionInventory, FileDiagnostics, LogicalInventory, RunDiagnosticCheckpoint,
    RunHomeDiagnostics, RunPartDiagnostics,
};
pub use manifest::{CURRENT, RUNS};
pub use range::{RangeBounds, RunHashRange, RunRangePage};
pub use read::{ReadBudget, RunRead, RunReadCosts, RunRoot};

use super::codec::corrupt;
use crate::durability::{sync_directory, sync_dirty_file};
use crate::{Error, Result};
use manifest::{Manifest, RunFile};
use open::build_empty;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
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
/// Directory of the home owner's cloud backup: the capture source and the run holds.
pub const BACKUP_DIR: &str = "cloud-byte-v2";
/// File name of the clear home descriptor.
pub const RUN_HOME_MARKER: &str = "laststore-run-home-v2";
/// `LDRHOME4`: every value is stored as given.
pub(super) const DESCRIPTOR_MAGIC: &[u8; 8] = b"LDRHOME4";
pub(super) const DESCRIPTOR_BYTES: usize = 64;

/// Level and size facts of the published CURRENT.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunHomeStatus {
    /// CURRENT generation.
    pub generation: u64,
    /// Complete commits.
    pub frontier: u64,
    /// Runs in tiered levels.
    pub tiered_runs: usize,
    /// Runs in a base merge in progress.
    pub merging_runs: usize,
    /// File bytes of every published run.
    pub file_bytes: u64,
}

/// An open v2 home. One process owns it; reads and writes share it.
pub struct RunHome {
    root: PathBuf,
    runs: PathBuf,
    key: [u8; 32],
    identity: Uuid,
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
        let parent = root
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let staging = parent.join(format!(
            ".{}.create-{}",
            name.to_string_lossy(),
            Uuid::new_v4()
        ));
        fs::DirBuilder::new().mode(0o700).create(&staging)?;
        if let Err(error) = build_empty(&staging, key).and_then(|()| {
            fs::rename(&staging, root)?;
            sync_directory(parent)
        }) {
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
            tiered_runs: current.levels.iter().map(Vec::len).sum(),
            merging_runs: current.merging.len(),
            file_bytes: current.newest_first().map(|run| run.meta.file_bytes).sum(),
        }
    }

    /// File bytes of the runs that merges replaced and that a held root or a read still keeps.
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

pub(super) fn write_private_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    let res = file
        .write_all(bytes)
        .map_err(Error::from)
        .and_then(|()| sync_dirty_file(&file));
    if res.is_err() {
        let _ = fs::remove_file(path);
    }
    res
}

pub(super) fn verified_hard_link(
    source: &Path,
    target: &Path,
    bytes: u64,
    reason: &'static str,
) -> Result<()> {
    let before = fs::symlink_metadata(source)?;
    if !before.is_file() || before.len() != bytes {
        return Err(Error::Config(reason.into()));
    }
    fs::hard_link(source, target)?;
    let after = fs::symlink_metadata(target)?;
    if !after.is_file()
        || (before.dev(), before.ino(), bytes) != (after.dev(), after.ino(), after.len())
    {
        return Err(Error::Config(reason.into()));
    }
    Ok(())
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
