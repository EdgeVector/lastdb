//! A disk format v2 home: sorted run files and one CURRENT.
//!
//! ```text
//! <root>/laststore-run-home-v2   64 B clear descriptor: magic, home id, CRC
//! <root>/LOCK                    exclusive owner lock (flock)
//! <root>/CURRENT                 sealed list of runs + frontier
//! <root>/runs/<run id>.ldrun     immutable run files
//! ```
//!
//! A commit writes one level-0 run with all of its records, makes the run
//! durable, then publishes a new CURRENT by rename. A read takes one
//! CURRENT for its whole call and searches the runs newest first, so it
//! sees one complete commit boundary and never a part of a commit.
//!
//! The runs are tiered levels, then the runs of a base merge in progress,
//! then the base: the oldest data, each live key once, in key order.
//! [`RunHome::merge_step`] merges a full level (eight runs) into one run on
//! the next level, and moves the oldest tiered runs into the base when they
//! or the stale versions grow past a fixed share of the home. The `plan`
//! module states the size bound. A read that holds an older CURRENT keeps
//! the merged runs alive; the last holder deletes their files.
//! [`RunHome::retired_bytes`] reports how much of that is still on disk, so
//! an owner whose reads are long can pace its merge steps.
//!
//! After a crash, the owner deletes temporary files and run files that
//! CURRENT does not name when it opens the home. Nothing is replayed: the
//! last CURRENT is the complete state.
//!
//! A commit stores each value in its [`crate::disk_run::value`] form, and a
//! read returns the value as given. The block seal of every run is the
//! encryption at rest; the home key protects every run and CURRENT.

mod capture;
mod cleanup;
mod cleanup_checkpoint;
mod cleanup_files;
mod commit;
mod commit_log;
mod create;
mod diagnostic;
mod diagnostic_cursor;
mod diagnostic_files;
mod diagnostic_inventory;
mod inventory;
mod keys;
mod maintenance;
mod manifest;
mod mutation_counts;
mod plan;
mod publication;
mod range;
mod read;
mod read_only;

pub use capture::{CapturedRunFile, CapturedRunHome, CapturedRunKind, SnapshotFile};
pub use cleanup_files::RunCleanupFiles;
pub use commit::{HomeMutation, MergeKind, RunCommit, RunMerge, MAX_MERGE_INPUTS, MERGE_WIDTH};
pub use diagnostic::{
    CollectionInventory, FileDiagnostics, LogicalInventory, RunDiagnosticCheckpoint,
    RunHomeDiagnostics,
};
pub use inventory::AdministrativePage;
pub use plan::{MIN_BASE_MERGE_BYTES, STALE_SHARE, TIER_SHARE};
pub use range::{RangeBounds, RunHashRange, RunRangePage};
pub use read::{CopiedRunBody, ReadBudget, RunRead, RunReadCosts, RunRoot};

use super::codec::corrupt;
use super::{RunOptions, RUN_SUFFIX};
use crate::durability::sync_directory;
use crate::{Error, Result};
use create::build_empty;
use manifest::{Manifest, RunFile, RUNS};
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use uuid::Uuid;

/// File name of the clear home descriptor. Its presence selects format v2.
pub const RUN_HOME_MARKER: &str = "laststore-run-home-v2";
/// `LDRHOME3`: values in their tagged stored form. An `LDRHOME2` home stored
/// values as given; no user data is in one, and this program refuses it.
const DESCRIPTOR_MAGIC: &[u8; 8] = b"LDRHOME3";
const UNTAGGED_MAGIC: &[u8; 8] = b"LDRHOME2";
const DESCRIPTOR_BYTES: usize = 64;

/// Largest value that a commit accepts: its stored form adds one tag byte.
pub const MAX_VALUE_BYTES: usize = super::MAX_RUN_VALUE_BYTES - 1;

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
    /// Serializes commits from the run write to the publish, so commits
    /// publish in call order.
    commit: Mutex<()>,
    /// Serializes CURRENT publication.
    publish: Mutex<()>,
    /// Serializes merge steps and hidden byte counts, so a step's inputs
    /// stay in CURRENT until it ends.
    merge: Mutex<()>,
    /// The runs that merges replaced. A run's file stays until the last
    /// root or read that names it ends; a dead handle is pruned.
    retired: Mutex<Vec<Weak<RunFile>>>,
    /// Set when a publish fails after its rename: the durable state is unknown.
    poisoned: AtomicBool,
    /// The owner lock. Each read holds a clone, so no other owner can open
    /// the home and remove runs while a read still names them.
    lock: Arc<File>,
}

impl RunHome {
    /// Create a new empty home in `root`, which must be absent or empty.
    ///
    /// The home is built in a hidden sibling directory and renamed into
    /// place, so a crash leaves either no home or a complete one. A crash
    /// can leave that `.<name>.create-<uuid>` directory beside the root;
    /// it is never opened, and the caller may delete it. The parent of
    /// `root` must exist.
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
        fs::create_dir(&staging)?;
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

    /// Open an existing home, take its owner lock, and remove what a crash
    /// left: temporary files and runs that CURRENT does not name.
    pub fn open(root: &Path, key: &[u8; 32]) -> Result<Self> {
        let root = root.canonicalize()?;
        let identity = read_descriptor(&root)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .mode(0o600)
            .open(root.join("LOCK"))?;
        match rustix::fs::flock(&lock, rustix::fs::FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => {}
            Err(error) if error == rustix::io::Errno::WOULDBLOCK => {
                return Err(Error::Config(
                    "disk run home: another owner holds the home".into(),
                ))
            }
            Err(error) => return Err(std::io::Error::from(error).into()),
        }
        let manifest = Manifest::load(&root, key, identity)?;
        let runs = root.join(RUNS);
        remove_leftovers(&root, &runs, &manifest)?;
        Ok(Self {
            root,
            runs,
            key: *key,
            identity,
            options: RunOptions::default(),
            current: RwLock::new(Arc::new(manifest)),
            commit: Mutex::new(()),
            publish: Mutex::new(()),
            merge: Mutex::new(()),
            retired: Mutex::new(Vec::new()),
            poisoned: AtomicBool::new(false),
            lock: Arc::new(lock),
        })
    }

    /// The home's directory.
    pub fn path(&self) -> &Path {
        &self.root
    }

    /// The home identity from its descriptor.
    pub fn identity(&self) -> Uuid {
        self.identity
    }

    /// True after a failed publish. The owner must reopen the home.
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
    /// a read still keeps on disk. They go when the last holder ends.
    pub fn retired_bytes(&self) -> u64 {
        let mut retired = self.retired.lock().unwrap_or_else(|e| e.into_inner());
        retired.retain(|run| run.strong_count() > 0);
        // A handle can be the last one: its drop then deletes the file.
        retired
            .iter()
            .filter_map(Weak::upgrade)
            .map(|run| run.meta.file_bytes)
            .sum()
    }

    /// Record the runs that a merge step just retired.
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
                "disk run home: an earlier publish failed; reopen the home".into(),
            ));
        }
        Ok(())
    }
}

fn read_descriptor(root: &Path) -> Result<Uuid> {
    read_descriptor_file(&root.join(RUN_HOME_MARKER))
}

fn read_descriptor_file(path: &Path) -> Result<Uuid> {
    let mut file = File::open(path)?;
    if file.metadata()?.len() != DESCRIPTOR_BYTES as u64 {
        return Err(corrupt("home descriptor length"));
    }
    let mut bytes = [0u8; DESCRIPTOR_BYTES];
    file.read_exact(&mut bytes)?;
    let crc = u32::from_le_bytes(bytes[60..].try_into().expect("4 bytes"));
    if &bytes[..8] == UNTAGGED_MAGIC {
        return Err(Error::Config(
            "disk run home: this home stores untagged values from an earlier v2 build; \
             create a new home"
                .into(),
        ));
    }
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

/// Remove temporary files and unnamed runs, and check that every named run
/// is present at its recorded size. Only the two home directories are read.
fn remove_leftovers(root: &Path, runs: &Path, manifest: &Manifest) -> Result<()> {
    for entry in fs::read_dir(root)? {
        let name = entry?.file_name();
        let name = name.to_string_lossy();
        if (name.starts_with(".CURRENT.") || name.starts_with(".NODE_CLEANUP."))
            && name.ends_with(".tmp")
        {
            fs::remove_file(root.join(&*name))?;
        }
    }
    let named: HashSet<Uuid> = manifest.newest_first().map(|run| run.meta.run_id).collect();
    for entry in fs::read_dir(runs)? {
        let name = entry?.file_name();
        let name = name.to_string_lossy();
        let unnamed_run = name
            .strip_suffix(RUN_SUFFIX)
            .and_then(|id| Uuid::parse_str(id).ok())
            .is_some_and(|id| !named.contains(&id));
        if unnamed_run || (name.starts_with('.') && name.ends_with(".tmp")) {
            fs::remove_file(runs.join(&*name))?;
        }
    }
    for run in manifest.newest_first() {
        if fs::metadata(&run.path)?.len() != run.meta.file_bytes {
            return Err(corrupt("a run that CURRENT names has the wrong size"));
        }
    }
    Ok(())
}
