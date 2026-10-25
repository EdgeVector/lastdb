//! A disk format v2 home: sorted run files and one CURRENT.
//!
//! ```text
//! <root>/laststore-run-home-v2   64 B clear descriptor: magic, home id, CRC
//! <root>/LOCK                    exclusive owner lock (flock)
//! <root>/CURRENT                 sealed list of runs by level + frontier
//! <root>/runs/<run id>.ldrun     immutable run files
//! ```
//!
//! A commit writes one level-0 run with all of its records, makes the run
//! durable, then publishes a new CURRENT by rename. A read takes one
//! CURRENT for its whole call and searches the runs newest first, so it
//! sees one complete commit boundary and never a part of a commit.
//!
//! [`RunHome::merge_step`] merges the runs of a full level (eight runs) into
//! one run on the next level. A merge into the deepest level drops
//! tombstones. A read that holds an older CURRENT keeps the merged runs
//! alive; the last holder deletes their files.
//!
//! After a crash, the owner deletes temporary files and run files that
//! CURRENT does not name when it opens the home. Nothing is replayed: the
//! last CURRENT is the complete state.
//!
//! Values are stored as given. The block seal of every run is the
//! encryption at rest; the home key protects every run and CURRENT.

mod commit;
mod keys;
mod manifest;
mod range;
mod read;

pub use commit::{RunCommit, RunMerge, LAST_LEVEL_RATIO, MERGE_WIDTH};
pub use range::{RunHashRange, RunRangePage};
pub use read::{RunRead, RunReadCosts};

use super::codec::corrupt;
use super::{RunOptions, RUN_SUFFIX};
use crate::durability::{sync_directory, sync_dirty_file};
use crate::{Error, Result};
use manifest::{Manifest, RUNS};
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use uuid::Uuid;

/// File name of the clear home descriptor. Its presence selects format v2.
pub const RUN_HOME_MARKER: &str = "laststore-run-home-v2";
const DESCRIPTOR_MAGIC: &[u8; 8] = b"LDRHOME2";
const DESCRIPTOR_BYTES: usize = 64;

/// Level and size facts of the published CURRENT.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunHomeStatus {
    /// CURRENT generation.
    pub generation: u64,
    /// Complete commits.
    pub frontier: u64,
    /// For each level: runs, records (tombstones included), file bytes.
    pub levels: Vec<(usize, u64, u64)>,
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
    /// Serializes merges, so a merge's inputs stay in CURRENT until it ends.
    merge: Mutex<()>,
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
    /// place, so a crash leaves either no home or a complete one.
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
            levels: current
                .levels
                .iter()
                .map(|level| {
                    let records: u64 = level.iter().map(|run| run.meta.records).sum();
                    let bytes: u64 = level.iter().map(|run| run.meta.file_bytes).sum();
                    (level.len(), records, bytes)
                })
                .collect(),
        }
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

    /// Publish `next` as CURRENT and make it the readable state.
    /// The caller holds the publish lock. An error before the rename leaves
    /// the old CURRENT in force; an error after it poisons the home.
    fn install(&self, next: Manifest) -> Result<Arc<Manifest>> {
        let staged = next.stage(&self.root, &self.key)?;
        if let Err(error) = Manifest::replace(&self.root, &staged) {
            // The rename may or may not have happened: stop all writes.
            self.poisoned.store(true, Ordering::Release);
            return Err(error);
        }
        let next = Arc::new(next);
        *self.current.write().unwrap_or_else(|e| e.into_inner()) = Arc::clone(&next);
        Ok(next)
    }
}

/// Write the descriptor and the first CURRENT of an empty home in `dir`.
fn build_empty(dir: &Path, key: &[u8; 32]) -> Result<()> {
    fs::create_dir(dir.join(RUNS))?;
    let identity = Uuid::new_v4();
    let mut descriptor = [0u8; DESCRIPTOR_BYTES];
    descriptor[..8].copy_from_slice(DESCRIPTOR_MAGIC);
    descriptor[8..12].copy_from_slice(&1u32.to_le_bytes());
    descriptor[16..32].copy_from_slice(identity.as_bytes());
    let crc = crc32fast::hash(&descriptor[..60]);
    descriptor[60..].copy_from_slice(&crc.to_le_bytes());
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(dir.join(RUN_HOME_MARKER))?;
    file.write_all(&descriptor)?;
    sync_dirty_file(&file)?;
    let staged = Manifest::empty(identity).stage(dir, key)?;
    Manifest::replace(dir, &staged)?;
    sync_directory(&File::open(dir.join(RUNS))?)
}

fn read_descriptor(root: &Path) -> Result<Uuid> {
    let mut file = File::open(root.join(RUN_HOME_MARKER))?;
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

/// Remove temporary files and unnamed runs, and check that every named run
/// is present at its recorded size. Only the two home directories are read.
fn remove_leftovers(root: &Path, runs: &Path, manifest: &Manifest) -> Result<()> {
    for entry in fs::read_dir(root)? {
        let name = entry?.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(".CURRENT.") && name.ends_with(".tmp") {
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
