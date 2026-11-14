//! A disk format v2 home: sorted run files and one CURRENT.

mod cleanup;
mod commit;
mod compaction;
mod diagnostics;
mod manifest;
mod range;
mod read;
mod snapshot;

pub use cleanup::CheckpointSpec;
pub use commit::{HomeMutation, RunCommit};
pub use diagnostics::{LogicalInventory, RunHomeDiagnostics};
pub use manifest::{CURRENT, RUNS};
pub use range::{RunHashRange, RunRangePage};
pub use read::{ReadBudget, RunRead, RunReadCosts};
pub use snapshot::{RunRoot, RunSnapshot};

use super::codec::corrupt;
use super::{MergeSpace, MAX_RUN_KEY_BYTES, RUN_SUFFIX};
use crate::durability::{sync_directory, sync_dirty_file};
use crate::{Error, Result};
use manifest::{Manifest, RunFile};
use rustix::fs::{FlockOperation, OFlags};
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, Weak};
use uuid::Uuid;

const DESCRIPTOR_MAGIC: &[u8; 8] = b"LDRHOME4";
/// File name of the exclusive owner lock (flock).
pub const LOCK: &str = "LOCK";
/// Directory of the home owner's cloud backup: the capture source and the run holds.
pub const BACKUP_DIR: &str = "cloud-byte-v2";
/// File name of the clear home descriptor.
pub const RUN_HOME_MARKER: &str = "laststore-run-home-v2";

fn invalid(msg: &str) -> Error {
    Error::Config(msg.into())
}

fn unpoison<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn require_path(root: &Path, lock: &File) -> Result<()> {
    let (held, cur) = (lock.metadata()?, fs::symlink_metadata(root.join(LOCK))?);
    (cur.is_file() && (held.dev(), held.ino()) == (cur.dev(), cur.ino()))
        .then_some(())
        .ok_or_else(|| invalid("disk run home: owner lock moved"))
}

fn read_descriptor(path: &Path) -> Result<Uuid> {
    let (mut file, mut buf) = (File::open(path)?, [0u8; 64]);
    if file.metadata()?.len() != 64 {
        return Err(corrupt("home descriptor length"));
    }
    file.read_exact(&mut buf)?;
    let crc = u32::from_le_bytes(buf[60..].try_into().unwrap());
    if &buf[..8] != DESCRIPTOR_MAGIC
        || buf[8..12] != [1, 0, 0, 0]
        || crc != crc32fast::hash(&buf[..60])
    {
        return Err(corrupt("unsupported home descriptor"));
    }
    let id = Uuid::from_slice(&buf[16..32]).unwrap();
    (!id.is_nil())
        .then_some(id)
        .ok_or_else(|| corrupt("nil home identity"))
}

/// An open v2 home. One process owns it; reads and writes share it.
pub struct RunHome {
    root_dir: PathBuf,
    runs_dir: PathBuf,
    key: [u8; 32],
    identity: Uuid,
    current: RwLock<Arc<Manifest>>,
    commit_mutex: Mutex<()>,
    publish_mutex: Mutex<()>,
    merge_mutex: Mutex<()>,
    retired: Mutex<Vec<Weak<RunFile>>>,
    poisoned: AtomicBool,
    lock: Arc<File>,
}

impl RunHome {
    /// Create a new empty home in `root`, which must be absent or empty.
    pub fn create(root: &Path, key: &[u8; 32]) -> Result<Self> {
        if root.try_exists()? && fs::read_dir(root)?.next().is_some() {
            return Err(invalid(
                "disk run home: create needs an absent or empty directory",
            ));
        }
        let name = root
            .file_name()
            .ok_or_else(|| invalid("disk run home: root has no name"))?;
        let parent = root
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let dir = parent.join(format!(
            ".{}.create-{}",
            name.to_string_lossy(),
            Uuid::new_v4()
        ));
        let mkdir = |p: &Path| fs::DirBuilder::new().mode(0o700).create(p);
        mkdir(&dir)?;
        (|| -> Result<()> {
            mkdir(&dir.join(RUNS))?;
            write_private_file(&dir.join(LOCK), &[])?;
            let (id, mut desc) = (Uuid::new_v4(), [0u8; 64]);
            desc[..8].copy_from_slice(DESCRIPTOR_MAGIC);
            desc[8..12].copy_from_slice(&[1, 0, 0, 0]);
            desc[16..32].copy_from_slice(id.as_bytes());
            let crc = crc32fast::hash(&desc[..60]).to_le_bytes();
            desc[60..].copy_from_slice(&crc);
            write_private_file(&dir.join(RUN_HOME_MARKER), &desc)?;
            let empty = Manifest {
                home_id: id,
                generation: 1,
                ..Manifest::default()
            };
            Manifest::replace(&dir, &empty.stage(&dir, key, None)?)?;
            sync_directory(&dir.join(RUNS))?;
            fs::rename(&dir, root)?;
            sync_directory(parent)
        })()
        .inspect_err(|_| drop(fs::remove_dir_all(&dir)))?;
        Self::open(root, key)
    }

    /// Open an existing home, take its owner lock, and remove crash leftovers.
    pub fn open(root: &Path, key: &[u8; 32]) -> Result<Self> {
        let root_dir = root.canonicalize()?;
        let identity = read_descriptor(&root_dir.join(RUN_HOME_MARKER))?;
        let flags = (OFlags::NOFOLLOW | OFlags::NONBLOCK).bits() as i32;
        let mut opts = OpenOptions::new();
        opts.create(true)
            .truncate(false)
            .write(true)
            .mode(0o600)
            .custom_flags(flags);
        let lock = opts.open(root_dir.join(LOCK))?;
        match rustix::fs::flock(&lock, FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => require_path(&root_dir, &lock)?,
            Err(e) if e == rustix::io::Errno::WOULDBLOCK => {
                return Err(Error::HomeBusy(
                    "disk run home: another owner holds the home".into(),
                ))
            }
            Err(e) => return Err(std::io::Error::from(e).into()),
        }
        let manifest = Manifest::load(&root_dir, key, identity, None)?;
        for entry in fs::read_dir(&root_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let text = name.to_string_lossy();
            if text.starts_with('.') && text.ends_with(".tmp") && entry.file_type()?.is_file() {
                fs::remove_file(entry.path())?;
            }
        }
        let runs_dir = root_dir.join(RUNS);
        let keep = manifest.newest_first().map(|r| r.meta.id).collect();
        sweep_runs(&runs_dir, &keep, usize::MAX)?;
        manifest.verify_files("a run that CURRENT names has the wrong size")?;
        Ok(Self {
            root_dir,
            runs_dir,
            key: *key,
            identity,
            current: RwLock::new(Arc::new(manifest)),
            commit_mutex: Mutex::new(()),
            publish_mutex: Mutex::new(()),
            merge_mutex: Mutex::new(()),
            retired: Mutex::default(),
            poisoned: AtomicBool::new(false),
            lock: Arc::new(lock),
        })
    }

    /// The home's directory.
    pub fn path(&self) -> &Path {
        &self.root_dir
    }

    /// The home identity from its descriptor.
    pub fn identity(&self) -> Uuid {
        self.identity
    }

    /// True after a failed publish or a poisoned commit or publication lock.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    /// Tiered run count, merging run count, and total active run file bytes.
    pub fn run_totals(&self) -> (usize, usize, u64) {
        let held = self.held();
        (
            held.tiered().count(),
            held.merging.len(),
            held.newest_first().map(|r| r.meta.file_bytes).sum(),
        )
    }

    /// File bytes of replaced runs still held by an active root or read.
    pub fn retired_bytes(&self) -> u64 {
        self.live_retired()
            .1
            .iter()
            .map(|f| f.meta.file_bytes)
            .sum()
    }

    fn live_retired(&self) -> (usize, Vec<Arc<RunFile>>) {
        let mut retired = unpoison(&self.retired);
        retired.retain(|w| w.strong_count() > 0);
        (
            retired.len(),
            retired.iter().filter_map(Weak::upgrade).collect(),
        )
    }

    fn next_manifest(&self, bump_frontier: bool) -> Result<Manifest> {
        self.refuse_if_poisoned()?;
        let mut next = (*self.held()).clone();
        let ovf = || Error::WriteCapacity("disk run home: generation counter is full".into());
        next.generation = next.generation.checked_add(1).ok_or_else(ovf)?;
        if bump_frontier {
            next.frontier = next.frontier.checked_add(1).ok_or_else(ovf)?;
        }
        Ok(next)
    }

    fn install_merge(&self, next: Manifest, space: &MergeSpace) -> Result<Arc<Manifest>> {
        let res = next
            .stage(&self.root_dir, &self.key, Some(space))
            .and_then(|s| self.publish_staged(next, &s));
        if res.is_ok() || self.is_poisoned() {
            space.keep_outputs();
        }
        res
    }

    fn retire_runs(&self, runs: &[Arc<RunFile>]) {
        for run in runs {
            run.retire();
        }
        let mut retired = unpoison(&self.retired);
        retired.retain(|w| w.strong_count() > 0);
        retired.extend(runs.iter().map(Arc::downgrade));
    }

    fn held(&self) -> Arc<Manifest> {
        Arc::clone(&self.current.read().unwrap_or_else(|e| e.into_inner()))
    }

    fn refuse_if_poisoned(&self) -> Result<()> {
        (!self.is_poisoned()).then_some(()).ok_or_else(|| {
            Error::WriteCapacity(
                "disk run home: an earlier publish failed; restart the process to reopen the home"
                    .into(),
            )
        })
    }
}

fn open_nofollow(path: &Path) -> std::io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(OFlags::NOFOLLOW.bits() as i32)
        .open(path)
}

fn run_key(collection: &str, key: &[u8]) -> Result<Vec<u8>> {
    let len = u16::try_from(collection.len())
        .map_err(|_| invalid("disk run home: collection name too long"))?;
    (2 + collection.len() + key.len() <= MAX_RUN_KEY_BYTES)
        .then(|| [&len.to_be_bytes()[..], collection.as_bytes(), key].concat())
        .ok_or_else(|| invalid("disk run home: storage key exceeds the run key limit"))
}

fn storage_key(collection: &str, rk: &[u8]) -> Result<String> {
    let at = 2 + collection.len();
    if rk.len() < at
        || rk[..2] != (collection.len() as u16).to_be_bytes()
        || &rk[2..at] != collection.as_bytes()
    {
        return Err(corrupt("run key outside its collection"));
    }
    String::from_utf8(rk[at..].to_vec()).map_err(|_| corrupt("storage key is not UTF-8"))
}

fn sweep_runs(runs: &Path, keep: &HashSet<Uuid>, cap: usize) -> Result<u64> {
    let mut removed = 0;
    for (idx, entry) in fs::read_dir(runs)?.enumerate() {
        if idx >= cap {
            return Err(Error::WriteCapacity(
                "cleanup run-directory entry bound exceeded".into(),
            ));
        }
        let entry = entry?;
        let name = entry.file_name();
        let text = name.to_string_lossy();
        let orphan = text
            .strip_suffix(RUN_SUFFIX)
            .and_then(|id| Uuid::parse_str(id).ok())
            .is_some_and(|id| !keep.contains(&id))
            || (text.starts_with('.') && text.ends_with(".tmp"));
        if orphan {
            match fs::remove_file(entry.path()) {
                Ok(()) => removed += 1,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
    }
    Ok(removed)
}

fn write_private_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut opts = OpenOptions::new();
    let mut file = opts.write(true).create_new(true).mode(0o600).open(path)?;
    file.write_all(bytes)
        .map_err(Error::from)
        .and_then(|()| sync_dirty_file(&file))
        .inspect_err(|_| drop(fs::remove_file(path)))
}
