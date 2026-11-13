//! A disk format v2 home: sorted run files and one CURRENT.

mod cleanup;
mod commit;
mod manifest;
mod range;
mod read;

pub use cleanup::{CheckpointKind, MAX_CAS_DELETE_CHECKPOINT_BYTES};
pub use commit::{HomeMutation, RunCommit};
pub use manifest::{CURRENT, RUNS};
pub use range::{RunHashRange, RunRangePage};
pub use read::{ReadBudget, RunRead, RunReadCosts, RunRoot};

use super::codec::corrupt;
use super::merge::count_merged_keys;
use super::{MergeSpace, PreparedRun, MAX_RUN_KEY_BYTES, RUN_SUFFIX};
use crate::durability::{sync_directory, sync_dirty_file};
use crate::{Error, Result};
use manifest::{Manifest, RunFile};
use rustix::fs::{FlockOperation, OFlags};
use serde::Serialize;
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::Instant;
use uuid::Uuid;

const DESCRIPTOR_MAGIC: &[u8; 8] = b"LDRHOME4";
/// File name of the exclusive owner lock (flock).
pub const LOCK: &str = "LOCK";
pub(super) const NODE_CLEANUP: &str = "NODE_CLEANUP";
pub(super) const CAS_DELETE: &str = "CAS_DELETE";
/// Directory of the home owner's mutation log, beside `runs/`.
pub const MUTATION_LOG_DIR: &str = "mutation-log-v1";
/// Directory of the home owner's cloud backup: the capture source and the run holds.
pub const BACKUP_DIR: &str = "cloud-byte-v2";
/// File name of the clear home descriptor.
pub const RUN_HOME_MARKER: &str = "laststore-run-home-v2";

pub(super) fn invalid(msg: &str) -> Error {
    Error::Config(msg.into())
}

pub(super) fn require_path(root: &Path, lock: &File) -> Result<()> {
    let (h, c) = (lock.metadata()?, fs::symlink_metadata(root.join(LOCK))?);
    (c.is_file() && (h.dev(), h.ino()) == (c.dev(), c.ino()))
        .then_some(())
        .ok_or_else(|| invalid("disk run home: owner lock moved"))
}

pub(super) fn read_descriptor(path: &Path) -> Result<Uuid> {
    let (mut f, mut b) = (File::open(path)?, [0u8; 64]);
    if f.metadata()?.len() != 64 {
        return Err(corrupt("home descriptor length"));
    }
    f.read_exact(&mut b)?;
    let crc = u32::from_le_bytes(b[60..].try_into().unwrap());
    if &b[..8] != DESCRIPTOR_MAGIC || b[8..12] != [1, 0, 0, 0] || crc != crc32fast::hash(&b[..60]) {
        return Err(corrupt("unsupported home descriptor"));
    }
    let id = Uuid::from_slice(&b[16..32]).unwrap();
    (!id.is_nil())
        .then_some(id)
        .ok_or_else(|| corrupt("nil home identity"))
}

/// A complete newest-key inventory, or an explicitly partial prefix after an audit cap.
#[derive(Debug, Serialize)]
pub struct LogicalInventory {
    /// True only after every selected run record enters the merge.
    pub complete: bool,
    /// Distinct newest keys counted, including live keys and tombstones.
    pub merged_keys: u64,
}

/// Published root coordinates and optional logical key inventory for diagnostics.
#[derive(Debug, Serialize)]
pub struct RunHomeDiagnostics {
    /// Generation of the captured published root.
    pub generation: u64,
    /// Complete-commit frontier of the captured published root.
    pub frontier: u64,
    /// Optional newest-key inventory.
    pub inventory: Option<LogicalInventory>,
}

/// An open v2 home. One process owns it; reads and writes share it.
pub struct RunHome {
    root: PathBuf,
    runs: PathBuf,
    key: [u8; 32],
    identity: Uuid,
    current: RwLock<Arc<Manifest>>,
    locks: (Mutex<()>, Mutex<()>, Mutex<()>),
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
            .ok_or_else(|| invalid("disk run home: root has no name"))?
            .to_string_lossy();
        let parent = root
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let dir = parent.join(format!(".{name}.create-{}", Uuid::new_v4()));
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
        let root = root.canonicalize()?;
        let identity = read_descriptor(&root.join(RUN_HOME_MARKER))?;
        let flags = (OFlags::NOFOLLOW | OFlags::NONBLOCK).bits() as i32;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .mode(0o600)
            .custom_flags(flags)
            .open(root.join(LOCK))?;
        match rustix::fs::flock(&lock, FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => require_path(&root, &lock)?,
            Err(e) if e == rustix::io::Errno::WOULDBLOCK => {
                return Err(invalid("disk run home: another owner holds the home"))
            }
            Err(e) => return Err(std::io::Error::from(e).into()),
        }
        let manifest = Manifest::load(&root, key, identity, None)?;
        for e in fs::read_dir(&root)? {
            let e = e?;
            let s = e.file_name().to_string_lossy().into_owned();
            if s.ends_with(".tmp")
                && [CURRENT, NODE_CLEANUP, CAS_DELETE]
                    .iter()
                    .any(|f| s.starts_with(&format!(".{f}.")))
            {
                fs::remove_file(e.path())?;
            }
        }
        let runs = root.join(RUNS);
        let named: HashSet<Uuid> = manifest.newest_first().map(|r| r.meta.run_id).collect();
        for e in fs::read_dir(&runs)? {
            let e = e?;
            if is_removable_run(&e.file_name().to_string_lossy(), &named) {
                fs::remove_file(e.path())?;
            }
        }
        for run in manifest.newest_first() {
            if fs::metadata(&run.path)?.len() != run.meta.file_bytes {
                return Err(corrupt("a run that CURRENT names has the wrong size"));
            }
        }
        Ok(Self {
            root,
            runs,
            key: *key,
            identity,
            current: RwLock::new(Arc::new(manifest)),
            locks: (Mutex::new(()), Mutex::new(()), Mutex::new(())),
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
    /// True after a failed publish or a poisoned commit or publication lock.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    /// Tiered run count, merging run count, and total active run file bytes.
    pub fn run_totals(&self) -> (usize, usize, u64) {
        let h = self.held();
        let bytes = h.newest_first().map(|r| r.meta.file_bytes).sum();
        (h.tiered().count(), h.merging.len(), bytes)
    }

    /// File bytes of replaced runs still held by an active root or read.
    pub fn retired_bytes(&self) -> u64 {
        let (_, held) = self.live_retired();
        held.iter().map(|f| f.meta.file_bytes).sum()
    }

    fn live_retired(&self) -> (usize, Vec<Arc<RunFile>>) {
        let mut r = self.retired.lock().unwrap_or_else(|e| e.into_inner());
        r.retain(|w| w.strong_count() > 0);
        (r.len(), r.iter().filter_map(Weak::upgrade).collect())
    }

    /// Capture isolated storage diagnostics for the current published root.
    pub fn diagnostics(&self, with_inv: bool, deadline: Instant) -> Result<RunHomeDiagnostics> {
        if !crate::diagnostics_enabled() {
            return Err(invalid(
                "storage diagnostics require an isolated timing capture",
            ));
        }
        let root = {
            let _c = self.locks.0.lock().unwrap_or_else(|e| e.into_inner());
            let _m = self.locks.2.lock().unwrap_or_else(|e| e.into_inner());
            self.refuse_if_poisoned()?;
            self.held()
        };
        let inventory = with_inv.then(|| {
            let runs: Vec<_> = root
                .tiered()
                .chain(&root.merging)
                .map(|r| r.path.clone())
                .collect();
            let base = root.base.iter().map(|r| r.path.clone()).collect();
            let (complete, merged_keys) =
                count_merged_keys(&self.key, root.retained_bytes(), &runs, base, deadline);
            LogicalInventory {
                complete,
                merged_keys,
            }
        });
        Ok(RunHomeDiagnostics {
            generation: root.generation,
            frontier: root.frontier,
            inventory,
        })
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
            .stage(&self.root, &self.key, Some(space))
            .and_then(|s| self.publish_staged(next, &s));
        if res.is_ok() || self.is_poisoned() {
            space.keep_outputs();
        }
        res
    }

    fn install_prepared(&self, file: Arc<RunFile>, run: PreparedRun) -> Result<Arc<Manifest>> {
        let slot = Mutex::new(Some(run));
        let finish = || slot.lock().unwrap().take().unwrap().finish().map(|_| ());
        std::thread::scope(|s| {
            let w = std::thread::Builder::new()
                .name("laststore-sync".into())
                .spawn_scoped(s, finish);
            let guard = self.publish_lock();
            let staged = guard.as_ref().ok().map(|_| self.stage_new_run(&file));
            let pan = |_| Err(invalid("disk run: durability worker panicked"));
            let synced = w.map_or_else(|_| finish(), |h| h.join().unwrap_or_else(pan));
            let (_g, (next, p)) = (guard?, staged.unwrap()?);
            synced.inspect_err(|_| drop(fs::remove_file(&p)))?;
            self.publish_staged(next, &p)
        })
    }

    fn retire_runs(&self, runs: &[Arc<RunFile>]) {
        for r in runs {
            r.retire();
        }
        let mut r = self.retired.lock().unwrap_or_else(|e| e.into_inner());
        r.retain(|w| w.strong_count() > 0);
        r.extend(runs.iter().map(Arc::downgrade));
    }

    fn held(&self) -> Arc<Manifest> {
        Arc::clone(&self.current.read().unwrap_or_else(|e| e.into_inner()))
    }

    fn refuse_if_poisoned(&self) -> Result<()> {
        let msg =
            "disk run home: an earlier publish failed; restart the process to reopen the home";
        (!self.is_poisoned())
            .then_some(())
            .ok_or_else(|| Error::WriteCapacity(msg.into()))
    }
}

pub(super) fn open_nofollow(path: &Path) -> std::io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(OFlags::NOFOLLOW.bits() as i32)
        .open(path)
}

pub(super) fn run_key(collection: &str, key: &[u8]) -> Result<Vec<u8>> {
    let len = u16::try_from(collection.len())
        .map_err(|_| invalid("disk run home: collection name too long"))?;
    (2 + collection.len() + key.len() <= MAX_RUN_KEY_BYTES)
        .then(|| [&len.to_be_bytes()[..], collection.as_bytes(), key].concat())
        .ok_or_else(|| invalid("disk run home: storage key exceeds the run key limit"))
}

pub(super) fn storage_key(collection: &str, rk: &[u8]) -> Result<String> {
    let at = 2 + collection.len();
    if rk.len() < at
        || rk[..2] != (collection.len() as u16).to_be_bytes()
        || &rk[2..at] != collection.as_bytes()
    {
        return Err(corrupt("run key outside its collection"));
    }
    String::from_utf8(rk[at..].to_vec()).map_err(|_| corrupt("storage key is not UTF-8"))
}

pub(super) fn is_removable_run(name: &str, keep: &HashSet<Uuid>) -> bool {
    name.strip_suffix(RUN_SUFFIX)
        .and_then(|id| Uuid::parse_str(id).ok())
        .is_some_and(|id| !keep.contains(&id))
        || (name.starts_with('.') && name.ends_with(".tmp"))
}

pub(super) fn write_private_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(bytes)
        .map_err(Error::from)
        .and_then(|()| sync_dirty_file(&f))
        .inspect_err(|_| drop(fs::remove_file(path)))
}
