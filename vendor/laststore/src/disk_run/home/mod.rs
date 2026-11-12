//! A disk format v2 home: sorted run files and one CURRENT.

mod capture;
mod cleanup;
mod commit;
mod manifest;
mod open;
mod range;
mod read;

pub use capture::{CapturedRunFile, CapturedRunHome, CapturedRunKind, SnapshotFile};
pub use cleanup::{CheckpointKind, MAX_CAS_DELETE_CHECKPOINT_BYTES};
pub use commit::{HomeMutation, RunCommit};
pub use manifest::{CURRENT, RUNS};
pub use range::{RunHashRange, RunRangePage};
pub use read::{ReadBudget, RunRead, RunReadCosts, RunRoot};

use super::codec::corrupt;
use super::{
    MergeIter, MergeSpace, PreparedRun, RunRange, RunReader, MAX_RUN_KEY_BYTES, RUN_SUFFIX,
};
use crate::durability::{sync_directory, sync_dirty_file};
use crate::{Error, Result};
use manifest::{Manifest, RunFile};
use open::{build_empty, read_descriptor_file};
use serde::Serialize;
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::ops::Bound;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::Instant;
use uuid::Uuid;

/// File name of the exclusive owner lock (flock).
pub const LOCK: &str = "LOCK";
const NODE_CLEANUP: &str = "NODE_CLEANUP";
const CAS_DELETE: &str = "CAS_DELETE";
/// Directory of the home owner's mutation log, beside `runs/`.
pub const MUTATION_LOG_DIR: &str = "mutation-log-v1";
/// Directory of the home owner's cloud backup: the capture source and the run holds.
pub const BACKUP_DIR: &str = "cloud-byte-v2";
/// File name of the clear home descriptor.
pub const RUN_HOME_MARKER: &str = "laststore-run-home-v2";

fn invalid(msg: &str) -> Error {
    Error::Config(msg.into())
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
        let staging = parent.join(format!(
            ".{}.create-{}",
            name.to_string_lossy(),
            Uuid::new_v4()
        ));
        fs::DirBuilder::new().mode(0o700).create(&staging)?;
        build_empty(&staging, key)
            .and_then(|()| fs::rename(&staging, root).map_err(Error::from))
            .and_then(|()| sync_directory(parent))
            .inspect_err(|_| drop(fs::remove_dir_all(&staging)))?;
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
        let mut retired = self.retired.lock().unwrap_or_else(|e| e.into_inner());
        retired.retain(|run| run.strong_count() > 0);
        retired
            .iter()
            .filter_map(Weak::upgrade)
            .map(|r| r.meta.file_bytes)
            .sum()
    }

    /// Capture isolated storage diagnostics for the current published root.
    pub fn diagnostics(
        &self,
        with_inventory: bool,
        deadline: Instant,
    ) -> Result<RunHomeDiagnostics> {
        if !crate::diagnostics_enabled() {
            return Err(invalid(
                "storage diagnostics require an isolated timing capture",
            ));
        }
        let root = {
            let _commit = self.commit.lock().unwrap_or_else(|e| e.into_inner());
            let _merge = self.merge.lock().unwrap_or_else(|e| e.into_inner());
            self.refuse_if_poisoned()?;
            self.held()
        };
        Ok(RunHomeDiagnostics {
            generation: root.generation,
            frontier: root.frontier,
            inventory: with_inventory.then(|| build_inventory(&self.key, &root, deadline)),
        })
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
        std::thread::scope(|scope| {
            let worker = std::thread::Builder::new()
                .name("laststore-sync".into())
                .spawn_scoped(scope, finish);
            let guard = self.publish_lock();
            let staged = guard.as_ref().ok().map(|_| self.stage_new_run(&file));
            let synced = match worker {
                Ok(h) => h
                    .join()
                    .unwrap_or_else(|_| Err(invalid("disk run: durability worker panicked"))),
                Err(_) => finish(),
            };
            let (_g, staged) = (guard?, staged.unwrap());
            match (staged, synced) {
                (Ok((_, p)), Err(e)) => {
                    let _ = fs::remove_file(p);
                    Err(e)
                }
                (Err(e), _) => Err(e),
                (Ok((next, p)), Ok(())) => self.publish_staged(next, &p),
            }
        })
    }

    fn retire_runs(&self, runs: &[Arc<RunFile>]) {
        runs.iter().for_each(|r| r.retire());
        let mut retired = self.retired.lock().unwrap_or_else(|e| e.into_inner());
        retired.retain(|run| run.strong_count() > 0);
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

fn build_inventory(key: &[u8; 32], root: &Manifest, deadline: Instant) -> LogicalInventory {
    let mut merged_keys = 0u64;
    let complete = (|| -> Result<()> {
        let count = root.tiered().count() + root.merging.len() + usize::from(!root.base.is_empty());
        if count == 0 {
            return Ok(());
        }
        let retained = root.retained_bytes();
        if count > 512 || retained > 56 << 20 {
            return Err(Error::ReadBudgetExceeded);
        }
        let allowance = ((56 << 20) - retained) / count as u64;
        let mut sources = Vec::with_capacity(count);
        for run in root.tiered().chain(&root.merging) {
            sources.push(Arc::new(RunReader::open_bounded(&run.path, key, allowance, 0)?).iter());
        }
        if !root.base.is_empty() {
            let paths = root.base.iter().map(|r| r.path.clone()).collect();
            sources.push(RunRange::sequence(
                paths,
                key,
                Bound::Unbounded,
                Bound::Unbounded,
            ));
        }
        for item in MergeIter::new(sources) {
            if merged_keys >= 50_000_000 || Instant::now() >= deadline {
                return Err(Error::ReadBudgetExceeded);
            }
            let (rk, _) = item?;
            if rk
                .get(..2)
                .is_none_or(|p| rk.len() < 2 + u16::from_be_bytes([p[0], p[1]]) as usize)
            {
                return Err(Error::Corrupt("diagnostic prefix".into()));
            }
            merged_keys += 1;
        }
        Ok(())
    })()
    .is_ok();
    LogicalInventory {
        complete,
        merged_keys,
    }
}

fn open_nofollow(path: &Path) -> std::io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
        .open(path)
}

fn run_key(collection: &str, key: &[u8]) -> Result<Vec<u8>> {
    let len = u16::try_from(collection.len())
        .map_err(|_| invalid("disk run home: collection name too long"))?;
    if 2 + collection.len() + key.len() > MAX_RUN_KEY_BYTES {
        return Err(invalid(
            "disk run home: storage key exceeds the run key limit",
        ));
    }
    Ok([&len.to_be_bytes()[..], collection.as_bytes(), key].concat())
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

fn is_removable_run(name: &str, keep: &HashSet<Uuid>) -> bool {
    name.strip_suffix(RUN_SUFFIX)
        .and_then(|id| Uuid::parse_str(id).ok())
        .is_some_and(|id| !keep.contains(&id))
        || (name.starts_with('.') && name.ends_with(".tmp"))
}

fn write_private_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)
        .map_err(Error::from)
        .and_then(|()| sync_dirty_file(&file))
        .inspect_err(|_| drop(fs::remove_file(path)))
}

fn verified_hard_link(
    source: &Path,
    target: &Path,
    bytes: u64,
    reason: &'static str,
) -> Result<()> {
    let before = fs::symlink_metadata(source)?;
    if !before.is_file() || before.len() != bytes {
        return Err(invalid(reason));
    }
    fs::hard_link(source, target)?;
    let after = fs::symlink_metadata(target)?;
    (after.is_file()
        && (before.dev(), before.ino(), bytes) == (after.dev(), after.ino(), after.len()))
        .then_some(())
        .ok_or_else(|| invalid(reason))
}
