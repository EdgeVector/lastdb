//! Home creation, owner lock acquisition, CURRENT publication, and stopped-home reads.

use super::manifest::{Manifest, RunFile, MAX_CURRENT_BYTES};
use super::{
    invalid, is_removable_run, open_nofollow, write_private_file, ReadBudget, RunHome, RunRead,
    CAS_DELETE, CURRENT, LOCK, NODE_CLEANUP, RUNS, RUN_HOME_MARKER,
};
use crate::disk_run::codec::corrupt;
use crate::disk_run::RunReader;
use crate::durability::sync_directory;
use crate::frame::{decode_frame_bounded, VerifiedFrameReader, MIN_ENCODED_LEN};
use crate::{Error, Result};
use rustix::fs::FlockOperation;
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use uuid::Uuid;
use zeroize::Zeroizing;

const DESCRIPTOR_MAGIC: &[u8; 8] = b"LDRHOME4";
const DESCRIPTOR_BYTES: usize = 64;

pub(super) type ReadSlot = (Arc<RunFile>, Option<Arc<RunReader>>);

pub(super) fn overflow() -> Error {
    Error::WriteCapacity("disk run home: generation counter is full".into())
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
    let id = Uuid::from_slice(&bytes[16..32]).expect("16 bytes");
    (!id.is_nil())
        .then_some(id)
        .ok_or_else(|| corrupt("nil home identity"))
}

impl Manifest {
    fn check_home(self, home_id: Uuid) -> Result<Self> {
        (self.home_id == home_id)
            .then_some(self)
            .ok_or_else(|| corrupt("CURRENT names another home"))
    }

    pub(super) fn load(root: &Path, key: &[u8; 32], home_id: Uuid) -> Result<Self> {
        let mut file = File::open(root.join(CURRENT))?;
        let len = file.metadata()?.len();
        if !(MIN_ENCODED_LEN as u64..=MAX_CURRENT_BYTES as u64).contains(&len) {
            return Err(corrupt("CURRENT length"));
        }
        let mut frame = Vec::with_capacity(len as usize);
        file.read_to_end(&mut frame)?;
        let payload = decode_frame_bounded(key, &frame, MAX_CURRENT_BYTES)?;
        Self::decode_bounded(&payload, 0, &root.join(RUNS), u64::MAX)?.check_home(home_id)
    }

    pub(super) fn load_paths_bounded(
        current: &Path,
        runs: &Path,
        key: &[u8; 32],
        home_id: Uuid,
        maximum: u64,
    ) -> Result<Self> {
        let policy = &lastdb_model::storage_policy::BACKUP_RESOURCES;
        let maximum = maximum.min(policy.working_memory_bytes);
        if maximum < 2 * policy.stream_buffer_bytes + 16 {
            return Err(Error::ReadBudgetExceeded);
        }
        let file = open_nofollow(current)?;
        let length = file.metadata()?.len();
        if !(MIN_ENCODED_LEN as u64..=MAX_CURRENT_BYTES as u64).contains(&length) {
            return Err(corrupt("CURRENT length"));
        }
        let mut input = VerifiedFrameReader::open(&file, 0, length, key)?;
        let (bytes, reader_mem) = (input.payload_len, input.memory_bytes);
        let count = (bytes.saturating_add(reader_mem) <= maximum)
            .then(|| usize::try_from(bytes).ok())
            .flatten()
            .ok_or(Error::ReadBudgetExceeded)?;
        let mut payload = Zeroizing::new(Vec::new());
        payload
            .try_reserve_exact(count)
            .map_err(|_| Error::ReadBudgetExceeded)?;
        if (payload.capacity() as u64).saturating_add(reader_mem) > maximum {
            return Err(Error::ReadBudgetExceeded);
        }
        payload.resize(count, 0);
        input.read_exact(&mut payload)?;
        if input.read(&mut [0])? != 0 {
            return Err(corrupt("CURRENT length"));
        }
        drop(input);
        Self::decode_bounded(&payload, payload.capacity(), runs, maximum)?.check_home(home_id)
    }
}

pub(super) fn build_empty(dir: &Path, key: &[u8; 32]) -> Result<()> {
    fs::DirBuilder::new().mode(0o700).create(dir.join(RUNS))?;
    write_private_file(&dir.join(LOCK), &[])?;
    let identity = Uuid::new_v4();
    let mut desc = [0u8; DESCRIPTOR_BYTES];
    desc[..8].copy_from_slice(DESCRIPTOR_MAGIC);
    desc[8..12].copy_from_slice(&1u32.to_le_bytes());
    desc[16..32].copy_from_slice(identity.as_bytes());
    let crc = crc32fast::hash(&desc[..60]).to_le_bytes();
    desc[60..].copy_from_slice(&crc);
    write_private_file(&dir.join(RUN_HOME_MARKER), &desc)?;
    let staged = Manifest::empty(identity).stage(dir, key, None)?;
    Manifest::replace(dir, &staged)?;
    sync_directory(&dir.join(RUNS))
}

fn open_owner_lock(root: &Path) -> Result<File> {
    let flags = (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .mode(0o600)
        .custom_flags(flags)
        .open(root.join(LOCK))?;
    match rustix::fs::flock(&lock, FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => require_path(root, &lock).map(|()| lock),
        Err(e) if e == rustix::io::Errno::WOULDBLOCK => {
            Err(invalid("disk run home: another owner holds the home"))
        }
        Err(e) => Err(std::io::Error::from(e).into()),
    }
}

fn require_path(root: &Path, lock: &File) -> Result<()> {
    let (held, current) = (lock.metadata()?, fs::symlink_metadata(root.join(LOCK))?);
    (current.is_file() && (held.dev(), held.ino()) == (current.dev(), current.ino()))
        .then_some(())
        .ok_or_else(|| invalid("disk run home: owner lock moved"))
}

fn remove_leftovers(root: &Path, runs: &Path, manifest: &Manifest) -> Result<()> {
    for entry in fs::read_dir(root)? {
        let name = entry?.file_name();
        let s = name.to_string_lossy();
        if s.ends_with(".tmp")
            && [CURRENT, NODE_CLEANUP, CAS_DELETE]
                .iter()
                .any(|f| s.starts_with(&format!(".{f}.")))
        {
            fs::remove_file(root.join(name))?;
        }
    }
    let named: HashSet<Uuid> = manifest.newest_first().map(|r| r.meta.run_id).collect();
    for entry in fs::read_dir(runs)? {
        let name = entry?.file_name();
        if is_removable_run(&name.to_string_lossy(), &named) {
            fs::remove_file(runs.join(name))?;
        }
    }
    for run in manifest.newest_first() {
        if fs::metadata(&run.path)?.len() != run.meta.file_bytes {
            return Err(corrupt("a run that CURRENT names has the wrong size"));
        }
    }
    Ok(())
}

impl RunHome {
    /// Open an existing home, take its owner lock, and remove crash leftovers.
    pub fn open(root: &Path, key: &[u8; 32]) -> Result<Self> {
        let root = root.canonicalize()?;
        let identity = read_descriptor_file(&root.join(RUN_HOME_MARKER))?;
        let lock = open_owner_lock(&root)?;
        let manifest = Manifest::load(&root, key, identity)?;
        let runs = root.join(RUNS);
        remove_leftovers(&root, &runs, &manifest)?;
        Ok(Self {
            root,
            runs,
            key: *key,
            identity,
            current: RwLock::new(Arc::new(manifest)),
            commit: Mutex::new(()),
            publish: Mutex::new(()),
            merge: Mutex::new(()),
            retired: Mutex::new(Vec::new()),
            poisoned: AtomicBool::new(false),
            lock: Arc::new(lock),
        })
    }

    pub(super) fn commit_lock(&self) -> Result<MutexGuard<'_, ()>> {
        self.commit.lock().map_err(|_| self.poisoned_lock())
    }

    pub(super) fn publish_lock(&self) -> Result<MutexGuard<'_, ()>> {
        self.publish.lock().map_err(|_| self.poisoned_lock())
    }

    pub(super) fn publish_staged(&self, next: Manifest, staged: &Path) -> Result<Arc<Manifest>> {
        if let Err(err) = Manifest::replace(&self.root, staged) {
            self.poisoned.store(true, Ordering::Release);
            return Err(err);
        }
        let next = Arc::new(next);
        *self.current.write().map_err(|_| self.poisoned_lock())? = Arc::clone(&next);
        Ok(next)
    }

    pub(super) fn stage_new_run(&self, file: &Arc<RunFile>) -> Result<(Manifest, PathBuf)> {
        self.refuse_if_poisoned()?;
        let mut next = (*self.held()).clone();
        next.generation = next.generation.checked_add(1).ok_or_else(overflow)?;
        next.frontier = next.frontier.checked_add(1).ok_or_else(overflow)?;
        if next.levels.is_empty() {
            next.levels.push(Vec::new());
        }
        next.levels[0].insert(0, Arc::clone(file));
        let path = next.stage(&self.root, &self.key, None)?;
        Ok((next, path))
    }

    fn poisoned_lock(&self) -> Error {
        self.poisoned.store(true, Ordering::Release);
        Error::WriteCapacity(
            "disk run home: a lock was poisoned; restart the process to reopen the home".into(),
        )
    }
}

pub(super) fn slots(manifest: &Manifest, maximum: u64) -> Result<Vec<ReadSlot>> {
    let (count, sz) = (
        manifest.newest_first().count(),
        std::mem::size_of::<ReadSlot>(),
    );
    if count.checked_mul(sz).is_none_or(|b| b as u64 > maximum) {
        return Err(Error::ReadBudgetExceeded);
    }
    let mut runs = Vec::new();
    runs.try_reserve_exact(count)
        .map_err(|_| Error::ReadBudgetExceeded)?;
    if (runs.capacity() * sz) as u64 > maximum {
        return Err(Error::ReadBudgetExceeded);
    }
    runs.extend(manifest.newest_first().map(|r| (Arc::clone(r), None)));
    Ok(runs)
}

impl RunRead {
    /// Fork the exact held root with preallocation-bounded backup reads enabled.
    pub fn fork_bounded(&self, budget: ReadBudget) -> Result<Self> {
        let retained = self.shared_metadata_bytes.max(self.retained_bytes());
        let avail = lastdb_model::storage_policy::BACKUP_RESOURCES
            .working_memory_bytes
            .checked_sub(retained)
            .ok_or(Error::ReadBudgetExceeded)?;
        Self::bounded_from(
            Arc::clone(&self.manifest),
            self.key,
            budget,
            avail,
            Some(retained),
            Arc::clone(&self._owner_lock),
        )
    }

    /// Open one existing CURRENT without the cleanup performed by `RunHome::open`.
    pub fn open_existing(root: &Path, key: &[u8; 32], budget: ReadBudget) -> Result<Self> {
        Self::open_existing_inner(
            root,
            key,
            budget,
            None,
            FlockOperation::NonBlockingLockExclusive,
        )
    }

    /// Open an inactive root with explicit combined metadata-memory admission.
    pub fn open_existing_bounded(
        root: &Path,
        key: &[u8; 32],
        budget: ReadBudget,
        maximum: u64,
    ) -> Result<Self> {
        Self::open_existing_inner(
            root,
            key,
            budget,
            Some(maximum),
            FlockOperation::NonBlockingLockExclusive,
        )
    }

    /// Open an existing root alongside other readers while excluding writers.
    pub fn open_existing_shared_bounded(
        root: &Path,
        key: &[u8; 32],
        budget: ReadBudget,
        maximum: u64,
    ) -> Result<Self> {
        Self::open_existing_inner(
            root,
            key,
            budget,
            Some(maximum),
            FlockOperation::NonBlockingLockShared,
        )
    }

    fn open_existing_inner(
        root: &Path,
        key: &[u8; 32],
        budget: ReadBudget,
        maximum: Option<u64>,
        operation: FlockOperation,
    ) -> Result<Self> {
        let root = root.canonicalize()?;
        let lock = open_nofollow(&root.join(LOCK))?;
        if !lock.metadata()?.is_file() {
            return Err(invalid("disk run snapshot: LOCK is not a file"));
        }
        rustix::fs::flock(&lock, operation)
            .map_err(|_| invalid("disk run snapshot: another owner holds the home"))?;
        require_path(&root, &lock)?;
        let identity = read_descriptor_file(&root.join(RUN_HOME_MARKER))?;
        let lock = Arc::new(lock);
        if let Some(bytes) = maximum {
            let manifest = Arc::new(Manifest::load_paths_bounded(
                &root.join(CURRENT),
                &root.join(RUNS),
                key,
                identity,
                bytes,
            )?);
            let avail = bytes
                .checked_sub(manifest.retained_bytes())
                .ok_or(Error::ReadBudgetExceeded)?;
            return Self::bounded_from(manifest, *key, budget, avail, None, lock);
        }
        let manifest = Arc::new(Manifest::load(&root, key, identity)?);
        let avail = u64::MAX
            .checked_sub(manifest.retained_bytes())
            .ok_or(Error::ReadBudgetExceeded)?;
        Ok(Self {
            bounded_streams: false,
            shared_metadata_bytes: 0,
            runs: slots(&manifest, avail)?,
            manifest,
            key: *key,
            budget,
            costs: super::RunReadCosts::default(),
            metadata: crate::disk_run::MetadataMemo::new(budget.bytes),
            _owner_lock: lock,
        })
    }
}
