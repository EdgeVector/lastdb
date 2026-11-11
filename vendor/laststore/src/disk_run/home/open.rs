//! Home creation, owner lock acquisition, crash-leftover removal, and stopped-home reads.

use super::manifest::{Manifest, RunFile, MAX_CURRENT_BYTES};
use super::{
    read_descriptor, write_private_file, ReadBudget, RunHome, RunRead, RunReadCosts, CAS_DELETE,
    CURRENT, DESCRIPTOR_BYTES, DESCRIPTOR_MAGIC, LOCK, NODE_CLEANUP, RUNS, RUN_HOME_MARKER,
};
use crate::disk_run::codec::corrupt;
use crate::disk_run::{MetadataMemo, RunReader, RUN_SUFFIX};
use crate::durability::sync_directory;
use crate::frame::{decode_frame_bounded, min_encoded_len, VerifiedFrameReader};
use crate::{Error, Result};
use rustix::fs::FlockOperation;
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use uuid::Uuid;
use zeroize::Zeroizing;

impl Manifest {
    pub(super) fn load(root: &Path, key: &[u8; 32], home_id: Uuid) -> Result<Self> {
        Self::load_paths(&root.join(CURRENT), &root.join(RUNS), key, home_id)
    }

    pub(in crate::disk_run::home) fn load_paths(
        current: &Path,
        runs: &Path,
        key: &[u8; 32],
        home_id: Uuid,
    ) -> Result<Self> {
        let mut file = File::open(current)?;
        let len = file.metadata()?.len();
        if len < min_encoded_len() as u64 || len > MAX_CURRENT_BYTES as u64 {
            return Err(corrupt("CURRENT length"));
        }
        let mut frame = Vec::with_capacity(len as usize);
        file.read_to_end(&mut frame)?;
        let decoded = decode_frame_bounded(key, &frame, MAX_CURRENT_BYTES)?;
        let manifest = Self::decode(&decoded.payload, runs)?;
        if manifest.home_id != home_id {
            return Err(corrupt("CURRENT names another home"));
        }
        Ok(manifest)
    }

    pub(in crate::disk_run::home) fn load_paths_bounded(
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
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(current)?;
        let length = file.metadata()?.len();
        if length < min_encoded_len() as u64 || length > MAX_CURRENT_BYTES as u64 {
            return Err(corrupt("CURRENT length"));
        }
        let mut input = VerifiedFrameReader::open(&file, 0, length, key)?;
        let (bytes, reader_mem) = (input.payload_len, input.memory_bytes);
        if bytes.checked_add(reader_mem).is_none_or(|n| n > maximum) {
            return Err(Error::ReadBudgetExceeded);
        }
        let count = usize::try_from(bytes).map_err(|_| Error::ReadBudgetExceeded)?;
        let mut payload = Zeroizing::new(Vec::new());
        payload
            .try_reserve_exact(count)
            .map_err(|_| Error::ReadBudgetExceeded)?;
        if (payload.capacity() as u64)
            .checked_add(reader_mem)
            .is_none_or(|n| n > maximum)
        {
            return Err(Error::ReadBudgetExceeded);
        }
        payload.resize(count, 0);
        input.read_exact(&mut payload)?;
        if input.read(&mut [0])? != 0 {
            return Err(corrupt("CURRENT length"));
        }
        drop(input);
        let manifest = Self::decode_bounded(&payload, payload.capacity(), runs, maximum)?;
        if manifest.home_id != home_id {
            return Err(corrupt("CURRENT names another home"));
        }
        Ok(manifest)
    }
}

pub(in crate::disk_run::home) type ReadSlot = (Arc<RunFile>, Option<RunReader>);

pub(in crate::disk_run::home) fn build_empty(dir: &Path, key: &[u8; 32]) -> Result<()> {
    fs::DirBuilder::new().mode(0o700).create(dir.join(RUNS))?;
    write_private_file(&dir.join(LOCK), &[])?;
    let identity = Uuid::new_v4();
    let mut descriptor = [0u8; DESCRIPTOR_BYTES];
    descriptor[..8].copy_from_slice(DESCRIPTOR_MAGIC);
    descriptor[8..12].copy_from_slice(&1u32.to_le_bytes());
    descriptor[16..32].copy_from_slice(identity.as_bytes());
    let crc = crc32fast::hash(&descriptor[..60]).to_le_bytes();
    descriptor[60..].copy_from_slice(&crc);
    write_private_file(&dir.join(RUN_HOME_MARKER), &descriptor)?;
    let staged = Manifest::empty(identity).stage(dir, key)?;
    Manifest::replace(dir, &staged)?;
    sync_directory(&dir.join(RUNS))
}

fn open_owner_lock(root: &Path) -> Result<File> {
    let flags =
        rustix::fs::OFlags::NOFOLLOW.bits() as i32 | rustix::fs::OFlags::NONBLOCK.bits() as i32;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .mode(0o600)
        .custom_flags(flags)
        .open(root.join(LOCK))?;
    match rustix::fs::flock(&lock, FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => {
            require_path(root, &lock)?;
            Ok(lock)
        }
        Err(error) if error == rustix::io::Errno::WOULDBLOCK => Err(Error::Config(
            "disk run home: another owner holds the home".into(),
        )),
        Err(error) => Err(std::io::Error::from(error).into()),
    }
}

pub(in crate::disk_run::home) fn require_path(root: &Path, lock: &File) -> Result<()> {
    let held = lock.metadata()?;
    let current = fs::symlink_metadata(root.join(LOCK))?;
    if !current.is_file() || (held.dev(), held.ino()) != (current.dev(), current.ino()) {
        return Err(Error::Config("disk run home: owner lock moved".into()));
    }
    Ok(())
}

fn remove_leftovers(root: &Path, runs: &Path, manifest: &Manifest) -> Result<()> {
    for entry in fs::read_dir(root)? {
        let name = entry?.file_name();
        let name = name.to_string_lossy();
        if [CURRENT, NODE_CLEANUP, CAS_DELETE]
            .iter()
            .any(|file| name.starts_with(&format!(".{file}.")))
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

impl RunHome {
    /// Open an existing home, take its owner lock, and remove crash leftovers.
    pub fn open(root: &Path, key: &[u8; 32]) -> Result<Self> {
        let root = root.canonicalize()?;
        let identity = read_descriptor(&root)?;
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

    pub(in crate::disk_run::home) fn commit_lock(&self) -> Result<MutexGuard<'_, ()>> {
        self.commit.lock().map_err(|_| self.poisoned_lock())
    }

    pub(in crate::disk_run::home) fn publish_lock(&self) -> Result<MutexGuard<'_, ()>> {
        self.publish.lock().map_err(|_| self.poisoned_lock())
    }

    pub(in crate::disk_run::home) fn install_current(&self, next: &Arc<Manifest>) -> Result<()> {
        *self.current.write().map_err(|_| self.poisoned_lock())? = Arc::clone(next);
        Ok(())
    }

    fn poisoned_lock(&self) -> Error {
        self.poisoned.store(true, Ordering::Release);
        Error::WriteCapacity(
            "disk run home: a lock was poisoned; restart the process to reopen the home".into(),
        )
    }
}

pub(in crate::disk_run::home) fn slots(manifest: &Manifest, maximum: u64) -> Result<Vec<ReadSlot>> {
    let count = manifest.newest_first().count();
    let bytes = count
        .checked_mul(std::mem::size_of::<ReadSlot>())
        .ok_or(Error::ReadBudgetExceeded)?;
    if bytes as u64 > maximum {
        return Err(Error::ReadBudgetExceeded);
    }
    let mut runs = Vec::new();
    runs.try_reserve_exact(count)
        .map_err(|_| Error::ReadBudgetExceeded)?;
    if (runs.capacity() * std::mem::size_of::<ReadSlot>()) as u64 > maximum {
        return Err(Error::ReadBudgetExceeded);
    }
    runs.extend(manifest.newest_first().map(|run| (Arc::clone(run), None)));
    Ok(runs)
}

impl RunRead {
    /// Fork the exact held root with preallocation-bounded backup reads enabled.
    pub fn fork_bounded(&self, budget: ReadBudget) -> Result<Self> {
        let retained = self.shared_metadata_bytes.max(self.retained_bytes());
        let available = lastdb_model::storage_policy::BACKUP_RESOURCES
            .working_memory_bytes
            .checked_sub(retained)
            .ok_or(Error::ReadBudgetExceeded)?;
        let runs = slots(&self.manifest, available)?;
        let extra = (runs.capacity() * std::mem::size_of::<ReadSlot>()) as u64;
        Ok(Self {
            bounded_streams: true,
            shared_metadata_bytes: retained + extra,
            manifest: Arc::clone(&self.manifest),
            runs,
            key: self.key,
            budget,
            costs: RunReadCosts::default(),
            metadata: MetadataMemo::new(0),
            _owner_lock: Arc::clone(&self._owner_lock),
        })
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
        let lock = OpenOptions::new()
            .read(true)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(root.join(LOCK))?;
        if !lock.metadata()?.is_file() {
            return Err(Error::Config(
                "disk run snapshot: LOCK is not a file".into(),
            ));
        }
        rustix::fs::flock(&lock, operation)
            .map_err(|_| Error::Config("disk run snapshot: another owner holds the home".into()))?;
        require_path(&root, &lock)?;
        let identity = read_descriptor(&root)?;
        let manifest = Arc::new(match maximum {
            Some(bytes) => Manifest::load_paths_bounded(
                &root.join(CURRENT),
                &root.join(RUNS),
                key,
                identity,
                bytes,
            )?,
            None => Manifest::load(&root, key, identity)?,
        });
        let available = maximum
            .unwrap_or(u64::MAX)
            .checked_sub(manifest.retained_bytes())
            .ok_or(Error::ReadBudgetExceeded)?;
        let runs = slots(&manifest, available)?;
        Ok(Self {
            bounded_streams: maximum.is_some(),
            shared_metadata_bytes: 0,
            manifest,
            runs,
            key: *key,
            budget,
            costs: RunReadCosts::default(),
            metadata: MetadataMemo::new(if maximum.is_some() { 0 } else { budget.bytes }),
            _owner_lock: Arc::new(lock),
        })
    }
}
