//! Bounded private sidecar store, local cancel, and streaming backup object reader.
use super::{
    invalid, open_planned_reader,
    planner::{clear_objects, held_root, RunSource},
    protocol, run_backup_destination,
    run_capture::Intent,
    ByteBackupCapture, Inner, PackRuntime, State, StorageError, StorageResult, MAX_OBJECTS,
    RUN_INTENT_FILE, STATE_FORMAT,
};
use crate::storage::cloud_backup_plan::PlanSegment;
use crate::storage::disk_backup::NativeCloudWorkGuard;
use crate::storage::disk_pack::run_blocking;
use cloud_backup_protocol::{bytes_sha256, v2::hash::canonical_bytes, v2::Begin, Digest};
use lastdb_model::storage_policy::{backup_transport::METADATA_PACK_BYTES, BACKUP_RESOURCES};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use zeroize::Zeroizing;

const RECORD_MAX: u64 = 256 * 1024;
const MAX_FILES: usize = 2 * MAX_OBJECTS as usize + 16;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Saved {
    value: serde_json::Value,
    sha256: Digest,
}

pub(super) struct Store {
    pub root: PathBuf,
    bytes: AtomicU64,
    coordinator: AtomicU64,
    policy: laststore::space::SpacePolicy,
    space_root: PathBuf,
}

pub(super) fn sync_dir(path: &Path) -> StorageResult<()> {
    Ok(File::open(path)?.sync_all()?)
}

pub(super) fn private_directory(path: &Path) -> StorageResult<()> {
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => sync_dir(
            path.parent()
                .ok_or_else(|| invalid("byte backup directory parent"))?,
        )?,
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }
    let m = fs::symlink_metadata(path)?;
    (m.is_dir() && m.uid() == unsafe { libc::geteuid() } && m.mode() & 0o077 == 0)
        .then_some(())
        .ok_or_else(|| invalid("byte backup directory ownership"))
}

pub(super) fn private(meta: &fs::Metadata) -> StorageResult<()> {
    (meta.is_file()
        && meta.uid() == unsafe { libc::geteuid() }
        && meta.mode() & 0o077 == 0
        && meta.nlink() == 1)
        .then_some(())
        .ok_or_else(|| invalid("byte source private regular file required"))
}

impl Store {
    pub fn open(home: &Path, policy: &laststore::space::SpacePolicy) -> StorageResult<Self> {
        let backup = home.join(laststore::disk_run::home::BACKUP_DIR);
        let root = backup.join("source");
        private_directory(&backup)?;
        private_directory(&root)?;
        let mut total = 0u64;
        for (i, entry) in fs::read_dir(&root)?.enumerate() {
            if i >= MAX_FILES {
                return Err(invalid("byte source file count"));
            }
            let meta = fs::symlink_metadata(entry?.path())?;
            private(&meta)?;
            total = total
                .checked_add(meta.len())
                .ok_or_else(|| invalid("byte source spool overflow"))?;
        }
        let store = Self {
            root,
            bytes: AtomicU64::new(total),
            coordinator: AtomicU64::new(0),
            policy: policy.clone(),
            space_root: home.to_path_buf(),
        };
        store.admit(0).map(|()| store)
    }

    pub fn coordinator(&self, bytes: u64) -> StorageResult<()> {
        self.coordinator.store(bytes, Ordering::Release);
        self.admit(0)
    }

    fn admit(&self, extra: u64) -> StorageResult<()> {
        self.bytes
            .load(Ordering::Acquire)
            .checked_add(self.coordinator.load(Ordering::Acquire))
            .and_then(|b| b.checked_add(extra))
            .filter(|&total| total <= BACKUP_RESOURCES.durable_spool_bytes)
            .map(|_| ())
            .ok_or_else(|| invalid("byte source combined spool bound"))
    }

    pub fn read_raw_opt(&self, name: &str, max: u64) -> StorageResult<Option<Vec<u8>>> {
        let file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(self.root.join(name))
        {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let meta = file.metadata()?;
        private(&meta)?;
        let mut bytes = Vec::new();
        if meta.len() > max || (&file).take(max + 1).read_to_end(&mut bytes)? as u64 > max {
            return Err(invalid("byte source value bound"));
        }
        file.sync_all()?;
        sync_dir(&self.root).map(|()| Some(bytes))
    }

    pub fn read_opt<T: DeserializeOwned>(&self, name: &str) -> StorageResult<Option<T>> {
        let Some(raw) = self.read_raw_opt(name, RECORD_MAX)? else {
            return Ok(None);
        };
        let rec: Saved =
            serde_json::from_slice(&raw).map_err(|_| invalid("byte source saved value format"))?;
        if rec.sha256 != bytes_sha256(&canonical_bytes(&rec.value).map_err(protocol)?) {
            return Err(invalid("byte source saved value checksum"));
        }
        serde_json::from_value(rec.value)
            .map(Some)
            .map_err(|_| invalid("byte source saved value format"))
    }

    pub fn read<T: DeserializeOwned>(&self, name: &str) -> StorageResult<T> {
        self.read_opt(name)?
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound).into())
    }

    pub fn write(&self, name: &str, value: &impl Serialize) -> StorageResult<()> {
        let value = serde_json::to_value(value).map_err(|_| invalid("byte source encoding"))?;
        let sha256 = bytes_sha256(&canonical_bytes(&value).map_err(protocol)?);
        let bytes = serde_json::to_vec(&Saved { value, sha256 })
            .map_err(|_| invalid("byte source encoding"))?;
        (bytes.len() as u64 <= RECORD_MAX)
            .then_some(())
            .ok_or_else(|| invalid("byte source value bound"))
            .and_then(|()| self.write_raw(name, &bytes))
    }

    pub fn write_raw(&self, name: &str, bytes: &[u8]) -> StorageResult<()> {
        let max = if name.starts_with("payload-") && name.ends_with(".enc") {
            METADATA_PACK_BYTES
        } else {
            RECORD_MAX
        };
        if bytes.len() as u64 > max + 29 {
            return Err(invalid("byte source value bound"));
        }
        let (path, pending) = (
            self.root.join(name),
            self.root.join(format!("{name}.pending")),
        );
        self.remove_path(&pending)?;
        self.admit(bytes.len() as u64)?;
        let prev = file_size(&path)?.unwrap_or(0);
        let _space =
            laststore::space::reserve_space(&self.space_root, &self.policy, bytes.len() as u64)
                .map_err(crate::storage::disk_pack::map)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&pending)?;
        let written = file.write_all(bytes).and_then(|()| file.sync_all());
        self.bytes
            .fetch_add(file.metadata()?.len(), Ordering::AcqRel);
        written?;
        fs::rename(&pending, &path)?;
        self.bytes.fetch_sub(prev, Ordering::AcqRel);
        sync_dir(&self.root)
    }

    pub fn remove(&self, name: &str) -> StorageResult<()> {
        self.remove_path(&self.root.join(name))
    }

    fn remove_path(&self, path: &Path) -> StorageResult<()> {
        if let Some(len) = file_size(path)? {
            fs::remove_file(path)?;
            self.bytes.fetch_sub(len, Ordering::AcqRel);
        }
        sync_dir(&self.root)
    }
}

fn file_size(path: &Path) -> StorageResult<Option<u64>> {
    match fs::symlink_metadata(path) {
        Ok(meta) => private(&meta).map(|()| Some(meta.len())),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

impl PackRuntime {
    pub async fn cancel_unsubmitted_byte_backup_capture(
        self: &Arc<Self>,
        begin: &Begin,
        key: &[u8; 32],
    ) -> StorageResult<()> {
        begin.validate().map_err(protocol)?;
        let admission = NativeCloudWorkGuard::acquire()?;
        let (runtime, begin, key) = (Arc::clone(self), begin.clone(), Zeroizing::new(*key));
        run_blocking(move || {
            let store = Store::open(runtime.physical.path(), &runtime.space_policy)?;
            let Some(mut state) = store.read_opt::<State>("job.json")? else {
                return cancel_intent(&runtime, &store, &begin, &key, None);
            };
            state.validate()?;
            if state.begin != begin {
                if !state.released || state.begin.job == begin.job {
                    return Err(invalid("run cancel identity"));
                }
                return cancel_intent(&runtime, &store, &begin, &key, Some(state.reference_id));
            }
            if state.terminal.is_some() || state.key_sha256 != bytes_sha256(key.as_ref()) {
                return Err(invalid("run cancel identity"));
            }
            state.local_cancelled = true;
            store.write("job.json", &state)?;
            ByteBackupCapture {
                inner: Arc::new(Inner::new(runtime, store, state, None, admission)),
            }
            .finish_terminal()
        })
        .await
    }
}

fn cancel_intent(
    runtime: &PackRuntime,
    store: &Store,
    begin: &Begin,
    key: &[u8; 32],
    prev: Option<uuid::Uuid>,
) -> StorageResult<()> {
    let Some(intent) = store.read_opt::<Intent>(RUN_INTENT_FILE)? else {
        return Ok(());
    };
    if intent.begin != *begin
        || intent.key_sha256 != bytes_sha256(key)
        || intent.reference_id.is_nil()
        || prev == Some(intent.reference_id)
    {
        return Err(invalid("run cancel intent"));
    }
    RunSource::release_saved(&held_root(runtime, intent.reference_id))?;
    store.remove(RUN_INTENT_FILE)
}

pub(super) fn retire_previous_format(runtime: &PackRuntime, store: &Store) -> StorageResult<()> {
    #[derive(Deserialize)]
    struct Ref {
        #[serde(default)]
        format: u16,
        reference_id: uuid::Uuid,
        #[serde(default)]
        objects: u64,
    }
    let Some(job) = store.read_opt::<Ref>("job.json")? else {
        return Ok(());
    };
    if job.format >= STATE_FORMAT {
        return Ok(());
    }
    let intent_id = store
        .read_opt::<Ref>(RUN_INTENT_FILE)?
        .map(|r| r.reference_id);
    for id in [Some(job.reference_id), intent_id].into_iter().flatten() {
        let root = held_root(runtime, id);
        if !id.is_nil() && root.try_exists()? {
            RunSource::release_saved(&root)?;
        }
    }
    clear_objects(store, job.objects.min(MAX_OBJECTS))?;
    store
        .remove(RUN_INTENT_FILE)
        .and_then(|()| store.remove("job.json"))
}

impl ByteBackupCapture {
    pub fn reader(&self, index: u64) -> StorageResult<Box<dyn Read + Send>> {
        let state = self.lock()?;
        if !state.ready || !state.is_active() || self.inner.run.is_none() {
            return Err(invalid("byte source reader phase"));
        }
        if self.inner.readers.load(Ordering::Acquire) >= BACKUP_RESOURCES.concurrent_streams {
            return Err(invalid("byte source stream admission"));
        }
        let object = self.inner.object(&state, index)?;
        let (inner, dest) = (Arc::clone(&self.inner), object.input.object.destination);
        let source = open_planned_reader(
            object,
            |name, max| {
                self.inner
                    .store
                    .read_raw_opt(name, max)?
                    .ok_or_else(|| StorageError::from(io::Error::from(io::ErrorKind::NotFound)))
            },
            move |part: PlanSegment| {
                let run = inner
                    .run
                    .as_ref()
                    .ok_or_else(|| io::Error::other("byte retained source unavailable"))?;
                let src = run
                    .files
                    .get(part.file)
                    .ok_or_else(|| io::Error::other("byte source segment index"))?;
                let path = src
                    .path
                    .to_str()
                    .ok_or_else(|| io::Error::other("byte native UTF8 path"))?;
                if run_backup_destination(path).ok() != Some(dest) {
                    return Err(io::Error::other("byte source destination differs"));
                }
                let mut r = run
                    .reader(&src.path)
                    .map_err(|_| io::Error::other("byte source generation read"))?;
                r.seek(SeekFrom::Start(part.offset))
                    .map(|_| (r, part.bytes))
            },
        )?;
        self.inner.readers.fetch_add(1, Ordering::AcqRel);
        Ok(Box::new(ReaderGuard(Some(source), Arc::clone(&self.inner))))
    }
}

struct ReaderGuard(Option<Box<dyn Read + Send>>, Arc<Inner>);
impl Read for ReaderGuard {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let cap = buf.len().min(BACKUP_RESOURCES.stream_buffer_bytes as usize);
        self.0
            .as_mut()
            .expect("active reader")
            .read(&mut buf[..cap])
    }
}
impl Drop for ReaderGuard {
    fn drop(&mut self) {
        self.0 = None;
        self.1.readers.fetch_sub(1, Ordering::AcqRel);
    }
}
