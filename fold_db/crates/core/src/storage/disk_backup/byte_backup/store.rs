//! Bounded private sidecar store and streaming backup object reader.

use super::{
    invalid, protocol, run_backup_destination, ByteBackupCapture, Inner, PlanSegment,
    StorageResult, MAX_OBJECTS,
};
use lastdb_model::storage_policy::{backup_transport::METADATA_PACK_BYTES, BACKUP_RESOURCES};
use serde::{de::DeserializeOwned, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const RECORD_MAX: u64 = 256 * 1024;
const MAX_FILES: usize = 2 * MAX_OBJECTS as usize + 16;

pub(super) struct Store {
    pub root: PathBuf,
    bytes: AtomicU64,
    coordinator: AtomicU64,
    policy: laststore::space::SpacePolicy,
    space_root: PathBuf,
}

pub(super) fn private_directory(path: &Path) -> StorageResult<()> {
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => {
            let parent = path
                .parent()
                .ok_or_else(|| invalid("byte backup directory parent"))?;
            File::open(parent)?.sync_all()?;
        }
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }
    let meta = fs::symlink_metadata(path)?;
    if !meta.is_dir() || meta.uid() != unsafe { libc::geteuid() } || meta.mode() & 0o077 != 0 {
        return Err(invalid("byte backup directory ownership"));
    }
    Ok(())
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
        let total = self
            .bytes
            .load(Ordering::Acquire)
            .checked_add(self.coordinator.load(Ordering::Acquire))
            .and_then(|b| b.checked_add(extra))
            .ok_or_else(|| invalid("byte source combined spool overflow"))?;
        (total <= BACKUP_RESOURCES.durable_spool_bytes)
            .then_some(())
            .ok_or_else(|| invalid("byte source combined spool bound"))
    }

    pub fn exists(&self, name: &str) -> StorageResult<bool> {
        Ok(file_size(&self.root.join(name))?.is_some())
    }

    pub fn read_bytes(&self, name: &str, max: u64) -> StorageResult<Vec<u8>> {
        let mut file = input(&self.root.join(name))?;
        let mut bytes = Vec::new();
        if file.metadata()?.len() > max
            || (&mut file).take(max + 1).read_to_end(&mut bytes)? as u64 > max
        {
            return Err(invalid("byte source value bound"));
        }
        file.sync_all()?;
        File::open(&self.root)?.sync_all()?;
        Ok(bytes)
    }

    pub fn read<T: DeserializeOwned>(&self, name: &str) -> StorageResult<T> {
        let rec: SavedValue = serde_json::from_slice(&self.read_bytes(name, RECORD_MAX)?)
            .map_err(|_| invalid("byte source saved value format"))?;
        if rec.sha256 != checksum(&rec.value)? {
            return Err(invalid("byte source saved value checksum"));
        }
        serde_json::from_value(rec.value).map_err(|_| invalid("byte source saved value format"))
    }

    pub fn write(&self, name: &str, value: &impl Serialize) -> StorageResult<()> {
        let value = serde_json::to_value(value).map_err(|_| invalid("byte source encoding"))?;
        let bytes = serde_json::to_vec(&SavedValue {
            sha256: checksum(&value)?,
            value,
        })
        .map_err(|_| invalid("byte source encoding"))?;
        if bytes.len() as u64 > RECORD_MAX {
            return Err(invalid("byte source value bound"));
        }
        self.write_raw(name, &bytes)
    }

    pub fn write_raw(&self, name: &str, bytes: &[u8]) -> StorageResult<()> {
        let is_enc = name.starts_with("payload-") && name.ends_with(".enc");
        let max = if is_enc {
            METADATA_PACK_BYTES
        } else {
            RECORD_MAX
        } + 29;
        if bytes.len() as u64 > max {
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
        Ok(File::open(&self.root)?.sync_all()?)
    }

    pub fn remove(&self, name: &str) -> StorageResult<()> {
        self.remove_path(&self.root.join(name))
    }

    fn remove_path(&self, path: &Path) -> StorageResult<()> {
        if let Some(len) = file_size(path)? {
            fs::remove_file(path)?;
            self.bytes.fetch_sub(len, Ordering::AcqRel);
        }
        Ok(File::open(&self.root)?.sync_all()?)
    }
}

fn file_size(path: &Path) -> StorageResult<Option<u64>> {
    match fs::symlink_metadata(path) {
        Ok(meta) => private(&meta).map(|()| Some(meta.len())),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

#[derive(Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedValue {
    value: serde_json::Value,
    sha256: cloud_backup_protocol::Digest,
}

fn checksum(value: &serde_json::Value) -> StorageResult<cloud_backup_protocol::Digest> {
    let bytes = cloud_backup_protocol::v2::hash::canonical_bytes(value).map_err(protocol)?;
    Ok(cloud_backup_protocol::bytes_sha256(&bytes))
}

fn input(path: &Path) -> StorageResult<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    private(&file.metadata()?).map(|()| file)
}

pub(super) fn private(meta: &fs::Metadata) -> StorageResult<()> {
    (meta.is_file()
        && meta.uid() == unsafe { libc::geteuid() }
        && meta.mode() & 0o077 == 0
        && meta.nlink() == 1)
        .then_some(())
        .ok_or_else(|| invalid("byte source private regular file required"))
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
        let expected = object.input.object;
        let source: Box<dyn Read + Send> = if let Some(name) = &object.ciphertext {
            let b = self.inner.store.read_bytes(name, expected.bytes.get())?;
            if b.len() as u64 != expected.bytes.get()
                || cloud_backup_protocol::bytes_sha256(&b) != expected.sha256
            {
                return Err(invalid("byte source encrypted metadata length"));
            }
            Box::new(io::Cursor::new(b))
        } else {
            let (inner, dest) = (Arc::clone(&self.inner), expected.destination);
            Box::new(super::SegmentReader::new(
                object.segments,
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
                    r.seek(SeekFrom::Start(part.offset))?;
                    Ok((r, part.bytes))
                },
            ))
        };
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
