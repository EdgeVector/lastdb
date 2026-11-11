//! Bounded private sidecar store and streaming backup object reader.

use super::{
    invalid, protocol, ByteBackupCapture, Inner, ObjectRecord, Segment, StorageResult, MAX_OBJECTS,
    SOURCE_DIR,
};
use serde::{de::DeserializeOwned, Serialize};
use sha2::{Digest as _, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const RECORD_MAX: u64 = 256 * 1024;
const MAX_FILES: usize = 2 * MAX_OBJECTS as usize + 16;

pub(in crate::storage::disk_backup::byte_backup) struct Store {
    pub root: PathBuf,
    bytes: AtomicU64,
    coordinator: AtomicU64,
    policy: laststore::space::SpacePolicy,
    space_root: PathBuf,
}

pub(in crate::storage::disk_backup::byte_backup) fn private_directory(
    path: &Path,
) -> StorageResult<()> {
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => File::open(
            path.parent()
                .ok_or_else(|| invalid("byte backup directory parent"))?,
        )?
        .sync_all()?,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
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
        let root = backup.join(SOURCE_DIR);
        private_directory(&backup)?;
        private_directory(&root)?;
        let mut total = 0u64;
        for (index, entry) in fs::read_dir(&root)?.enumerate() {
            if index >= MAX_FILES {
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
        store.admit(0)?;
        Ok(store)
    }

    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Acquire)
    }

    pub fn coordinator(&self, bytes: u64) -> StorageResult<()> {
        self.coordinator.store(bytes, Ordering::Release);
        self.admit(0)
    }

    fn admit(&self, extra: u64) -> StorageResult<()> {
        let total = self
            .bytes()
            .checked_add(self.coordinator.load(Ordering::Acquire))
            .and_then(|bytes| bytes.checked_add(extra))
            .ok_or_else(|| invalid("byte source combined spool overflow"))?;
        if total > lastdb_model::storage_policy::BACKUP_RESOURCES.durable_spool_bytes {
            return Err(invalid("byte source combined spool bound"));
        }
        Ok(())
    }

    pub fn exists(&self, name: &str) -> StorageResult<bool> {
        Ok(file_size(&self.root.join(name))?.is_some())
    }

    pub fn raw(&self, name: &str, max: u64) -> StorageResult<Vec<u8>> {
        let mut file = input(&self.root.join(name))?;
        if file.metadata()?.len() > max {
            return Err(invalid("byte source value bound"));
        }
        let mut bytes = Vec::new();
        (&mut file).take(max + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > max {
            return Err(invalid("byte source value bound"));
        }
        file.sync_all()?;
        File::open(&self.root)?.sync_all()?;
        Ok(bytes)
    }

    pub fn read<T: DeserializeOwned>(&self, name: &str) -> StorageResult<T> {
        let record: SavedValue = serde_json::from_slice(&self.raw(name, RECORD_MAX)?)
            .map_err(|_| invalid("byte source saved value format"))?;
        if record.sha256 != checksum(&record.value)? {
            return Err(invalid("byte source saved value checksum"));
        }
        serde_json::from_value(record.value).map_err(|_| invalid("byte source saved value format"))
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
        let maximum = if name.starts_with("payload-") && name.ends_with(".enc") {
            lastdb_model::storage_policy::backup_transport::METADATA_PACK_BYTES + 29
        } else {
            RECORD_MAX + 29
        };
        if bytes.len() as u64 > maximum {
            return Err(invalid("byte source value bound"));
        }
        let path = self.root.join(name);
        let pending = self.root.join(format!("{name}.pending"));
        self.remove_path(&pending)?;
        self.admit(bytes.len() as u64)?;
        let previous = file_size(&path)?.unwrap_or(0);
        let _space =
            laststore::space::reserve_space(&self.space_root, &self.policy, bytes.len() as u64)
                .map_err(crate::storage::disk_pack::map)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&pending)?;
        let written = (|| {
            file.write_all(bytes)?;
            file.sync_all()
        })();
        self.bytes
            .fetch_add(file.metadata()?.len(), Ordering::AcqRel);
        written?;
        fs::rename(&pending, &path)?;
        self.bytes.fetch_sub(previous, Ordering::AcqRel);
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }

    pub fn remove(&self, name: &str) -> StorageResult<()> {
        self.remove_path(&self.root.join(name))
    }

    fn remove_path(&self, path: &Path) -> StorageResult<()> {
        if let Some(len) = file_size(path)? {
            fs::remove_file(path)?;
            self.bytes.fetch_sub(len, Ordering::AcqRel);
        }
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }
}

fn file_size(path: &Path) -> StorageResult<Option<u64>> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            private(&meta)?;
            Ok(Some(meta.len()))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
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
    private(&file.metadata()?)?;
    Ok(file)
}

fn private(meta: &fs::Metadata) -> StorageResult<()> {
    if !meta.is_file()
        || meta.uid() != unsafe { libc::geteuid() }
        || meta.mode() & 0o077 != 0
        || meta.nlink() != 1
    {
        return Err(invalid("byte source private regular file required"));
    }
    Ok(())
}

impl ByteBackupCapture {
    pub fn reader(&self, index: u64) -> StorageResult<ByteBackupReader> {
        let state = self.lock()?;
        if !state.ready
            || state.terminal.is_some()
            || state.local_cancelled
            || state.released
            || !self.inner.available()
        {
            return Err(invalid("byte source reader phase"));
        }
        self.require_export(&state)?;
        if self.inner.readers.load(Ordering::Acquire)
            >= lastdb_model::storage_policy::BACKUP_RESOURCES.concurrent_streams
        {
            return Err(invalid("byte source stream admission"));
        }
        let object = self.inner.object(&state, index)?;
        let payload = object
            .ciphertext
            .as_ref()
            .map(|name| input(&self.inner.store.root.join(name)))
            .transpose()?;
        if let Some(file) = &payload {
            if file.metadata()?.len() != object.input.object.bytes.get() {
                return Err(invalid("byte source encrypted metadata length"));
            }
        }
        self.inner.readers.fetch_add(1, Ordering::AcqRel);
        Ok(ByteBackupReader {
            inner: Arc::clone(&self.inner),
            object,
            payload,
            reader: None,
            position: 0,
            remaining: 0,
            bytes: 0,
            hash: Sha256::new(),
            failed: false,
        })
    }
}

pub struct ByteBackupReader {
    inner: Arc<Inner>,
    object: ObjectRecord,
    payload: Option<File>,
    reader: Option<File>,
    position: usize,
    remaining: u64,
    bytes: u64,
    hash: Sha256,
    failed: bool,
}

impl ByteBackupReader {
    fn open_segment(&mut self, part: &Segment) -> io::Result<()> {
        if self
            .inner
            .destination(Path::new(part.path.as_str()))
            .map_err(|_| io::Error::other("byte source destination"))?
            != self.object.input.object.destination
        {
            return Err(io::Error::other("byte source destination differs"));
        }
        let mut reader = self
            .inner
            .file_reader(Path::new(part.path.as_str()))
            .map_err(|_| io::Error::other("byte source generation read"))?;
        reader.seek(SeekFrom::Start(part.offset))?;
        self.reader = Some(reader);
        self.remaining = part.bytes;
        Ok(())
    }

    fn next(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.failed {
            return Err(io::Error::other("byte source reader failed"));
        }
        if buffer.is_empty() {
            return Ok(0);
        }
        let maximum = buffer
            .len()
            .min(lastdb_model::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes as usize);
        let count = if let Some(payload) = &mut self.payload {
            payload.read(&mut buffer[..maximum])?
        } else {
            loop {
                if self.position == self.object.segments.len() {
                    break 0;
                }
                if self.reader.is_none() {
                    self.open_segment(&self.object.segments[self.position].clone())?;
                }
                if self.remaining == 0 {
                    self.reader = None;
                    self.position += 1;
                    continue;
                }
                let maximum = maximum.min(self.remaining as usize);
                let reader = self
                    .reader
                    .as_mut()
                    .ok_or_else(|| io::Error::other("byte native reader missing"))?;
                let count = reader.read(&mut buffer[..maximum])?;
                if count == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "byte native source truncated",
                    ));
                }
                self.remaining -= count as u64;
                break count;
            }
        };
        self.bytes = self
            .bytes
            .checked_add(count as u64)
            .ok_or_else(|| io::Error::other("byte source stream overflow"))?;
        self.hash.update(&buffer[..count]);
        let expected = self.object.input.object.bytes.get();
        if self.bytes > expected || (count == 0 && self.bytes != expected) {
            return Err(io::Error::other("byte source exact stream length"));
        }
        if self.bytes == expected
            && lastdb_model::hex::hex_lower(self.hash.clone().finalize())
                != self.object.input.object.sha256.as_str()
        {
            return Err(io::Error::other("byte source stream checksum"));
        }
        Ok(count)
    }
}

impl Read for ByteBackupReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let result = self.next(buffer);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
}

impl Drop for ByteBackupReader {
    fn drop(&mut self) {
        self.reader = None;
        self.payload = None;
        self.inner.readers.fetch_sub(1, Ordering::AcqRel);
    }
}
