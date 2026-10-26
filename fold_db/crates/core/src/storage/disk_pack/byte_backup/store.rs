//! Bounded private sidecar files. Atomic names never replace native payload bytes.
use super::{invalid, StorageResult};
use serde::{de::DeserializeOwned, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const RECORD_MAX: u64 = 256 * 1024;
const MAX_FILES: usize = 2 * super::model::MAX_OBJECTS as usize + 16;
pub(super) struct Store {
    pub root: PathBuf,
    bytes: AtomicU64,
    coordinator: AtomicU64,
    policy: laststore::space::SpacePolicy,
    space_root: PathBuf,
}
impl Store {
    pub fn open(home: &Path, policy: &laststore::space::SpacePolicy) -> StorageResult<Self> {
        let root = home.join("cloud-byte-source-v2");
        match fs::DirBuilder::new().mode(0o700).create(&root) {
            Ok(()) => File::open(
                root.parent()
                    .ok_or_else(|| invalid("byte source directory parent"))?,
            )?
            .sync_all()?,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        let meta = fs::symlink_metadata(&root)?;
        if !meta.is_dir() || meta.uid() != unsafe { libc::geteuid() } || meta.mode() & 0o077 != 0 {
            return Err(invalid("byte source directory ownership"));
        }
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
        if total > lastdb_storage_policy::BACKUP_RESOURCES.durable_spool_bytes {
            return Err(invalid("byte source combined spool bound"));
        }
        Ok(())
    }
    pub fn exists(&self, name: &str) -> StorageResult<bool> {
        match fs::symlink_metadata(self.root.join(name)) {
            Ok(meta) => {
                private(&meta)?;
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
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
            lastdb_storage_policy::backup_transport::METADATA_PACK_BYTES + 29
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
        let previous = match fs::symlink_metadata(&path) {
            Ok(meta) => {
                private(&meta)?;
                meta.len()
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        let _space =
            laststore::space::reserve_space(&self.space_root, &self.policy, bytes.len() as u64)
                .map_err(super::super::map)?;
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
        match fs::symlink_metadata(path) {
            Ok(meta) => {
                private(&meta)?;
                fs::remove_file(path)?;
                self.bytes.fetch_sub(meta.len(), Ordering::AcqRel);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedValue {
    value: serde_json::Value,
    sha256: cloud_backup_protocol::Digest,
}
fn checksum(value: &serde_json::Value) -> StorageResult<cloud_backup_protocol::Digest> {
    let bytes = cloud_backup_protocol::v2::hash::canonical_bytes(value).map_err(super::protocol)?;
    Ok(cloud_backup_protocol::bytes_sha256(&bytes))
}
pub(super) fn input(path: &Path) -> StorageResult<File> {
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
