use super::{invalid, NativeBackupCapture, StorageResult};
use cloud_backup_protocol::{bytes_sha256, canonical_bytes, Digest};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub(super) const CONTROL_MAX: u64 = 256 * 1024;
pub(super) const FEED_MAX: u64 = 2 * 1024 * 1024;
const FILES_MAX: u64 = 3 * super::receipts::IDS_MAX + 8192;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Checked<T> {
    sha256: Digest,
    value: T,
}

pub(super) struct Store {
    pub root: PathBuf,
    bytes: u64,
}
impl Store {
    pub fn open(root: PathBuf) -> StorageResult<Self> {
        match fs::DirBuilder::new().mode(0o700).create(&root) {
            Ok(()) => {
                File::open(root.parent().ok_or_else(|| invalid("manifest parent"))?)?.sync_all()?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        if !fs::symlink_metadata(&root)?.is_dir() {
            return Err(invalid("manifest directory type"));
        }
        let mut bytes = 0u64;
        let mut count = 0;
        for entry in fs::read_dir(&root)? {
            let path = entry?.path();
            count += 1;
            if count > FILES_MAX
                || !valid_name(
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or(""),
                )
            {
                return Err(invalid("manifest directory member"));
            }
            bytes = bytes
                .checked_add(size(&path)?)
                .ok_or_else(|| invalid("manifest spool overflow"))?;
        }
        Ok(Self { root, bytes })
    }
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
    pub fn resize_stream(&mut self, old: u64, current: u64) -> StorageResult<()> {
        self.bytes = self
            .bytes
            .checked_sub(old)
            .and_then(|bytes| bytes.checked_add(current))
            .ok_or_else(|| invalid("manifest stream spool count"))?;
        Ok(())
    }
    pub fn exists(&self, name: &str) -> StorageResult<bool> {
        match fs::symlink_metadata(self.root.join(name)) {
            Ok(meta) if meta.is_file() => Ok(true),
            Ok(_) => Err(invalid("manifest member type")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }
    pub fn read<T: DeserializeOwned + Serialize>(
        &self,
        name: &str,
        maximum: u64,
    ) -> StorageResult<T> {
        let raw = self.raw(name, maximum + 1024)?;
        let checked: Checked<T> =
            serde_json::from_slice(&raw).map_err(|_| invalid("manifest local metadata"))?;
        if bytes_sha256(&canonical_bytes(&checked.value).map_err(super::protocol)?)
            != checked.sha256
        {
            return Err(invalid("manifest local checksum"));
        }
        Ok(checked.value)
    }
    pub fn write<T: Serialize>(
        &mut self,
        capture: &NativeBackupCapture,
        name: &str,
        value: &T,
        maximum: u64,
    ) -> StorageResult<()> {
        let canonical = canonical_bytes(value).map_err(super::protocol)?;
        if canonical.len() as u64 > maximum {
            return Err(invalid("manifest local value bound"));
        }
        let bytes = serde_json::to_vec(&Checked {
            sha256: bytes_sha256(&canonical),
            value,
        })
        .map_err(|_| invalid("manifest local encode"))?;
        self.write_raw(capture, name, &bytes)
    }
    pub fn immutable<T: DeserializeOwned + Serialize + PartialEq>(
        &mut self,
        capture: &NativeBackupCapture,
        name: &str,
        value: &T,
        maximum: u64,
    ) -> StorageResult<()> {
        if self.exists(name)? {
            if self.read::<T>(name, maximum)? != *value {
                return Err(invalid("frozen manifest input changed"));
            }
            self.barrier(name)
        } else {
            self.write(capture, name, value, maximum)
        }
    }
    pub fn raw(&self, name: &str, maximum: u64) -> StorageResult<Vec<u8>> {
        let file = input(&self.root.join(name))?;
        if file.metadata()?.len() > maximum {
            return Err(invalid("manifest member byte bound"));
        }
        let mut bytes = Vec::new();
        file.take(maximum + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > maximum {
            return Err(invalid("manifest member grew"));
        }
        Ok(bytes)
    }
    pub fn write_raw(
        &mut self,
        capture: &NativeBackupCapture,
        name: &str,
        bytes: &[u8],
    ) -> StorageResult<()> {
        if !valid_name(name) {
            return Err(invalid("manifest output name"));
        }
        let path = self.root.join(name);
        let pending = path.with_extension("pending");
        let old = size(&path)?;
        let old_pending = size(&pending)?;
        if old_pending != 0 || pending.try_exists()? {
            fs::remove_file(&pending)?;
            File::open(&self.root)?.sync_all()?;
        }
        self.bytes -= old_pending;
        capture.admit_assembly_metadata(
            self.bytes
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| invalid("manifest spool overflow"))?,
        )?;
        let _space = capture.reserve_assembly_output(bytes.len() as u64 + 65536)?;
        let result = (|| {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&pending)?;
            file.write_all(bytes)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&pending, &path)?;
            File::open(&self.root)?.sync_all()?;
            Ok(())
        })();
        self.bytes = self
            .bytes
            .checked_sub(old)
            .and_then(|sum| sum.checked_add(size(&path).ok()?))
            .and_then(|sum| sum.checked_add(size(&pending).ok()?))
            .ok_or_else(|| invalid("manifest spool count differs"))?;
        capture.track_assembly_metadata(self.bytes)?;
        result
    }
    pub fn barrier(&self, name: &str) -> StorageResult<()> {
        input(&self.root.join(name))?.sync_all()?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }
}

pub(super) fn input(path: &Path) -> StorageResult<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid("manifest input type"));
    }
    Ok(file)
}
fn size(path: &Path) -> StorageResult<u64> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.is_file() => Ok(meta.len()),
        Ok(_) => Err(invalid("manifest member type")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
}
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'.')
        })
        && [
            "state.",
            "native-",
            "external-",
            "receipt-",
            "manifest-",
            "reference-",
            "commit.",
        ]
        .iter()
        .any(|prefix| name.starts_with(prefix))
}
pub(super) fn cleanup(root: &Path) -> StorageResult<()> {
    if !root.try_exists()? {
        File::open(root.parent().ok_or_else(|| invalid("manifest parent"))?)?.sync_all()?;
        return Ok(());
    }
    let store = Store::open(root.to_path_buf())?;
    for entry in fs::read_dir(&store.root)? {
        let path = entry?.path();
        size(&path)?;
        fs::remove_file(path)?;
    }
    File::open(&store.root)?.sync_all()?;
    fs::remove_dir(&store.root)?;
    File::open(root.parent().ok_or_else(|| invalid("manifest parent"))?)?.sync_all()?;
    Ok(())
}
