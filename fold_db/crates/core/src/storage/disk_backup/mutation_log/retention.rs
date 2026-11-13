//! Exact private metadata store and release after an accepted recovery point.
use super::*;
use serde::{de::DeserializeOwned, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub(super) const MAX_JSON: u64 = cloud_backup_protocol::v2::limits::CONTROL_JSON_MAX_BYTES;
pub(super) const MAX_EPOCHS: usize = 32;
pub(super) const MAX_ENTRIES: u64 = 131_072;

pub(super) fn epoch(root: &Path, id: &Id) -> PathBuf {
    root.join(id.as_str())
}

pub(super) fn entry(root: &Path, id: &Id, sequence: u64) -> StorageResult<PathBuf> {
    if sequence == 0 || sequence > MAX_ENTRIES {
        return Err(invalid("mutation log sequence resource bound"));
    }
    Ok(epoch(root, id).join(format!("{sequence:020}")))
}

pub(super) fn directory(path: &Path) -> StorageResult<()> {
    let info = fs::symlink_metadata(path)?;
    if !info.is_dir()
        || info.uid() != unsafe { libc::geteuid() }
        || info.mode() & 0o077 != 0
        || path.canonicalize()? != path
    {
        return Err(invalid("mutation log private directory"));
    }
    Ok(())
}

pub(super) fn create(path: &Path) -> StorageResult<()> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid("mutation log directory parent"))?;
    directory(parent)?;
    fs::DirBuilder::new().mode(0o700).create(path)?;
    directory(path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

pub(super) fn read<T: DeserializeOwned>(path: &Path) -> StorageResult<Option<T>> {
    match fs::symlink_metadata(path) {
        Ok(info) => private(&info)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    directory(
        path.parent()
            .ok_or_else(|| invalid("mutation log metadata parent"))?,
    )?;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let before = file.metadata()?;
    private(&before)?;
    let mut bytes = Vec::new();
    file.take(MAX_JSON + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != before.len() {
        return Err(invalid("mutation log metadata length"));
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| invalid("mutation log metadata format"))
}

pub(super) fn write<T: Serialize>(path: &Path, value: &T) -> StorageResult<()> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid("mutation log metadata parent"))?;
    directory(parent)?;
    let bytes = cloud_backup_protocol::v2::hash::canonical_bytes(value)
        .map_err(|_| invalid("mutation log metadata encoding"))?;
    if bytes.len() as u64 > MAX_JSON {
        return Err(invalid("mutation log metadata resource bound"));
    }
    for candidate in [path.to_owned(), path.with_extension("pending")] {
        match fs::symlink_metadata(candidate) {
            Ok(info) => private(&info)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    let pending = path.with_extension("pending");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(&pending)?;
    private(&file.metadata()?)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(pending, path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn private(info: &fs::Metadata) -> StorageResult<()> {
    if !info.is_file()
        || info.uid() != unsafe { libc::geteuid() }
        || info.mode() & 0o077 != 0
        || info.nlink() != 1
        || info.len() > MAX_JSON
    {
        return Err(invalid("mutation log private metadata"));
    }
    Ok(())
}

pub(super) fn remove_metadata(path: &Path) -> StorageResult<()> {
    match fs::symlink_metadata(path) {
        Ok(info) => private(&info)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    directory(
        path.parent()
            .ok_or_else(|| invalid("mutation log cleanup parent"))?,
    )?;
    fs::remove_file(path)?;
    Ok(())
}

pub(super) fn remove_part(path: &Path, bytes: u64) -> StorageResult<()> {
    match fs::symlink_metadata(path) {
        Ok(info) => {
            if !info.is_file()
                || info.uid() != unsafe { libc::geteuid() }
                || info.mode() & 0o077 != 0
                || info.len() != bytes
            {
                return Err(invalid("mutation log cleanup file identity"));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    directory(
        path.parent()
            .ok_or_else(|| invalid("mutation log cleanup parent"))?,
    )?;
    fs::remove_file(path)?;
    Ok(())
}

pub(super) fn remove_empty(path: &Path) -> StorageResult<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => directory(path)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    fs::remove_dir(path)?;
    Ok(())
}

fn read_entry(log: &MutationLog, epoch: &Id, sequence: u64) -> StorageResult<LogEntry> {
    read(&entry(&log.root, epoch, sequence)?.join("entry.json"))?
        .ok_or_else(|| invalid("complete mutation log entry is absent"))
}

fn parts(directory: &Path, entry: &LogEntry) -> StorageResult<()> {
    for part in &entry.parts {
        let parent = directory.join(part.role.directory());
        remove_part(
            &parent.join(format!("{}.ldrun", part.run_id.as_str())),
            part.bytes,
        )?;
        if parent.try_exists()? {
            File::open(&parent)?.sync_all()?;
        }
    }
    Ok(())
}

impl MutationLog {
    pub fn acknowledge(&self, position: &LogPosition) -> StorageResult<()> {
        let (released, end) = {
            let cache = self.lock()?;
            let index = cache
                .index
                .as_ref()
                .ok_or_else(|| invalid("mutation log owner is absent"))?;
            if !index.epochs.contains(&position.epoch) {
                return Err(invalid("mutation log acknowledged epoch"));
            }
            let mut state = self.state(&position.epoch)?;
            let mut cursor = state.released.clone();
            if position.sequence <= cursor.sequence {
                return if position == &cursor {
                    Ok(())
                } else {
                    Err(invalid("mutation log acknowledgement regressed"))
                };
            }
            if position.sequence > state.head.sequence
                || position.sequence - cursor.sequence > MAX_ENTRIES
            {
                return Err(invalid("mutation log acknowledgement bound"));
            }
            let released = cursor.clone();
            for sequence in cursor.sequence + 1..=position.sequence {
                cursor = read_entry(self, &position.epoch, sequence)?.position_after(&cursor)?;
            }
            if cursor != *position {
                return Err(invalid("mutation log exact acknowledgement"));
            }
            state.acknowledged = state.acknowledged.max(position.sequence);
            write(
                &epoch(&self.root, &position.epoch).join("state.json"),
                &state,
            )?;
            (released, cursor)
        };
        let mut bytes = 0u64;
        for sequence in released.sequence + 1..=end.sequence {
            let log_entry = read_entry(self, &end.epoch, sequence)?;
            parts(&entry(&self.root, &end.epoch, sequence)?, &log_entry)?;
            bytes = bytes
                .checked_add(log_entry.bytes()?)
                .ok_or_else(|| invalid("mutation log released size overflow"))?;
        }
        let _owner = self.lock()?;
        let mut state = self.state(&end.epoch)?;
        if state.released != released || state.acknowledged < end.sequence {
            return Err(invalid("mutation log release identity changed"));
        }
        state.retained_bytes = state
            .retained_bytes
            .checked_sub(bytes)
            .ok_or_else(|| invalid("mutation log retained release counter"))?;
        state.released = end;
        write(
            &epoch(&self.root, &state.anchor.epoch).join("state.json"),
            &state,
        )
    }
}
