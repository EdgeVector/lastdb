//! Private disk output with free-space admission before each bounded write.
use super::{invalid, StorageResult};
use laststore::space::{reserve_space, SpacePolicy, SpaceReservation};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

pub(crate) struct Scratch {
    file: File,
    root: PathBuf,
    policy: SpacePolicy,
    reservation: Option<SpaceReservation>,
    bytes: u64,
}

impl Scratch {
    pub(crate) fn create(path: &Path, policy: &SpacePolicy) -> StorageResult<Self> {
        let root = path
            .parent()
            .ok_or_else(|| invalid("native CAS scratch parent"))?;
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?;
        Ok(Self {
            file,
            root: root.to_path_buf(),
            policy: policy.clone(),
            reservation: None,
            bytes: 0,
        })
    }

    pub(crate) fn finish(mut self) -> StorageResult<File> {
        self.file.sync_all()?;
        self.file.rewind()?;
        Ok(self.file)
    }
}

impl Write for Scratch {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = bytes
            .len()
            .min(lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes as usize);
        let total = self
            .bytes
            .checked_add(count as u64)
            .ok_or_else(|| io::Error::other("native CAS scratch length overflow"))?;
        let extra = match &self.reservation {
            Some(previous) => previous.reserve_extension(&self.policy, total),
            None => reserve_space(&self.root, &self.policy, total),
        }
        .map_err(|error| io::Error::other(error.to_string()))?;
        match &mut self.reservation {
            Some(previous) => previous.absorb(extra),
            None => self.reservation = Some(extra),
        }
        self.file.write_all(&bytes[..count])?;
        self.bytes = total;
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

pub(super) fn input(path: &Path) -> StorageResult<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid("native CAS scratch type"));
    }
    Ok(file)
}

pub(super) fn remove(paths: &[&Path]) -> StorageResult<()> {
    for path in paths {
        match fs::symlink_metadata(path) {
            Ok(meta) if meta.is_file() => fs::remove_file(path)?,
            Ok(_) => return Err(invalid("native CAS scratch type")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    if let Some(parent) = paths.first().and_then(|path| path.parent()) {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}
