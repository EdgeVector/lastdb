use super::{Registration, Result, ShareCacheError};
use crate::byte_cloud_backup::connection::files::Directory;
use cloud_backup_protocol::Id;
use fold_db::crypto::at_rest::{open_at_rest_bounded, seal_at_rest_raw};
use std::fs::{self, File, OpenOptions};
use std::io::ErrorKind;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use zeroize::Zeroizing;

const STATE_FILE: &str = "registration";
const STATE_BYTES: u64 = 16 * 1024;

pub(super) struct Entry {
    path: PathBuf,
    directory: Directory,
    key: Zeroizing<[u8; 32]>,
}

#[derive(Clone, Copy)]
pub(super) enum Lock {
    Writer,
    Pointer,
    Slot(u8),
}

type LoadedRegistration = (Option<Registration>, Option<Zeroizing<Vec<u8>>>);

pub(super) fn cache_root(path: &Path, identity_home: &Path) -> Result<PathBuf> {
    let invalid = || ShareCacheError::Invalid("directory");
    if !path.is_absolute() {
        return Err(invalid());
    }
    let parent = path.parent().ok_or_else(invalid)?.canonicalize()?;
    let path = parent.join(path.file_name().ok_or_else(invalid)?);
    let identity = identity_home.canonicalize()?;
    if identity.starts_with(&path) {
        return Err(invalid());
    }
    crate::offline_home::refuse_primary(&path).map_err(|_| invalid())?;
    Directory::open(&parent).map_err(|_| invalid())?;
    private_directory(&path, true)?;
    Ok(path)
}

impl Entry {
    pub(super) fn open(
        root: &Path,
        handle: &Id,
        key: Zeroizing<[u8; 32]>,
        create: bool,
    ) -> Result<Self> {
        let path = root.join(handle.as_str());
        private_directory(root, false)?;
        let directory = private_directory(&path, create)?;
        Ok(Self {
            path,
            directory,
            key,
        })
    }

    pub(super) fn load(&self) -> Result<LoadedRegistration> {
        let invalid = || ShareCacheError::Invalid("registration receipt");
        let previous = self.directory.read(STATE_FILE).map_err(|_| invalid())?;
        let Some(bytes) = &previous else {
            return Ok((None, None));
        };
        let decoded = Zeroizing::new(
            open_at_rest_bounded(&self.key, bytes, STATE_BYTES).map_err(|_| invalid())?,
        );
        let registration: Registration = serde_json::from_slice(&decoded).map_err(|_| invalid())?;
        if registration.version != 1
            || registration
                .active
                .as_ref()
                .is_some_and(|active| active.slot > 1 || active.snapshot.validate().is_err())
        {
            return Err(invalid());
        }
        Ok((Some(registration), previous))
    }

    pub(super) fn publish(&self, previous: Option<&[u8]>, state: &Registration) -> Result<Vec<u8>> {
        let invalid = || ShareCacheError::Invalid("registration publication");
        let encoded = Zeroizing::new(serde_json::to_vec(state).map_err(|_| invalid())?);
        let ciphertext = seal_at_rest_raw(&self.key, &encoded).map_err(|_| invalid())?;
        self.directory
            .publish(STATE_FILE, previous, &ciphertext)
            .map_err(|_| invalid())?;
        Ok(ciphertext)
    }

    pub(super) fn lock(&self, lock: Lock, exclusive: bool) -> Result<File> {
        let pointer = matches!(lock, Lock::Pointer);
        let name = match lock {
            Lock::Writer => "refresh.lock",
            Lock::Pointer => "pointer.lock",
            Lock::Slot(0) => "snapshot-0.lock",
            Lock::Slot(1) => "snapshot-1.lock",
            Lock::Slot(_) => return Err(ShareCacheError::Invalid("snapshot slot")),
        };
        self.directory
            .check()
            .map_err(|_| ShareCacheError::Invalid("registration directory"))?;
        let path = self.path.join(name);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)?;
        let meta = file.metadata()?;
        if !meta.is_file()
            || meta.uid() != unsafe { libc::geteuid() }
            || meta.mode() & 0o7777 != 0o600
            || meta.nlink() != 1
        {
            return Err(ShareCacheError::Invalid("lock file"));
        }
        // The pointer gate only covers small local operations and never a fetch.
        if pointer {
            if exclusive {
                file.lock()?;
            } else {
                file.lock_shared()?;
            }
        } else if exclusive {
            file.try_lock().map_err(|_| ShareCacheError::Busy)?;
        } else {
            file.try_lock_shared().map_err(|_| ShareCacheError::Busy)?;
        }
        Ok(file)
    }

    fn slot(&self, slot: u8) -> Result<PathBuf> {
        let name = match slot {
            0 => "snapshot-0",
            1 => "snapshot-1",
            _ => return Err(ShareCacheError::Invalid("snapshot slot")),
        };
        Ok(self.path.join(name))
    }

    pub(super) fn snapshot_data(&self, slot: u8) -> Result<PathBuf> {
        let slot = self.slot(slot)?;
        private_directory(&slot, false)?;
        Ok(slot.join("data"))
    }

    pub(super) fn reset_slot(&self, slot: u8) -> Result<PathBuf> {
        let path = self.slot(slot)?;
        match private_directory(&path, false) {
            Ok(_) => fs::remove_dir_all(&path)?,
            Err(ShareCacheError::Unavailable) => {}
            Err(error) => return Err(error),
        }
        private_directory(&path, true)?;
        Ok(path)
    }
}

pub(super) fn finish_slot(slot: &Path, data: &Path, workspace: &Path) -> Result<()> {
    fs::rename(data, slot.join("data"))?;
    fs::remove_dir_all(workspace)?;
    File::open(slot)?.sync_all()?;
    Ok(())
}

fn private_directory(path: &Path, create: bool) -> Result<Directory> {
    let mut metadata = fs::symlink_metadata(path);
    if create
        && metadata
            .as_ref()
            .is_err_and(|e| e.kind() == ErrorKind::NotFound)
    {
        match fs::DirBuilder::new().mode(0o700).create(path) {
            Ok(()) => {
                File::open(path.parent().ok_or(ShareCacheError::Invalid("parent"))?)?.sync_all()?;
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        metadata = fs::symlink_metadata(path);
    }
    let meta = match metadata {
        Ok(meta) => meta,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Err(ShareCacheError::Unavailable);
        }
        Err(error) => return Err(error.into()),
    };
    if !meta.is_dir() || meta.uid() != unsafe { libc::geteuid() } || meta.mode() & 0o7777 != 0o700 {
        return Err(ShareCacheError::Invalid("private directory"));
    }
    Directory::open(path).map_err(|_| ShareCacheError::Invalid("private directory"))
}
