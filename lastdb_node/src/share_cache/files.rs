use super::{Registration, Result, ShareCacheError};
use crate::offline_home::Directory;
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
}

type LoadedRegistration = (Option<Registration>, Option<Zeroizing<Vec<u8>>>);

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
        if registration.version != 1 {
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
}

pub(super) fn private_directory(path: &Path, create: bool) -> Result<Directory> {
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
