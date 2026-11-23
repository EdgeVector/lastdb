//! Recipient-owned registration store for received Share databases.

use crate::offline_home::Directory;
use crate::registered_share::ManagedRegistration;
use crate::share_invitation::ShareRecipient;
use cloud_backup_protocol::v2::share_access::ShareEnvironment;
use cloud_backup_protocol::{v2::Stream, Digest, Id};
use fold_db::crypto::at_rest::{open_at_rest_bounded, seal_at_rest_raw};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::ErrorKind;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use zeroize::{Zeroize, Zeroizing};

pub(crate) const MANAGED_SHARE_DIRECTORY: &str = "received-shares-v1";
const STATE_FILE: &str = "registration";
const STATE_BYTES: u64 = 16 * 1024;

/// A stable node-owned registry for received shares.
pub(crate) struct ShareCache {
    pub(crate) root: PathBuf,
    pub(crate) recipient: ShareRecipient,
}

#[derive(Debug)]
pub(crate) enum ShareCacheError {
    Busy,
    Unavailable,
    Invalid(&'static str),
    Io,
}

pub(crate) type Result<T> = std::result::Result<T, ShareCacheError>;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Registration {
    version: u8,
    pub(crate) publisher: Digest,
    pub(crate) stream: Stream,
    pub(crate) environment: ShareEnvironment,
    content_key: [u8; 32],
    #[serde(rename = "active", default, skip_serializing)]
    _retired_active: serde::de::IgnoredAny,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) managed: Option<ManagedRegistration>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.content_key.zeroize();
    }
}

pub(crate) struct Entry {
    path: PathBuf,
    directory: Directory,
    key: Zeroizing<[u8; 32]>,
}

#[derive(Clone, Copy)]
pub(crate) enum Lock {
    Writer,
    PointerRead,
    PointerWrite,
}

type LoadedRegistration = (Option<Registration>, Option<Zeroizing<Vec<u8>>>);

impl Entry {
    fn open(root: &Path, handle: &Id, key: Zeroizing<[u8; 32]>, create: bool) -> Result<Self> {
        let path = root.join(handle.as_str());
        private_directory(root, false)?;
        let directory = private_directory(&path, create)?;
        Ok(Self {
            path,
            directory,
            key,
        })
    }

    pub(crate) fn load(&self) -> Result<LoadedRegistration> {
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

    fn publish(&self, previous: Option<&[u8]>, state: &Registration) -> Result<Vec<u8>> {
        let invalid = || ShareCacheError::Invalid("registration publication");
        let encoded = Zeroizing::new(serde_json::to_vec(state).map_err(|_| invalid())?);
        let ciphertext = seal_at_rest_raw(&self.key, &encoded).map_err(|_| invalid())?;
        self.directory
            .publish(STATE_FILE, previous, &ciphertext)
            .map_err(|_| invalid())?;
        Ok(ciphertext)
    }

    pub(crate) fn lock(&self, lock: Lock) -> Result<File> {
        let name = match lock {
            Lock::Writer => "refresh.lock",
            Lock::PointerRead | Lock::PointerWrite => "pointer.lock",
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
        if !crate::offline_home::is_private_file(&meta) || meta.mode() & 0o7777 != 0o600 {
            return Err(ShareCacheError::Invalid("lock file"));
        }
        match lock {
            Lock::PointerWrite => file.lock()?,
            Lock::PointerRead => file.lock_shared()?,
            Lock::Writer => file.try_lock().map_err(|_| ShareCacheError::Busy)?,
        }
        Ok(file)
    }
}

pub(crate) fn private_directory(path: &Path, create: bool) -> Result<Directory> {
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

impl ShareCache {
    pub(crate) fn entry(&self, handle: &Id, create: bool) -> Result<Entry> {
        Entry::open(
            &self.root,
            handle,
            self.recipient
                .cache_key(handle)
                .map_err(|_| ShareCacheError::Invalid("recipient identity"))?,
            create,
        )
    }

    pub(crate) fn require_management(&self, registration: &Registration) -> Result<()> {
        if registration.managed.is_none() {
            return Err(ShareCacheError::Invalid("registration owner"));
        }
        Ok(())
    }
}

impl Registration {
    pub(crate) fn with_link(
        publisher: Digest,
        stream: Stream,
        environment: ShareEnvironment,
        content_key: [u8; 32],
        managed: ManagedRegistration,
    ) -> Result<Self> {
        if content_key == [0; 32] || !managed.link {
            return Err(ShareCacheError::Invalid("content key"));
        }
        Ok(Self {
            version: 1,
            publisher,
            stream,
            environment,
            content_key,
            _retired_active: serde::de::IgnoredAny,
            managed: Some(managed),
        })
    }

    /// Stored content key of an open-link registration.
    pub(crate) fn link_key(&self) -> Result<Zeroizing<[u8; 32]>> {
        if self.content_key == [0; 32] || !self.managed.as_ref().is_some_and(|state| state.link) {
            return Err(ShareCacheError::Invalid("content key"));
        }
        Ok(Zeroizing::new(self.content_key))
    }

    pub(crate) fn accepts_link_refresh(
        &self,
        publisher: &Digest,
        stream: &Stream,
        environment: ShareEnvironment,
        key: &[u8; 32],
    ) -> bool {
        &self.publisher == publisher
            && &self.stream == stream
            && self.environment == environment
            && self.content_key == *key
    }
}

pub(crate) fn publish(
    entry: &Entry,
    previous: Option<&[u8]>,
    registration: &Registration,
) -> Result<Vec<u8>> {
    let _gate = entry.lock(Lock::PointerWrite)?;
    entry.publish(previous, registration)
}

impl std::fmt::Display for ShareCacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy => f.write_str("share cache is busy"),
            Self::Unavailable => f.write_str("no verified snapshot is available for this share"),
            Self::Invalid(field) => write!(f, "invalid share cache {field}"),
            Self::Io => f.write_str("share cache disk operation failed"),
        }
    }
}

impl std::error::Error for ShareCacheError {}

impl From<std::io::Error> for ShareCacheError {
    fn from(_: std::io::Error) -> Self {
        Self::Io
    }
}
