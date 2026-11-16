//! Recipient-owned registration store for received Share databases.
mod managed;

use crate::cloud_backup_client::storage_transfer::ShareEnvironment;
use crate::offline_home::Directory;
use crate::share_invitation::{OpenedShareInvitation, ShareRecipient};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
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
    root: PathBuf,
    recipient: ShareRecipient,
}

#[derive(Debug)]
pub(crate) enum ShareCacheError {
    Busy,
    Unavailable,
    Invalid(&'static str),
    Invitation(crate::share_invitation::ShareInvitationError),
    Io(std::io::Error),
}

type Result<T> = std::result::Result<T, ShareCacheError>;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Registration {
    version: u8,
    publisher: Digest,
    stream: Stream,
    environment: ShareEnvironment,
    content_key: [u8; 32],
    invitation: String,
    #[serde(rename = "active", default, skip_serializing)]
    _retired_active: serde::de::IgnoredAny,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    managed: Option<managed::ManagedRegistration>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.content_key.zeroize();
    }
}

struct Entry {
    path: PathBuf,
    directory: Directory,
    key: Zeroizing<[u8; 32]>,
}

#[derive(Clone, Copy)]
enum Lock {
    Writer,
    Pointer,
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

    fn load(&self) -> Result<LoadedRegistration> {
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

    fn lock(&self, lock: Lock, exclusive: bool) -> Result<File> {
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

impl ShareCache {
    pub(crate) fn open_verified(&self, id: &Id) -> Result<OpenedShareInvitation> {
        let entry = self.entry(id, false)?;
        let _pointer = entry.lock(Lock::Pointer, false)?;
        let (registration, _) = entry.load()?;
        let registration = registration.ok_or(ShareCacheError::Unavailable)?;
        self.require_management(&registration)?;
        let invitation = URL_SAFE_NO_PAD
            .decode(registration.invitation.as_bytes())
            .map_err(|_| ShareCacheError::Invalid("stored invitation"))?;
        let signed_log = registration
            .managed
            .as_ref()
            .is_some_and(|state| state.signed_log);
        let opened = if signed_log {
            self.recipient
                .open_stored(&invitation, &registration.publisher)?
        } else {
            self.recipient.open(&invitation, &registration.publisher)?
        };
        registration.require_binding(&opened, &registration.publisher)?;
        if opened.permission.stream().stream_id.as_str() != id.as_str() {
            return Err(ShareCacheError::Invalid("stored invitation"));
        }
        Ok(opened)
    }

    fn entry(&self, handle: &Id, create: bool) -> Result<Entry> {
        Entry::open(
            &self.root,
            handle,
            self.recipient.cache_key(handle)?,
            create,
        )
    }

    fn require_management(&self, registration: &Registration) -> Result<()> {
        if registration.managed.is_none() {
            return Err(ShareCacheError::Invalid("registration owner"));
        }
        Ok(())
    }
}

impl Registration {
    fn with_invitation(
        registration: Option<Self>,
        opened: &OpenedShareInvitation,
        publisher: &Digest,
        invitation: &[u8],
    ) -> Result<Self> {
        let mut registration = registration.unwrap_or_else(|| Self {
            version: 1,
            publisher: publisher.clone(),
            stream: opened.permission.stream().clone(),
            environment: opened.permission.environment(),
            content_key: *opened.publication_key,
            invitation: String::new(),
            _retired_active: serde::de::IgnoredAny,
            managed: None,
        });
        registration.require_binding(opened, publisher)?;
        registration.invitation = URL_SAFE_NO_PAD.encode(invitation);
        Ok(registration)
    }

    fn require_binding(&self, opened: &OpenedShareInvitation, publisher: &Digest) -> Result<()> {
        if &self.publisher != publisher
            || &self.stream != opened.permission.stream()
            || self.environment != opened.permission.environment()
            || self.content_key != *opened.publication_key
        {
            return Err(ShareCacheError::Invalid(
                "invitation differs from this share handle",
            ));
        }
        Ok(())
    }
}

fn publish(entry: &Entry, previous: Option<&[u8]>, registration: &Registration) -> Result<Vec<u8>> {
    let _gate = entry.lock(Lock::Pointer, true)?;
    entry.publish(previous, registration)
}

impl std::fmt::Display for ShareCacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy => f.write_str("share cache is busy"),
            Self::Unavailable => f.write_str("no verified snapshot is available for this share"),
            Self::Invalid(field) => write!(f, "invalid share cache {field}"),
            Self::Invitation(error) => write!(f, "{error}"),
            Self::Io(_) => f.write_str("share cache disk operation failed"),
        }
    }
}

impl std::error::Error for ShareCacheError {}

impl From<std::io::Error> for ShareCacheError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<crate::share_invitation::ShareInvitationError> for ShareCacheError {
    fn from(error: crate::share_invitation::ShareInvitationError) -> Self {
        Self::Invitation(error)
    }
}
