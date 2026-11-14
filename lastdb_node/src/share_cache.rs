//! Recipient-owned registration store for received Share databases.
mod files;
mod managed;

use crate::cloud_backup_client::byte_backup::ShareEnvironment;
use crate::share_invitation::{OpenedShareInvitation, ShareRecipient};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use cloud_backup_protocol::{v2::Stream, Digest, Id};
use files::{Entry, Lock};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use zeroize::Zeroize;

pub use cloud_backup_protocol::v2::publication::Head as ShareSnapshot;

pub const MANAGED_SHARE_DIRECTORY: &str = "received-shares-v1";

/// A stable node-owned registry for received shares.
pub struct ShareCache {
    root: PathBuf,
    recipient: ShareRecipient,
}

#[derive(Debug)]
pub enum ShareCacheError {
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
    #[serde(default)]
    active: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    managed: Option<managed::ManagedRegistration>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.content_key.zeroize();
    }
}

impl ShareCache {
    /// Re-open the invitation saved by the managed share bootstrap.
    ///
    /// The returned publication key is the one registration verified.
    /// A signed log skips bearer expiry. This does not fetch a snapshot
    /// and does not create a database directory.
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
            active: None,
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
