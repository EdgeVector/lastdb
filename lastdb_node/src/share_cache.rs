//! Recipient-owned read-only recovery points of a registered Share.
mod files;
mod managed;
mod read;

use crate::byte_cloud_restore::{
    GuestReplayCache, RestoreError, RestoreSession, StagedRecord, StagedRecordQuery,
    MANAGED_SHARE_DIRECTORY as MANAGED_DIRECTORY,
};
use crate::cloud_backup_client::byte_backup::{ByteBackupReader, ShareEnvironment};
use crate::share_invitation::{OpenedShareInvitation, ShareRecipient};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use cloud_backup_protocol::{v2::Stream, Digest, Id, NativeRoot};
use files::{Entry, Lock};
use fold_db::storage::disk_pack::{mutation_log::LogPosition, run_restore::RunSnapshotReader};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::{Path, PathBuf};
use zeroize::{Zeroize, Zeroizing};

pub use cloud_backup_protocol::v2::publication::Head as ShareSnapshot;

/// A stable app-owned registry. Handles are chosen and retained by the app.
pub struct ShareCache {
    root: PathBuf,
    recipient: ShareRecipient,
    managed: bool,
}

/// Holds one verified version until the reader is dropped.
pub struct ShareReader {
    snapshot: ShareSnapshot,
    native: RunSnapshotReader,
    content_key: Zeroizing<[u8; 32]>,
    _slot: File,
}

#[derive(Debug)]
pub enum ShareCacheError {
    Busy,
    Unavailable,
    Invalid(&'static str),
    Invitation(crate::share_invitation::ShareInvitationError),
    Restore(RestoreError),
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
    active: Option<VerifiedSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    managed: Option<managed::ManagedRegistration>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.content_key.zeroize();
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VerifiedSnapshot {
    slot: u8,
    snapshot: ShareSnapshot,
    native_root: NativeRoot,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    through: Option<LogPosition>,
}

impl ShareCache {
    /// Use a private app-owned directory and an existing recipient identity.
    pub fn open(cache_root: &Path, identity_home: &Path) -> Result<Self> {
        let root = files::cache_root(cache_root, identity_home)?;
        let recipient = crate::share_publication::recipient(identity_home)
            .map_err(|_| ShareCacheError::Invalid("recipient identity"))?;
        Ok(Self {
            root,
            recipient,
            managed: false,
        })
    }

    /// Register before fetching. A transient fetch failure leaves the handle
    /// registered for refresh. Accepting a renewal preserves the verified copy.
    pub async fn accept(
        &self,
        handle: &Id,
        invitation: &[u8],
        publisher: &Digest,
    ) -> Result<ShareSnapshot> {
        let opened = self.recipient.open(invitation, publisher)?;
        let entry = self.entry(handle, true)?;
        let _writer = entry.lock(Lock::Writer, true)?;
        let (registration, previous) = entry.load()?;
        if let Some(registration) = &registration {
            self.require_management(registration)?;
        }
        let registration =
            Registration::with_invitation(registration, &opened, publisher, invitation)?;
        let stored = publish(
            &entry,
            previous.as_deref().map(Vec::as_slice),
            &registration,
        )?;
        self.fetch(&entry, opened, registration, &stored).await
    }

    /// Fetch and verify the current exact accepted point. Callers may
    /// retry this operation whenever the network returns.
    pub async fn refresh(&self, handle: &Id) -> Result<ShareSnapshot> {
        let entry = self.entry(handle, false)?;
        let _writer = entry.lock(Lock::Writer, true)?;
        let (registration, previous) = entry.load()?;
        let registration = registration.ok_or(ShareCacheError::Unavailable)?;
        self.require_management(&registration)?;
        let previous = previous.ok_or(ShareCacheError::Unavailable)?;
        let invitation = URL_SAFE_NO_PAD
            .decode(&registration.invitation)
            .map_err(|_| ShareCacheError::Invalid("stored invitation"))?;
        let opened = self.recipient.open(&invitation, &registration.publisher)?;
        registration.require_binding(&opened, &registration.publisher)?;
        self.fetch(&entry, opened, registration, &previous).await
    }

    /// Open the most recent verified point without network or invite checks.
    pub fn reader(&self, handle: &Id) -> Result<ShareReader> {
        let entry = self.entry(handle, false)?;
        let (registration, slot) = {
            let _gate = entry.lock(Lock::Pointer, false)?;
            let (registration, _) = entry.load()?;
            let registration = registration.ok_or(ShareCacheError::Unavailable)?;
            self.require_management(&registration)?;
            let active = registration
                .active
                .as_ref()
                .ok_or(ShareCacheError::Unavailable)?;
            let slot = entry.lock(Lock::Slot(active.slot), false)?;
            (registration, slot)
        };
        let active = registration
            .active
            .as_ref()
            .ok_or(ShareCacheError::Unavailable)?;
        let content_key = Zeroizing::new(registration.content_key);
        let native = RunSnapshotReader::open(
            &entry.snapshot_data(active.slot)?,
            &content_key,
            &active.native_root,
        )
        .map_err(RestoreError::native_proof)?;
        Ok(ShareReader {
            snapshot: active.snapshot.clone(),
            native,
            content_key,
            _slot: slot,
        })
    }

    /// Re-open the invitation saved by the managed share bootstrap.
    ///
    /// The returned publication key is the one `register_managed` verified.
    /// This does not fetch a snapshot and does not create a database directory.
    pub(crate) fn open_verified(&self, id: &Id) -> Result<OpenedShareInvitation> {
        let entry = self.entry(id, false)?;
        let _pointer = entry.lock(Lock::Pointer, false)?;
        let (registration, _) = entry.load()?;
        let registration = registration.ok_or(ShareCacheError::Unavailable)?;
        self.require_management(&registration)?;
        let invitation = URL_SAFE_NO_PAD
            .decode(registration.invitation.as_bytes())
            .map_err(|_| ShareCacheError::Invalid("stored invitation"))?;
        let opened = self.recipient.open(&invitation, &registration.publisher)?;
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
        if registration.managed.is_some() != self.managed {
            return Err(ShareCacheError::Invalid("registration owner"));
        }
        Ok(())
    }

    async fn fetch(
        &self,
        entry: &Entry,
        opened: OpenedShareInvitation,
        mut registration: Registration,
        previous: &[u8],
    ) -> Result<ShareSnapshot> {
        let reader = ByteBackupReader::guest_recovery(opened.permission)
            .map_err(|_| ShareCacheError::Invalid("guest reader configuration"))?;
        let accepted = RestoreSession::guest_accepted(&reader).await?;
        let snapshot = accepted.head();
        let slot = if let Some(active) = &registration.active {
            if snapshot == active.snapshot {
                return Ok(snapshot);
            }
            if snapshot.ordinal <= active.snapshot.ordinal {
                return Err(ShareCacheError::Invalid("guest head regressed or forked"));
            }
            1 - active.slot
        } else {
            0
        };
        let _slot = entry.lock(Lock::Slot(slot), true)?;
        let path = entry.reset_slot(slot)?;
        let active = registration
            .active
            .as_ref()
            .filter(|_| !accepted.begin.dependencies().is_empty());
        let cached_slot = active
            .map(|active| {
                let held = entry.lock(Lock::Slot(active.slot), false)?;
                Ok::<_, ShareCacheError>((held, entry.snapshot_data(active.slot)?))
            })
            .transpose()?;
        let cache = active
            .zip(cached_slot.as_ref())
            .map(|(active, (_, data))| GuestReplayCache {
                data,
                head: &active.snapshot,
                native_root: &active.native_root,
                through: active.through.as_ref(),
            });
        let mut session = if self.managed {
            RestoreSession::begin_managed_guest(&path, accepted)?
        } else {
            RestoreSession::begin_guest_at(&path.join("unactivated"), accepted)?
        };
        let (data, native_root, through) = session
            .stage_guest_recovery(
                &reader,
                opened.publication_key,
                registration.publisher.clone(),
                cache.as_ref(),
            )
            .await?;
        let workspace = session.path().to_path_buf();
        drop(session);
        files::finish_slot(&path, &data, &workspace)?;
        registration.active = Some(VerifiedSnapshot {
            slot,
            snapshot: snapshot.clone(),
            native_root,
            through,
        });
        publish(entry, Some(previous), &registration)?;
        Ok(snapshot)
    }
}

impl ShareReader {
    pub fn snapshot(&self) -> &ShareSnapshot {
        &self.snapshot
    }

    /// Read an exact primary key and its optional inline file. Reuse this
    /// reader for every key of a logical read to hold one version throughout.
    pub fn read_record(&self, query: &StagedRecordQuery<'_>) -> Result<Option<StagedRecord>> {
        self.native
            .read_record(query, &self.content_key)
            .map_err(RestoreError::native_proof)
            .map_err(ShareCacheError::from)
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
            Self::Restore(error) => write!(f, "{error}"),
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

impl From<RestoreError> for ShareCacheError {
    fn from(error: RestoreError) -> Self {
        Self::Restore(error)
    }
}

impl From<crate::share_invitation::ShareInvitationError> for ShareCacheError {
    fn from(error: crate::share_invitation::ShareInvitationError) -> Self {
        Self::Invitation(error)
    }
}
