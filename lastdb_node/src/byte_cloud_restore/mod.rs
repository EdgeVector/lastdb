//! Verified recipient staging of accepted opaque-byte cloud backups and restore receipts.
mod download;
mod logs;
mod native;
mod runs;

use crate::byte_cloud_log::MutationLogRoot;
use crate::cloud_backup_client::byte_backup::{BackupTransportError, ByteBackupReader};
use cloud_backup_protocol::recovery::RecoveryProfile;
use cloud_backup_protocol::v2::{
    publication::{CommitResponse, CommitStatusRequest, Head, HeadReadResult},
    AcceptedBackup, Version,
};
use cloud_backup_protocol::{Digest, ErrorCode, Id, NativeRoot, ProtocolError, RecoveryDescriptor};
pub use download::verify_restore_receipt;
use download::{bounded_bytes, check_capacity, verify_file};
pub use fold_db::storage::disk_pack::run_restore::{StagedRecord, StagedRecordQuery};
use fold_db::storage::disk_pack::{mutation_log::LogPosition, NativeCloudWorkGuard};
use fold_db::storage::error::{NativeProofCause, NativeProofStage};
use native as local;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::sync::{atomic::AtomicU64, Arc};
use zeroize::Zeroizing;

pub(crate) const MANAGED_SHARE_DIRECTORY: &str = "received-shares-v1";
pub const RESTORE_RECEIPT_NAME: &str = "byte-cloud-restore-receipt.json";

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeFileProof {
    pub path: String,
    pub sha256: Digest,
    pub bytes: u64,
}

#[derive(Debug)]
pub enum RestoreError {
    Invalid(&'static str),
    Limit(&'static str),
    Io(std::io::Error),
    Protocol(ProtocolError),
    Source(&'static str),
    NativeProof {
        stage: NativeProofStage,
        cause: NativeProofCause,
    },
}

pub type Result<T> = std::result::Result<T, RestoreError>;

impl From<std::io::Error> for RestoreError {
    fn from(v: std::io::Error) -> Self {
        Self::Io(v)
    }
}
impl From<ProtocolError> for RestoreError {
    fn from(v: ProtocolError) -> Self {
        Self::Protocol(v)
    }
}
impl RestoreError {
    pub(crate) fn native_proof(error: fold_db::storage::StorageError) -> Self {
        let (stage, cause) = error.native_proof_context(NativeProofStage::Restore);
        drop(error);
        Self::NativeProof { stage, cause }
    }
}
impl std::fmt::Display for RestoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(s) => write!(f, "invalid restore {s}"),
            Self::Limit(s) => write!(f, "restore limit: {s}"),
            Self::Io(_) => f.write_str("restore disk operation failed"),
            Self::Protocol(_) => f.write_str("restore protocol validation failed"),
            Self::Source(s) => write!(f, "restore source unavailable: {s}"),
            Self::NativeProof { stage, cause } => {
                write!(f, "native restore proof {stage}: {cause}")
            }
        }
    }
}
impl std::error::Error for RestoreError {}

/// Key material used to verify and decrypt a backup.
pub(super) struct RecoveryDecryption {
    pub(super) content: Zeroizing<[u8; 32]>,
    pub(super) account_public_key: String,
}

impl RecoveryDecryption {
    pub(super) fn validate_account(&self, pubkey: &Digest) -> Result<()> {
        (self.account_public_key == pubkey.as_str())
            .then_some(())
            .ok_or(RestoreError::Invalid("recovery public key"))
    }
}

/// Read-only previous state while the cache holds its slot lock.
pub(crate) struct GuestReplayCache<'a> {
    pub(crate) data: &'a Path,
    pub(crate) head: &'a Head,
    pub(crate) native_root: &'a NativeRoot,
    pub(crate) through: Option<&'a LogPosition>,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RestoreMode {
    Recipient,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Selection {
    version: u8,
    session_id: uuid::Uuid,
    accepted: AcceptedBackup,
    destination: PathBuf,
    endpoint: Option<String>,
    mode: RestoreMode,
}

/// Durable exact V2 selection.
pub struct RestoreSession {
    root: PathBuf,
    selection: Selection,
    _lock: File,
    spool_bytes: Arc<AtomicU64>,
}

impl RestoreSession {
    fn begin_in(destination: &Path, accepted: AcceptedBackup, check_primary: bool) -> Result<Self> {
        if check_primary {
            crate::offline_home::refuse_primary(destination)
                .map_err(|_| RestoreError::Invalid("destination"))?;
        }
        accepted.validate()?;
        let parent = destination
            .parent()
            .ok_or(RestoreError::Invalid("destination parent"))?
            .canonicalize()?;
        let name = destination
            .file_name()
            .ok_or(RestoreError::Invalid("destination name"))?;
        let destination = parent.join(name);
        match fs::symlink_metadata(&destination) {
            Ok(_) => return Err(RestoreError::Invalid("destination exists")),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        let session_id = uuid::Uuid::new_v4();
        let root = parent.join(format!(".byte-cloud-restore-{session_id}"));
        fs::DirBuilder::new().mode(0o700).create(&root)?;
        let _lock = local::owner_file(&root.join("owner.lock"), true)?;
        _lock
            .try_lock()
            .map_err(|_| RestoreError::Invalid("session already owned"))?;
        let selection = Selection {
            version: 2,
            session_id,
            accepted,
            destination,
            endpoint: None,
            mode: RestoreMode::Recipient,
        };
        local::atomic_json(&root.join("selection.json"), &selection)?;
        for sub in ["cache", "entries", "chain", "home", "home/data"] {
            local::ensure_directory(&root.join(sub))?;
        }
        local::sync_dir(&root)?;
        local::sync_dir(&parent)?;
        Ok(Self {
            root,
            selection,
            _lock,
            spool_bytes: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn path(&self) -> &Path {
        &self.root
    }
    pub fn accepted(&self) -> &AcceptedBackup {
        &self.selection.accepted
    }

    pub(super) fn ancestor(&self, accepted: AcceptedBackup) -> Result<Self> {
        accepted.validate()?;
        if accepted.job().stream != self.accepted().job().stream
            || accepted.ordinal.get() > self.accepted().ordinal.get()
        {
            return Err(RestoreError::Invalid("accepted ancestor stream or order"));
        }
        Ok(Self {
            root: self.root.clone(),
            selection: Selection {
                accepted,
                ..self.selection.clone()
            },
            _lock: self._lock.try_clone()?,
            spool_bytes: Arc::clone(&self.spool_bytes),
        })
    }

    pub(crate) async fn guest_accepted(reader: &ByteBackupReader) -> Result<AcceptedBackup> {
        let stream = reader
            .guest_stream()
            .ok_or(RestoreError::Invalid("guest reader"))?;
        let accepted = match reader
            .guest_head()
            .await
            .map_err(|e| reader_error(reader, &e))?
        {
            HeadReadResult::Ok { response } => response
                .accepted
                .ok_or(RestoreError::Source("share has no published backup"))?,
            HeadReadResult::Error { error } => return Err(reader_error(reader, &error.into())),
        };
        (&accepted.job().stream == stream)
            .then_some(accepted)
            .ok_or(RestoreError::Invalid("guest stream"))
    }

    pub(crate) fn begin_guest_at(destination: &Path, accepted: AcceptedBackup) -> Result<Self> {
        Self::begin_in(destination, accepted, true)
    }

    pub(crate) fn begin_managed_guest(slot: &Path, accepted: AcceptedBackup) -> Result<Self> {
        let invalid = || RestoreError::Invalid("managed guest slot");
        let entry = slot.parent().ok_or_else(invalid)?;
        let registry = entry.parent().ok_or_else(invalid)?;
        fn name_of(p: &Path) -> Option<&str> {
            p.file_name().and_then(std::ffi::OsStr::to_str)
        }
        if !matches!(name_of(slot), Some("snapshot-0" | "snapshot-1"))
            || name_of(entry).is_none_or(|n| Id::try_from(n.to_owned()).is_err())
            || name_of(registry) != Some(MANAGED_SHARE_DIRECTORY)
            || slot.canonicalize()? != slot
        {
            return Err(invalid());
        }
        [registry, entry, slot]
            .into_iter()
            .try_for_each(local::directory)?;
        Self::begin_in(&slot.join("unactivated"), accepted, false)
    }

    pub(crate) async fn stage_guest_recovery(
        &mut self,
        reader: &ByteBackupReader,
        content_key: Zeroizing<[u8; 32]>,
        publisher_public_key: Digest,
        cache: Option<&GuestReplayCache<'_>>,
    ) -> Result<(PathBuf, NativeRoot, Option<LogPosition>)> {
        let accepted = self.accepted();
        if self.selection.mode != RestoreMode::Recipient
            || reader.guest_stream() != Some(&accepted.job().stream)
        {
            return Err(RestoreError::Invalid("recipient restore session"));
        }
        let recovery = RecoveryDescriptor {
            profile: RecoveryProfile::MiniPortableV1,
            account_public_key: publisher_public_key,
        };
        recovery.validate_for(&accepted.job().stream.scope)?;
        let dec = RecoveryDecryption {
            content: content_key,
            account_public_key: recovery.account_public_key.as_str().to_owned(),
        };
        let _admission = NativeCloudWorkGuard::acquire()
            .map_err(|_| RestoreError::Limit("active native cloud operation"))?;
        self.require_unactivated()?;
        self.authorize_read(reader).await?;
        let (inventory, bytes) = runs::load_closure(self, reader).await?;
        let (proof, through) = if self.accepted().begin.dependencies().is_empty() {
            let root = runs::snapshot_closure(self, &dec, &bytes)?;
            (
                runs::stage_and_verify(self, reader, &dec, &root, inventory).await?,
                None,
            )
        } else {
            let root = MutationLogRoot::decode(&bytes, &dec.content, &self.accepted().begin)
                .map_err(|_| RestoreError::Invalid("recipient mutation log root"))?;
            drop(inventory);
            let (p, t) = logs::stage_guest(self, reader, &dec, root, cache).await?;
            (p, Some(t))
        };
        self.require_unactivated()?;
        let native_root = proof.native_root().map_err(RestoreError::native_proof)?;
        Ok((self.root.join("home/data"), native_root, through))
    }

    fn require_unactivated(&self) -> Result<()> {
        let home = self.root.join("home");
        for path in [
            self.selection.destination.clone(),
            home.join(crate::identity::IDENTITY_KEY_FILE),
            crate::cloud::cloud_credentials_path(&home),
        ] {
            match fs::symlink_metadata(path) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
                Ok(_) => return Err(RestoreError::Invalid("recipient stage already activated")),
            }
        }
        Ok(())
    }

    async fn authorize_read(&mut self, client: &ByteBackupReader) -> Result<()> {
        let ep = client.canonical_endpoint();
        match &self.selection.endpoint {
            Some(saved) if *saved != ep => return Err(RestoreError::Invalid("restore endpoint")),
            None => {
                self.selection.endpoint = Some(ep);
                local::atomic_json(&self.root.join("selection.json"), &self.selection)?;
            }
            Some(_) => {}
        }
        self.check_spool()?;
        let req = CommitStatusRequest {
            version: Version::V2,
            job: self.accepted().job().clone(),
        };
        let status = client
            .commit_status(&req)
            .await
            .map_err(|e| reader_error(client, &e))?;
        match status {
            CommitResponse::Accepted { backup } if *backup == *self.accepted() => Ok(()),
            _ => Err(RestoreError::Invalid("authoritative accepted selection")),
        }
    }
}

fn reader_error(reader: &ByteBackupReader, error: &BackupTransportError) -> RestoreError {
    if reader.guest_stream().is_some() {
        if matches!(error, BackupTransportError::Authentication)
            || matches!(error, BackupTransportError::Protocol(e) if e.code == ErrorCode::Unauthorized)
        {
            return RestoreError::Source("guest permission refused");
        }
        if matches!(
            error,
            BackupTransportError::Protocol(e)
                if e.code == ErrorCode::RetryLater && e.field == "guest.head_changed"
        ) {
            return RestoreError::Source("share changed, start a new guest stage");
        }
    }
    RestoreError::Source("byte transport")
}
