//! Offline restore of an exact accepted opaque-byte backup.
mod download;
mod logs;
mod native;
mod recipient;
mod runs;

use download::{bounded_bytes, check_capacity, verify_file};
pub use fold_db::storage::disk_pack::run_restore::{StagedRecord, StagedRecordQuery};
use native as local;
pub(crate) use recipient::GuestReplayCache;
use recipient::RecoveryDecryption;
pub use recipient::{verify_restore_receipt, RecoveryKeys};

use crate::cloud_backup_client::byte_backup::{
    BackupTransportError, ByteBackupClient, ByteBackupReader,
};
use cloud_backup_protocol::v2::{
    access::ReadSelector,
    publication::{CommitResponse, CommitStatusRequest},
    AcceptedBackup, Destination, Version,
};
use cloud_backup_protocol::{Digest, ErrorCode, ProtocolError};
use fold_db::storage::disk_pack::{
    byte_backup::{ClientRoot, CODEC_ENVELOPE_MAX_BYTES},
    NativeCloudWorkGuard,
};
use fold_db::storage::error::{NativeProofCause, NativeProofStage};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::sync::{atomic::AtomicU64, Arc};

pub(crate) const MANAGED_SHARE_DIRECTORY: &str = "received-shares-v1";
pub const RESTORE_RECEIPT_NAME: &str = "byte-cloud-restore-receipt.json";
pub(super) const RESTORE_RECEIPT_VERSION: u8 = 2;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreReport {
    pub commit_sha256: String,
    pub native_files: u64,
    pub native_bytes: u64,
    pub verified_records: u64,
    pub destination: PathBuf,
}

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

#[derive(Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum RestoreMode {
    #[default]
    Owner,
    Recipient,
}
impl RestoreMode {
    fn is_owner(&self) -> bool {
        *self == Self::Owner
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Selection {
    version: u8,
    pub session_id: uuid::Uuid,
    pub accepted: AcceptedBackup,
    pub destination: PathBuf,
    pub endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "RestoreMode::is_owner")]
    pub mode: RestoreMode,
}

/// Durable exact V2 selection. A resume never discovers or substitutes a new head.
pub struct RestoreSession {
    pub(super) root: PathBuf,
    pub(super) selection: Selection,
    _lock: File,
    pub(super) spool_bytes: Arc<AtomicU64>,
}

impl RestoreSession {
    pub fn begin(destination: &Path, accepted: AcceptedBackup) -> Result<Self> {
        Self::begin_in(destination, accepted, RestoreMode::Owner, true)
    }

    pub(super) fn begin_in(
        destination: &Path,
        accepted: AcceptedBackup,
        mode: RestoreMode,
        check_primary: bool,
    ) -> Result<Self> {
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
        let _lock = lock_session(&root, true)?;
        let selection = Selection {
            version: 2,
            session_id,
            accepted,
            destination,
            endpoint: None,
            mode,
        };
        local::atomic_json(&root.join("selection.json"), &selection)?;
        scaffold(&root)?;
        local::sync_dir(&root)?;
        local::sync_dir(&parent)?;
        Ok(Self {
            root,
            selection,
            _lock,
            spool_bytes: Arc::default(),
        })
    }

    pub fn resume(root: &Path) -> Result<Self> {
        local::directory(root)?;
        crate::offline_home::refuse_primary(root)
            .map_err(|_| RestoreError::Invalid("session directory"))?;
        let root = root.canonicalize()?;
        let _lock = lock_session(&root, false)?;
        let sel: Selection = local::read_json(&root.join("selection.json"), 256 * 1024)?;
        sel.accepted.validate()?;
        let exp = format!(".byte-cloud-restore-{}", sel.session_id);
        if sel.version != 2
            || sel.session_id.is_nil()
            || root.file_name().and_then(std::ffi::OsStr::to_str) != Some(&exp)
            || sel.destination.parent() != root.parent()
            || sel
                .endpoint
                .as_ref()
                .is_some_and(|v| v.is_empty() || v.len() > 8192)
        {
            return Err(RestoreError::Invalid("saved selection"));
        }
        crate::offline_home::refuse_primary(&sel.destination)
            .map_err(|_| RestoreError::Invalid("destination"))?;
        if !sel.destination.try_exists()? {
            scaffold(&root)?;
        }
        let session = Self {
            root,
            selection: sel,
            _lock,
            spool_bytes: Arc::default(),
        };
        session.check_spool().map(|()| session)
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

    pub async fn restore(
        &mut self,
        client: &ByteBackupClient,
        keys: &RecoveryKeys,
    ) -> Result<RestoreReport> {
        if !self.selection.mode.is_owner() {
            return Err(RestoreError::Invalid("owner restore session"));
        }
        let _admission = acquire_native_work()?;
        let reader = client.reader();
        self.authorize_read(&reader).await?;
        let (inventory, bytes) = load_closure(self, &reader).await?;
        let key = &keys.decryption.content;
        if ClientRoot::version_of(&bytes, key)
            .map_err(|_| RestoreError::Invalid("client native metadata"))?
            == crate::byte_cloud_log::VERSION
        {
            let root =
                crate::byte_cloud_log::MutationLogRoot::decode(&bytes, key, &self.accepted().begin)
                    .map_err(|_| RestoreError::Invalid("client mutation log root"))?;
            drop(inventory);
            return logs::restore(self, &reader, &keys.decryption, &keys.seed, root).await;
        }
        let root = snapshot_closure(self, &keys.decryption, &bytes)?;
        if let Some(report) = recipient::published(self)? {
            return Ok(report);
        }
        let proof =
            runs::stage_and_verify(self, &reader, &keys.decryption, &root, inventory).await?;
        recipient::finish_run(self, &root.native_root, &keys.seed, &proof)
    }

    pub(super) async fn authorize_read(&mut self, client: &ByteBackupReader) -> Result<()> {
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
        match client
            .commit_status(&req)
            .await
            .map_err(|e| reader_error(client, &e))?
        {
            CommitResponse::Accepted { backup } if *backup == *self.accepted() => Ok(()),
            _ => Err(RestoreError::Invalid("authoritative accepted selection")),
        }
    }
}

fn lock_session(root: &Path, create: bool) -> Result<File> {
    let lock = local::owner_file(&root.join("owner.lock"), create)?;
    lock.try_lock()
        .map(|()| lock)
        .map_err(|_| RestoreError::Invalid("session already owned"))
}

pub(super) fn acquire_native_work() -> Result<NativeCloudWorkGuard> {
    NativeCloudWorkGuard::acquire()
        .map_err(|_| RestoreError::Limit("active native cloud operation"))
}

fn scaffold(root: &Path) -> Result<()> {
    ["cache", "entries", "chain", "home", "home/data"]
        .into_iter()
        .try_for_each(|name| local::ensure_directory(&root.join(name)))
}

fn reader_error(reader: &ByteBackupReader, error: &BackupTransportError) -> RestoreError {
    RestoreError::Source(match error {
        _ if reader.guest_stream().is_none() => "byte transport",
        BackupTransportError::Protocol(e)
            if e.code == ErrorCode::RetryLater && e.field == "guest.head_changed" =>
        {
            "share changed, start a new guest stage"
        }
        BackupTransportError::Authentication => "guest permission refused",
        BackupTransportError::Protocol(e) if e.code == ErrorCode::Unauthorized => {
            "guest permission refused"
        }
        _ => "byte transport",
    })
}

fn snapshot_closure(
    session: &RestoreSession,
    decryption: &RecoveryDecryption,
    bytes: &[u8],
) -> Result<ClientRoot> {
    let accepted = session.accepted();
    let root = ClientRoot::decode(bytes, &decryption.content, &accepted.begin)
        .map_err(|_| RestoreError::Invalid("client native metadata"))?;
    root.recovery.validate_for(&accepted.job().stream.scope)?;
    decryption
        .validate_account(&root.recovery.account_public_key)
        .map(|()| root)
}

async fn load_closure(
    session: &RestoreSession,
    client: &ByteBackupReader,
) -> Result<(runs::Inventory, Vec<u8>)> {
    let id = &session.accepted().root_receipt_id;
    let inventory = runs::Inventory::load(session, client).await?;
    let receipt = inventory.receipt(id)?;
    let grant = download::one(
        session,
        client,
        ReadSelector::Receipt {
            receipt_id: id.clone(),
        },
    )
    .await?;
    grant.validate_for_receipt(receipt)?;
    if grant.bytes.get() > CODEC_ENVELOPE_MAX_BYTES {
        return Err(RestoreError::Limit("client root metadata"));
    }
    let path = download::fetch(session, client, &grant, true).await?;
    let bytes = bounded_bytes(&path, CODEC_ENVELOPE_MAX_BYTES)?;
    if receipt.object.destination != Destination::R2 {
        return Err(RestoreError::Invalid("client root destination"));
    }
    Ok((inventory, bytes))
}
