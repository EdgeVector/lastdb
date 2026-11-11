//! Offline restore of an exact accepted opaque-byte backup.
//! The cloud authorizes bytes; this client verifies all native semantics,
//! then installs and publishes the verified home.
mod download;
mod inventory;
mod logs;
mod native;
mod recipient;
mod runs;
mod session;

pub mod local {
    pub(crate) use crate::byte_cloud_restore::native::{
        atomic_json, atomic_json_bounded, directory, owner_file, prepare_native_ephemera,
        read_json, writable_file,
    };
}

pub(crate) mod publish {
    pub(crate) use crate::byte_cloud_restore::recipient::{
        activate_owned_home, sync_owned_tree, sync_publish_parents,
    };
}

pub(crate) use download::check_capacity;
use download::{bounded_bytes, verify_file};
pub use fold_db::storage::disk_pack::run_restore::{StagedRecord, StagedRecordQuery};
use inventory::RecoveryDecryption;
pub use inventory::RecoveryKeys;
pub(crate) use recipient::GuestReplayCache;
pub use session::RestoreSession;

use crate::cloud_backup_client::byte_backup::{
    BackupTransportError, ByteBackupClient, ByteBackupReader,
};
use cloud_backup_protocol::v2::access::ReadSelector;
use cloud_backup_protocol::{Digest, ErrorCode, NativeRoot};
use fold_db::storage::disk_pack::byte_backup::{ClientRoot, CODEC_ENVELOPE_MAX_BYTES};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub(crate) const MANAGED_SHARE_DIRECTORY: &str = "received-shares-v1";
pub(super) const RESTORE_RECEIPT_NAME: &str = "byte-cloud-restore-receipt.json";
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

pub(super) struct VerifiedNativeContext<'a> {
    pub root: &'a Path,
    pub destination: &'a Path,
    pub session_id: uuid::Uuid,
    pub commit_sha256: &'a Digest,
    pub native_root: &'a NativeRoot,
}

#[derive(Clone, Copy)]
pub(super) struct NativeInventory {
    pub files: u64,
    pub bytes: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeFileProof {
    pub path: String,
    pub sha256: Digest,
    pub bytes: u64,
}

impl<'a> VerifiedNativeContext<'a> {
    pub fn new(session: &'a RestoreSession, native_root: &'a NativeRoot) -> Self {
        Self {
            root: &session.root,
            destination: &session.selection.destination,
            session_id: session.selection.session_id,
            commit_sha256: &session.accepted().commit_sha256,
            native_root,
        }
    }

    pub fn data_root(&self) -> PathBuf {
        self.root.join("home/data")
    }

    pub fn published(&self) -> Result<Option<RestoreReport>> {
        recipient::published_context(self)
    }

    pub fn finish_run(
        &self,
        identity_seed: &[u8; 32],
        inventory: NativeInventory,
        proof: &fold_db::storage::disk_pack::run_restore::RunRestoreGuard,
    ) -> Result<RestoreReport> {
        recipient::finish_run_context(self, identity_seed, inventory, proof)
    }
}

/// Static errors never contain keys, record values, provider URLs, or local secrets.
#[derive(Debug)]
pub enum RestoreError {
    Invalid(&'static str),
    Limit(&'static str),
    Io(std::io::Error),
    Protocol(cloud_backup_protocol::ProtocolError),
    Source(&'static str),
    SharedStage {
        stage: &'static str,
        cause: Box<Self>,
    },
    SharedWorkspace {
        path: PathBuf,
        cause: Box<Self>,
    },
    NativeProof {
        stage: fold_db::storage::error::NativeProofStage,
        cause: fold_db::storage::error::NativeProofCause,
    },
}
pub type Result<T> = std::result::Result<T, RestoreError>;

impl From<std::io::Error> for RestoreError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
impl From<cloud_backup_protocol::ProtocolError> for RestoreError {
    fn from(value: cloud_backup_protocol::ProtocolError) -> Self {
        Self::Protocol(value)
    }
}
impl RestoreError {
    pub(crate) fn shared_stage(self, stage: &'static str) -> Self {
        Self::SharedStage {
            stage,
            cause: Box::new(self),
        }
    }

    pub(crate) fn native_proof(error: fold_db::storage::StorageError) -> Self {
        let (stage, cause) =
            error.native_proof_context(fold_db::storage::error::NativeProofStage::Restore);
        drop(error);
        Self::NativeProof { stage, cause }
    }
}
impl std::fmt::Display for RestoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(field) => write!(f, "invalid restore {field}"),
            Self::Limit(field) => write!(f, "restore limit: {field}"),
            Self::Io(_) => f.write_str("restore disk operation failed"),
            Self::Protocol(_) => f.write_str("restore protocol validation failed"),
            Self::Source(field) => write!(f, "restore source unavailable: {field}"),
            Self::SharedStage { stage, cause } => write!(f, "shared restore {stage}: {cause}"),
            Self::SharedWorkspace { path, cause } => {
                write!(
                    f,
                    "shared restore retained workspace {}: {cause}",
                    path.display()
                )
            }
            Self::NativeProof { stage, cause } => {
                write!(f, "native restore proof {stage}: {cause}")
            }
        }
    }
}
impl std::error::Error for RestoreError {}

fn source_error(_: BackupTransportError) -> RestoreError {
    RestoreError::Source("byte transport")
}

fn reader_error(reader: &ByteBackupReader, error: BackupTransportError) -> RestoreError {
    if reader.guest_stream().is_some() {
        match &error {
            BackupTransportError::Protocol(error)
                if error.code == ErrorCode::RetryLater && error.field == "guest.head_changed" =>
            {
                return RestoreError::Source("share changed, start a new guest stage");
            }
            BackupTransportError::Authentication => {
                return RestoreError::Source("guest permission refused");
            }
            BackupTransportError::Protocol(error) if error.code == ErrorCode::Unauthorized => {
                return RestoreError::Source("guest permission refused");
            }
            _ => {}
        }
    }
    source_error(error)
}

async fn restore_closure(
    session: &RestoreSession,
    client: &ByteBackupReader,
    decryption: &RecoveryDecryption,
    identity_seed: &[u8; 32],
) -> Result<RestoreReport> {
    let (inventory, bytes) = load_closure(session, client).await?;
    let version = ClientRoot::version_of(&bytes, decryption.content_key())
        .map_err(|_| RestoreError::Invalid("client native metadata"))?;
    if version == crate::byte_cloud_log::VERSION {
        let root = crate::byte_cloud_log::MutationLogRoot::decode(
            &bytes,
            decryption.content_key(),
            &session.accepted().begin,
        )
        .map_err(|_| RestoreError::Invalid("client mutation log root"))?;
        drop(inventory);
        return logs::restore(session, client, decryption, identity_seed, root).await;
    }
    let root = snapshot_closure(session, decryption, &bytes)?;
    if let Some(report) = VerifiedNativeContext::new(session, &root.native_root).published()? {
        return Ok(report);
    }
    runs::restore(session, client, decryption, identity_seed, &root, inventory).await
}

fn snapshot_closure(
    session: &RestoreSession,
    decryption: &RecoveryDecryption,
    bytes: &[u8],
) -> Result<ClientRoot> {
    let accepted = session.accepted();
    let root = ClientRoot::decode(bytes, decryption.content_key(), &accepted.begin)
        .map_err(|_| RestoreError::Invalid("client native metadata"))?;
    root.recovery.validate_for(&accepted.job().stream.scope)?;
    decryption.validate_account(&root.recovery.account_public_key)?;
    Ok(root)
}

async fn load_closure(
    session: &RestoreSession,
    client: &ByteBackupReader,
) -> Result<(inventory::Inventory, Vec<u8>)> {
    let accepted = session.accepted();
    let inventory = inventory::Inventory::load(session, client).await?;
    let grant = download::one(
        session,
        client,
        ReadSelector::Receipt {
            receipt_id: accepted.root_receipt_id.clone(),
        },
    )
    .await?;
    grant.validate_for_receipt(inventory.receipt(&accepted.root_receipt_id)?)?;
    if grant.bytes.get() > CODEC_ENVELOPE_MAX_BYTES {
        return Err(RestoreError::Limit("client root metadata"));
    }
    let path = download::fetch(session, client, &grant, true).await?;
    let bytes = bounded_bytes(&path, CODEC_ENVELOPE_MAX_BYTES)?;
    if inventory
        .receipt(&accepted.root_receipt_id)?
        .object
        .destination
        != cloud_backup_protocol::v2::Destination::R2
    {
        return Err(RestoreError::Invalid("client root destination"));
    }
    Ok((inventory, bytes))
}
