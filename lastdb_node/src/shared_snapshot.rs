//! Publish an exact frozen, recoverable-only native checkpoint.
//!
//! Native admission owns the freeze and journal. The publisher preserves its
//! exact inventory and publication ID through uncertain transport results.

use crate::byte_cloud_restore::{RestoreError, Result};
use crate::shared_restore::recovery::{SnapshotContents, SnapshotFile, SnapshotSchema};
use cloud_backup_protocol::shared::{
    inventory_sha256, SealedObject, SnapshotManifest, SnapshotReceipt, StreamCover,
};
use cloud_backup_protocol::v2::Destination;
use cloud_backup_protocol::{bytes_sha256, DecimalU64, Digest, ErrorCode, Id, RelativePath};
use fold_db::portable_mutation::{StreamId, StreamProgress};
use fold_db::storage::disk_pack::{byte_backup::RunBackupProfile, HOME_FORMAT};
use std::collections::BTreeMap;
use std::io::Read;
use std::path::Path;
use std::time::Duration;

pub(crate) mod native;
mod owner;
pub(crate) mod sequence;
mod state;
pub use owner::SignedSnapshotTransport;
use state::{Intent, IntentStore, Phase};

#[derive(PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenFile {
    pub path: RelativePath,
    pub destination: Destination,
    pub bytes: DecimalU64,
    pub sha256: Digest,
}

/// The one cut owned by the freeze, with accepted per-stream covers.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenSnapshot {
    pub database_id: Id,
    pub snapshot_id: Id,
    pub replaces: Option<Id>,
    pub expires_at_ms: u64,
    pub profile: RunBackupProfile,
    pub covers: Vec<StreamCover>,
    pub progress: BTreeMap<StreamId, StreamProgress>,
    pub delete_reclaimed_through_ms: Option<u64>,
    pub files: Vec<FrozenFile>,
    pub schemas: Vec<SnapshotSchema>,
}

/// Adapter from native checkpoint admission. Open files through the same
/// frozen owner that supplied the immutable inventory.
#[async_trait::async_trait]
pub trait FrozenCheckpoint: Send + Sync {
    fn cut(&self) -> &FrozenSnapshot;

    /// An owned directory outside the exported recoverable files.
    fn intent_directory(&self) -> &Path;

    /// Require the native file hold for this cut.
    fn require_hold(&self) -> Result<()>;

    fn open_file(&self, path: &RelativePath) -> Result<Box<dyn Read + Send>>;

    /// Thaw this exact publication and catch up its journal. A repeat of an
    /// already completed thaw for this ID succeeds.
    async fn thaw(&mut self, snapshot_id: &Id) -> Result<()>;
}

pub enum PublishReply {
    Accepted(SnapshotReceipt),
    /// An authoritative final rejection of this exact input. Timeouts and
    /// missing transport replies must return an error instead.
    Refused,
}

/// The authorized transport verifies exact immutable stored bytes and signs
/// the conditional publication as the designated device.
#[async_trait::async_trait]
pub trait SnapshotTransport: Sync {
    /// Finish or cancel and release the source reader before returning.
    async fn upload(
        &self,
        database: &Id,
        snapshot: &Id,
        object: &SealedObject,
        destination: Destination,
        bytes: Box<dyn Read + Send>,
        deadline_ms: u64,
    ) -> Result<SealedObject>;

    async fn stage_inventory(
        &self,
        manifest: &SnapshotManifest,
        inventory: &[SealedObject],
    ) -> Result<()>;

    /// Repeating this exact input must reconcile to its durable receipt, even
    /// if a later snapshot has become current.
    async fn publish(&self, manifest: &SnapshotManifest) -> Result<PublishReply>;
}

pub enum SnapshotOutcome {
    Published(SnapshotReceipt),
    Abandoned,
}

/// Resume or publish the source's exact freeze. The coordinator persists
/// submission before issuing it and thaws only after a durable outcome.
pub async fn publish(
    source: &mut impl FrozenCheckpoint,
    transport: &impl SnapshotTransport,
    content_key: &[u8; 32],
) -> Result<SnapshotOutcome> {
    let store = IntentStore::open(source.intent_directory())?;
    let mut intent = match store.load()? {
        Some(intent) => intent,
        None => prepare(source, &store, content_key)?,
    };
    require_identity(source.cut(), &intent.manifest)?;

    if matches!(intent.phase, Phase::Prepared | Phase::Submitting) {
        let sealed = store.read_manifest(&intent.manifest.encrypted_manifest)?;
        let contents = SnapshotContents::open(&sealed, &intent.manifest, content_key)?;
        let inventory = contents.inventory();
        require_capture(source.cut(), &contents)?;
        let first_submission = matches!(intent.phase, Phase::Prepared);
        if first_submission {
            source.require_hold()?;
            let transfer = async {
                upload(source, transport, &intent.manifest, &contents, sealed).await?;
                within_freeze(
                    source.cut(),
                    transport.stage_inventory(&intent.manifest, &inventory),
                )
                .await
            }
            .await;
            if let Err(error) = transfer {
                let closed = staging_expired(&error);
                if closed || remaining(source.cut()).is_err() {
                    intent.phase = Phase::Abandoned;
                    store.save(&intent)?;
                    source.thaw(&intent.manifest.snapshot_id).await?;
                }
                if closed {
                    return Ok(SnapshotOutcome::Abandoned);
                }
                return Err(error);
            }
            intent.phase = Phase::Submitting;
            store.save(&intent)?;
        }
        let publish = transport.publish(&intent.manifest);
        let reply = if first_submission {
            within_freeze(source.cut(), publish).await
        } else {
            publish.await
        };
        let receipt = match reply {
            Ok(PublishReply::Accepted(receipt)) => {
                if receipt != intent.manifest.receipt()? {
                    return Err(RestoreError::Invalid("shared snapshot receipt"));
                }
                Phase::Accepted(receipt)
            }
            Ok(PublishReply::Refused) => Phase::Abandoned,
            Err(error) if staging_expired(&error) => Phase::Abandoned,
            Err(error) => return Err(error),
        };
        intent.phase = receipt;
        store.save(&intent)?;
    }

    let outcome = match intent.phase {
        Phase::Accepted(receipt) => {
            if receipt != intent.manifest.receipt()? {
                return Err(RestoreError::Invalid("saved shared snapshot receipt"));
            }
            SnapshotOutcome::Published(receipt)
        }
        Phase::Abandoned => SnapshotOutcome::Abandoned,
        Phase::Prepared | Phase::Submitting => {
            return Err(RestoreError::Invalid(
                "unresolved shared snapshot publication",
            ));
        }
    };
    source.thaw(&intent.manifest.snapshot_id).await?;
    Ok(outcome)
}

fn staging_expired(error: &RestoreError) -> bool {
    // The service checks the accepted receipt before returning this refusal.
    matches!(error, RestoreError::Protocol(error)
        if error.code == ErrorCode::JobClosed && error.field == "shared.snapshot.staging_expired")
}

fn prepare(source: &impl FrozenCheckpoint, store: &IntentStore, key: &[u8; 32]) -> Result<Intent> {
    source.require_hold()?;
    let cut = source.cut();
    remaining(cut)?;
    let files = cut
        .files
        .iter()
        .map(|file| {
            Ok(SnapshotFile {
                path: file.path.clone(),
                object: SealedObject {
                    version_id: new_id()?,
                    bytes: file.bytes,
                    sha256: file.sha256.clone(),
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let contents = SnapshotContents {
        version: 1,
        database_id: cut.database_id.clone(),
        snapshot_id: cut.snapshot_id.clone(),
        disk_format: HOME_FORMAT,
        native_profile: cut.profile.clone(),
        progress: cut.progress.clone(),
        delete_reclaimed_through_ms: cut.delete_reclaimed_through_ms,
        files,
        schemas: cut.schemas.clone(),
    };
    require_capture(cut, &contents)?;
    let sealed = contents.seal(key)?;
    let inventory = contents.inventory();
    let manifest = SnapshotManifest {
        database_id: cut.database_id.clone(),
        snapshot_id: cut.snapshot_id.clone(),
        replaces: cut.replaces.clone(),
        disk_format: HOME_FORMAT,
        covers: cut.covers.clone(),
        encrypted_manifest: SealedObject {
            version_id: new_id()?,
            bytes: DecimalU64::new(sealed.len() as u64),
            sha256: bytes_sha256(&sealed),
        },
        object_count: u32::try_from(inventory.len())
            .map_err(|_| RestoreError::Limit("shared snapshot object count"))?,
        inventory_sha256: inventory_sha256(&inventory)?,
    };
    manifest.validate()?;
    manifest.validate_inventory(&inventory)?;
    SnapshotContents::open(&sealed, &manifest, key)?;
    store.write_manifest(&sealed)?;
    let intent = Intent {
        manifest,
        phase: Phase::Prepared,
    };
    store.save(&intent)?;
    Ok(intent)
}

async fn upload(
    source: &impl FrozenCheckpoint,
    transport: &impl SnapshotTransport,
    manifest: &SnapshotManifest,
    contents: &SnapshotContents,
    sealed: Vec<u8>,
) -> Result<()> {
    let cut = source.cut();
    for (held, file) in cut.files.iter().zip(&contents.files) {
        remaining(cut)?;
        let bytes = source.open_file(&file.path)?;
        upload_exact(
            transport,
            manifest,
            &file.object,
            held.destination,
            bytes,
            cut.expires_at_ms,
        )
        .await?;
    }
    remaining(cut)?;
    let bytes = Box::new(std::io::Cursor::new(sealed));
    upload_exact(
        transport,
        manifest,
        &manifest.encrypted_manifest,
        Destination::R2,
        bytes,
        cut.expires_at_ms,
    )
    .await?;
    source.require_hold()?;
    remaining(cut)?;
    Ok(())
}

async fn upload_exact(
    transport: &impl SnapshotTransport,
    manifest: &SnapshotManifest,
    expected: &SealedObject,
    destination: Destination,
    bytes: Box<dyn Read + Send>,
    deadline_ms: u64,
) -> Result<()> {
    let stored = transport
        .upload(
            &manifest.database_id,
            &manifest.snapshot_id,
            expected,
            destination,
            bytes,
            deadline_ms,
        )
        .await?;
    if &stored != expected {
        return Err(RestoreError::Invalid("uploaded shared snapshot object"));
    }
    Ok(())
}

fn require_identity(cut: &FrozenSnapshot, manifest: &SnapshotManifest) -> Result<()> {
    if cut.database_id != manifest.database_id
        || cut.snapshot_id != manifest.snapshot_id
        || cut.replaces != manifest.replaces
        || cut.covers != manifest.covers
        || manifest.disk_format != HOME_FORMAT
    {
        return Err(RestoreError::Invalid("saved shared snapshot identity"));
    }
    Ok(())
}

fn require_capture(cut: &FrozenSnapshot, contents: &SnapshotContents) -> Result<()> {
    if cut.profile != contents.native_profile
        || cut.progress != contents.progress
        || cut.delete_reclaimed_through_ms != contents.delete_reclaimed_through_ms
        || cut.schemas != contents.schemas
        || cut.files.len() != contents.files.len()
    {
        return Err(RestoreError::Invalid("frozen checkpoint inventory"));
    }
    let mut bytes = 0u64;
    for (expected, saved) in cut.files.iter().zip(&contents.files) {
        if expected.path != saved.path
            || expected.bytes != saved.object.bytes
            || expected.sha256 != saved.object.sha256
        {
            return Err(RestoreError::Invalid("frozen checkpoint file"));
        }
        bytes = bytes
            .checked_add(expected.bytes.get())
            .filter(|total| *total <= lastdb_storage_policy::BACKUP_TOTAL_STORED_BYTES)
            .ok_or(RestoreError::Limit("frozen checkpoint bytes"))?;
    }
    Ok(())
}

fn new_id() -> Result<Id> {
    Ok(Id::try_from(uuid::Uuid::new_v4().to_string())?)
}

fn remaining(cut: &FrozenSnapshot) -> Result<Duration> {
    cut.expires_at_ms
        .checked_sub(fold_db::clock::unix_millis())
        .filter(|duration| *duration != 0)
        .map(Duration::from_millis)
        .ok_or(RestoreError::Source("shared snapshot freeze deadline"))
}

async fn within_freeze<T>(
    cut: &FrozenSnapshot,
    work: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    tokio::time::timeout(remaining(cut)?, work)
        .await
        .map_err(|_| RestoreError::Source("shared snapshot freeze deadline"))?
}
