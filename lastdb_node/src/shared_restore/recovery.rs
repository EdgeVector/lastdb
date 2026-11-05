//! Verify and load one authenticated shared recovery selection into staging.

use super::{RestoreCut, SharedRestore};
use crate::byte_cloud_restore::{check_capacity, local, RestoreError, Result};
use cloud_backup_protocol::shared::{
    LogBatch, LogHead, RecoveryObjectParent, RecoveryView, SealedObject, SnapshotManifest,
};
use cloud_backup_protocol::{DecimalU64, Id};
use fold_db::portable_mutation::{MutationOperation, Position, StreamId};
use fold_db::portable_replay::{LogRangeId, VerifiedLogRange, MAX_LOG_BYTES};
use fold_db::storage::disk_pack::{
    run_restore, NativeCloudWorkGuard, CAS_DIR, HOME_FORMAT, RUN_DIR,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::Path;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncReadExt};

mod manifest;
pub(crate) use manifest::open_log_plaintext;
pub use manifest::{seal_log, SnapshotContents, SnapshotFile, SnapshotSchema};

pub(crate) const MAX_MANIFEST_BYTES: u64 =
    cloud_backup_protocol::limits::DECODED_VALUES_MAX_BYTES / 2;
pub(crate) const MAX_SEALED_LOG_BYTES: u64 = MAX_LOG_BYTES as u64 + 64;

/// A response body from the authenticated service or its exact-object grant.
pub type RecoveryObject = Pin<Box<dyn AsyncRead + Send>>;

/// A source authenticates the caller and scopes every operation to its service
/// hold. The loader independently checks the manifest, chains and bytes.
#[async_trait::async_trait]
pub trait SharedRecoverySource: Sync {
    async fn select(&self, database_id: &Id) -> Result<(RecoveryView, SnapshotManifest)>;

    async fn next_batch(
        &self,
        view: &RecoveryView,
        source_stream: &Id,
        previous: Option<&LogHead>,
    ) -> Result<LogBatch>;

    async fn open_object(
        &self,
        view: &RecoveryView,
        object: &SealedObject,
        parent: &RecoveryObjectParent,
    ) -> Result<RecoveryObject>;
}

/// A native checkpoint and all verified log tails, still private to restoration.
pub struct PreparedSharedRestore<'a, S> {
    pub session: SharedRestore,
    pub checkpoint: RestoreCut,
    pub schemas: Vec<SnapshotSchema>,
    pub files: HeldFileVersions<'a, S>,
}

/// Exact retained file versions declared by verified log batches in this hold.
pub struct HeldFileVersions<'a, S> {
    source: &'a S,
    view: RecoveryView,
    objects: BTreeMap<Position, HeldObject>,
    _admission: NativeCloudWorkGuard,
}

struct HeldObject {
    object: SealedObject,
    parent: RecoveryObjectParent,
}

impl<S: SharedRecoverySource> HeldFileVersions<'_, S> {
    /// Return exact sealed bytes within the staged writer's admission budget.
    /// The writer verifies them against the winning record's file marker.
    pub async fn fetch(&self, position: &Position, maximum: u64) -> Result<Vec<u8>> {
        let held = self.object(position)?;
        read_object(self.source, &self.view, &held.object, &held.parent, maximum).await
    }

    pub(super) fn bytes(&self, position: &Position) -> Result<u64> {
        Ok(self.object(position)?.object.bytes.get())
    }

    fn object(&self, position: &Position) -> Result<&HeldObject> {
        self.objects
            .get(position)
            .ok_or(RestoreError::Invalid("missing held file version"))
    }
}

pub async fn prepare<'a, S: SharedRecoverySource>(
    source: &'a S,
    database_id: &Id,
    content_key: &[u8; 32],
    destination: &Path,
) -> Result<PreparedSharedRestore<'a, S>> {
    let destination = super::fresh_destination(destination)?;
    let admission = NativeCloudWorkGuard::acquire()
        .map_err(|_| RestoreError::Limit("active native cloud operation"))?;
    let (view, manifest) = source.select(database_id).await?;
    view.validate_for(&manifest, now_ms())?;
    if &manifest.database_id != database_id || manifest.disk_format != HOME_FORMAT {
        return Err(RestoreError::Invalid(
            "shared snapshot scope or disk format",
        ));
    }

    let sealed = read_object(
        source,
        &view,
        &manifest.encrypted_manifest,
        &snapshot_parent(&view),
        MAX_MANIFEST_BYTES,
    )
    .await?;
    let contents = SnapshotContents::open(&sealed, &manifest, content_key)?;
    let checkpoint = contents.cut(&view)?;
    let mut session = SharedRestore::begin(&destination, checkpoint.clone())?;
    let prepared = async {
        install_checkpoint(source, &view, &contents, content_key, session.stage()).await?;
        let objects = load_tails(source, &view, &manifest, content_key, &mut session).await?;
        require_live(&view)?;
        Ok(objects)
    }
    .await;
    let objects = match prepared {
        Ok(objects) => objects,
        Err(error) => {
            session.discard_unsealed()?;
            return Err(error);
        }
    };
    Ok(PreparedSharedRestore {
        session,
        checkpoint,
        schemas: contents.schemas,
        files: HeldFileVersions {
            source,
            view,
            objects,
            _admission: admission,
        },
    })
}

async fn install_checkpoint(
    source: &impl SharedRecoverySource,
    view: &RecoveryView,
    contents: &SnapshotContents,
    key: &[u8; 32],
    stage: &Path,
) -> Result<()> {
    let bytes = contents.files.iter().try_fold(0u64, |total, file| {
        total
            .checked_add(file.object.bytes.get())
            .filter(|total| *total <= lastdb_storage_policy::BACKUP_TOTAL_STORED_BYTES)
            .ok_or(RestoreError::Limit("native checkpoint bytes"))
    })?;
    check_capacity(stage, bytes)?;
    let data = stage.join("data");
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    builder.create(&data)?;
    for home in [RUN_DIR, CAS_DIR] {
        builder.create(data.join(home))?;
    }
    local::prepare_native_ephemera(&data)?;
    let parent = snapshot_parent(view);
    for member in &contents.files {
        let destination = data.join(member.path.as_str());
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(destination)?;
        copy_exact(source, view, &member.object, &parent, &mut file).await?;
        file.sync_all()?;
    }

    let native_root = contents
        .native_profile
        .native_root()
        .map_err(RestoreError::native_proof)?;
    let verified = run_restore::verify(&data, &contents.native_profile, &native_root, key)
        .map_err(RestoreError::native_proof)?;
    let (files, verified_bytes) = verified.totals().map_err(RestoreError::native_proof)?;
    if files != contents.files.len() as u64 || verified_bytes != bytes {
        return Err(RestoreError::Invalid(
            "verified native checkpoint inventory",
        ));
    }
    for file in verified.files() {
        let listed = contents
            .files
            .binary_search_by(|listed| listed.path.as_str().cmp(file.path.as_str()))
            .ok()
            .and_then(|index| contents.files.get(index))
            .is_some_and(|listed| listed.object.bytes.get() == file.bytes);
        if !listed {
            return Err(RestoreError::Invalid("unlisted native checkpoint file"));
        }
    }
    verified.check_binding().map_err(RestoreError::native_proof)
}

async fn load_tails(
    source: &impl SharedRecoverySource,
    view: &RecoveryView,
    manifest: &SnapshotManifest,
    key: &[u8; 32],
    session: &mut SharedRestore,
) -> Result<BTreeMap<Position, HeldObject>> {
    let mut objects = BTreeMap::new();
    for cut in &view.streams {
        let mut previous = manifest.cover(&cut.source_stream).cloned();
        while previous.as_ref() != cut.head.as_ref() {
            require_live(view)?;
            let batch = source
                .next_batch(view, &cut.source_stream, previous.as_ref())
                .await?;
            let next = cut.validate_next(&manifest.database_id, previous.as_ref(), &batch)?;
            let first = previous
                .as_ref()
                .map_or(0, |head| head.sequence.get())
                .checked_add(1)
                .ok_or(RestoreError::Invalid("shared log sequence"))?;
            let id = LogRangeId::new(stream_id(&cut.source_stream)?, first, next.sequence.get())
                .map_err(|_| RestoreError::Invalid("shared log range"))?;
            let parent = RecoveryObjectParent::LogBatch {
                source_stream: cut.source_stream.clone(),
                head: next.clone(),
            };
            let sealed =
                read_object(source, view, &batch.segment, &parent, MAX_SEALED_LOG_BYTES).await?;
            let range = manifest::open_log(key, &manifest.database_id, id, &sealed)?;
            bind_file_versions(&range, batch, &parent, &mut objects)?;
            session.ingest(std::slice::from_ref(&range))?;
            previous = Some(next);
        }
    }
    Ok(objects)
}

fn bind_file_versions(
    range: &VerifiedLogRange,
    batch: LogBatch,
    parent: &RecoveryObjectParent,
    objects: &mut BTreeMap<Position, HeldObject>,
) -> Result<()> {
    for (position, object) in exact_file_versions(range, batch)? {
        if let Some(saved) = objects.get(&position) {
            if saved.object != object {
                return Err(RestoreError::Invalid("conflicting shared log file version"));
            }
        } else {
            if objects.len() == cloud_backup_protocol::limits::NATIVE_FILES_MAX {
                return Err(RestoreError::Limit("held file versions"));
            }
            objects.insert(
                position,
                HeldObject {
                    object,
                    parent: parent.clone(),
                },
            );
        }
    }
    Ok(())
}

pub(crate) fn exact_file_versions(
    range: &VerifiedLogRange,
    batch: LogBatch,
) -> Result<Vec<(Position, SealedObject)>> {
    let needed: BTreeSet<_> = range
        .entries()
        .iter()
        .filter_map(|entry| match &entry.operation {
            MutationOperation::ReplaceRecord { data, .. } => {
                data.file.as_ref().map(|marker| &marker.version)
            }
            MutationOperation::DeleteRecord { .. } => None,
        })
        .collect();
    if needed.len() != batch.file_versions.len() {
        return Err(RestoreError::Invalid("shared log file version coverage"));
    }
    batch
        .file_versions
        .into_iter()
        .map(|dependency| {
            let position = Position {
                source_stream: stream_id(&dependency.source_stream)?,
                sequence: dependency.sequence.get(),
            };
            if !needed.contains(&position) {
                return Err(RestoreError::Invalid("undeclared shared log file version"));
            }
            Ok((position, dependency.object))
        })
        .collect()
}

async fn read_object(
    source: &impl SharedRecoverySource,
    view: &RecoveryView,
    object: &SealedObject,
    parent: &RecoveryObjectParent,
    maximum: u64,
) -> Result<Vec<u8>> {
    if object.bytes.get() > maximum {
        return Err(RestoreError::Limit("shared object decode"));
    }
    let mut bytes = Vec::new();
    copy_exact(source, view, object, parent, &mut bytes).await?;
    Ok(bytes)
}

async fn copy_exact(
    source: &impl SharedRecoverySource,
    view: &RecoveryView,
    object: &SealedObject,
    parent: &RecoveryObjectParent,
    output: &mut impl Write,
) -> Result<()> {
    require_live(view)?;
    object.validate()?;
    let mut input = source.open_object(view, object, parent).await?;
    let mut left = object.bytes.get();
    let mut digest = Sha256::new();
    let mut buffer = vec![0u8; cloud_backup_protocol::limits::STREAM_BUFFER_MAX_BYTES as usize];
    while left > 0 {
        let maximum = left.min(buffer.len() as u64) as usize;
        let count = input.read(&mut buffer[..maximum]).await?;
        if count == 0 {
            return Err(RestoreError::Invalid("short shared object"));
        }
        output.write_all(&buffer[..count])?;
        digest.update(&buffer[..count]);
        left -= count as u64;
    }
    if input.read(&mut buffer[..1]).await? != 0 {
        return Err(RestoreError::Invalid("long shared object"));
    }
    if hex::encode(digest.finalize()) != object.sha256.as_str() {
        return Err(RestoreError::Invalid("shared object digest"));
    }
    require_live(view)
}

fn snapshot_parent(view: &RecoveryView) -> RecoveryObjectParent {
    RecoveryObjectParent::Snapshot {
        snapshot_id: view.snapshot.snapshot_id.clone(),
    }
}

fn require_live(view: &RecoveryView) -> Result<()> {
    if view.expires_at_ms <= now_ms() {
        return Err(RestoreError::Source("shared recovery hold expired"));
    }
    Ok(())
}

fn now_ms() -> DecimalU64 {
    DecimalU64::new(fold_db::clock::unix_millis())
}

fn stream_id(id: &Id) -> Result<StreamId> {
    id.as_str()
        .parse()
        .map_err(|_| RestoreError::Invalid("shared source stream"))
}
