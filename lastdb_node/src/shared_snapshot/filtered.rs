//! Persist a recoverable-only native export and its exact publication cut.

use super::{state::IntentStore, state::Phase, FrozenCheckpoint, FrozenFile, FrozenSnapshot};
use crate::byte_cloud_restore::{local, RecoveryKeys, RestoreError, Result};
use crate::shared_restore::recovery::SnapshotSchema;
use cloud_backup_protocol::shared::LogHead;
use cloud_backup_protocol::{DecimalU64, Digest, Id, RelativePath};
use fold_db::fold_db_core::mutation_manager::portable_checkpoint::PortableCheckpoint;
use fold_db::portable_mutation::StreamId;
use fold_db::storage::disk_pack::byte_backup::SharedRunCapture;
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Read;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};

mod build;
mod publish;
pub(super) use publish::prepare_from_source;
pub use publish::{prepare_native, publish_native, resume_native, NativePublishRequest};

const DATA: &str = "filtered-native";
const CUT: &str = "shared-checkpoint-cut.json";
const CUT_BYTES: u64 = crate::shared_restore::recovery::MAX_MANIFEST_BYTES
    + cloud_backup_protocol::limits::REQUEST_JSON_MAX_BYTES;

/// Remove an owned partial export that never reached a saved cut or upload.
pub(super) fn saved_cut(root: &Path) -> Result<Option<Id>> {
    if !root.try_exists()? {
        return Ok(None);
    }
    check_root(root)?;
    let _owner = match owner(root, false) {
        Err(RestoreError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            owner(root, true)?
        }
        result => result?,
    };
    match local::read_json::<FrozenSnapshot>(&root.join(CUT), CUT_BYTES) {
        Ok(cut) => Ok(Some(cut.snapshot_id)),
        Err(RestoreError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            if IntentStore::open(root)?.load()?.is_some() {
                return Err(RestoreError::Invalid("snapshot intent is missing its cut"));
            }
            fs::remove_dir_all(root)?;
            File::open(
                root.parent()
                    .ok_or(RestoreError::Invalid("checkpoint parent"))?,
            )?
            .sync_all()?;
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

pub struct FilteredCheckpointRequest {
    pub snapshot_id: Id,
    pub replaces: Option<Id>,
    pub expires_at_ms: u64,
    pub accepted: BTreeMap<StreamId, LogHead>,
}

pub struct FilteredCheckpoint {
    root: PathBuf,
    cut: FrozenSnapshot,
    capture: Option<SharedRunCapture>,
    _owner: File,
}

impl FilteredCheckpoint {
    /// Build from one held source root into a new private export. Only the
    /// completed native cut can acquire a publication intent.
    pub async fn prepare(
        root: &Path,
        mut source: PortableCheckpoint,
        request: FilteredCheckpointRequest,
        keys: &RecoveryKeys,
    ) -> Result<Self> {
        let database_id = source.writer().database_id.clone();
        let covers = source
            .covers(&request.accepted)
            .map_err(|_| RestoreError::Invalid("shared checkpoint accepted ends"))?;
        let progress = source.progress().clone();
        let delete_reclaimed_through_ms = source.delete_reclaimed_through_ms();
        let mut schemas: Vec<_> = source
            .definitions()
            .iter()
            .map(|(local_schema, definition)| SnapshotSchema {
                local_schema: local_schema.clone(),
                definition: definition.clone(),
            })
            .collect();
        schemas.sort_unstable_by(|left, right| left.definition.id().cmp(right.definition.id()));
        check_root(root)?;
        fs::DirBuilder::new().mode(0o700).create(root)?;
        File::open(
            root.parent()
                .ok_or(RestoreError::Invalid("checkpoint parent"))?,
        )?
        .sync_all()?;
        let owner = owner(root, true)?;
        fs::DirBuilder::new().mode(0o700).create(root.join(DATA))?;
        let result = async {
            let db = build::open(&root.join(DATA), keys).await?;
            let capture = async {
                build::copy(&db, &mut source, &schemas, request.expires_at_ms).await?;
                db.db_ops()
                    .native_runtime()
                    .capture_shared_checkpoint_files(
                        uuid(&request.snapshot_id)?,
                        keys.content_key(),
                        false,
                    )
                    .await
                    .map_err(RestoreError::native_proof)
            }
            .await;
            drop(source);
            let shutdown = db.shutdown().await.map_err(RestoreError::native_proof);
            drop(db);
            let capture = capture?;
            shutdown?;
            let profile = capture.profile().map_err(RestoreError::native_proof)?;
            let (capture, files) = hash_inventory(capture, request.expires_at_ms).await?;
            let cut = FrozenSnapshot {
                database_id,
                snapshot_id: request.snapshot_id,
                replaces: request.replaces,
                expires_at_ms: request.expires_at_ms,
                profile,
                covers,
                progress,
                delete_reclaimed_through_ms,
                files,
                schemas,
            };
            local::atomic_json_bounded(&root.join(CUT), &cut, CUT_BYTES)?;
            Ok((cut, capture))
        }
        .await;
        match result {
            Ok((cut, capture)) => Ok(Self {
                root: root.to_path_buf(),
                cut,
                capture: Some(capture),
                _owner: owner,
            }),
            Err(error) => {
                retire_owned(root).await?;
                Err(error)
            }
        }
    }

    /// Reopen exactly the saved export. After a terminal remote outcome, a
    /// restart can finish retiring the export even if its files are gone.
    pub async fn resume(root: &Path, keys: &RecoveryKeys) -> Result<Self> {
        check_root(root)?;
        let owner = owner(root, false)?;
        let cut: FrozenSnapshot = local::read_json(&root.join(CUT), CUT_BYTES)?;
        let terminal = IntentStore::open(root)?
            .load()?
            .is_some_and(|intent| matches!(intent.phase, Phase::Accepted(_) | Phase::Abandoned));
        let capture = if terminal {
            None
        } else {
            let db = build::open(&root.join(DATA), keys).await?;
            db.mutation_manager()
                .validate_portable_restore_checkpoint(
                    &cut.progress,
                    cut.delete_reclaimed_through_ms,
                )
                .await
                .map_err(|_| RestoreError::Invalid("saved checkpoint native cut changed"))?;

            let capture = db
                .db_ops()
                .native_runtime()
                .capture_shared_checkpoint_files(uuid(&cut.snapshot_id)?, keys.content_key(), true)
                .await
                .map_err(RestoreError::native_proof)?;
            db.shutdown().await.map_err(RestoreError::native_proof)?;
            drop(db);
            // Submission reconciliation remains possible after the transfer deadline.
            let (capture, files) = hash_inventory(capture, u64::MAX).await?;
            if capture.profile().map_err(RestoreError::native_proof)? != cut.profile
                || files != cut.files
            {
                return Err(RestoreError::Invalid(
                    "saved checkpoint native files changed",
                ));
            }
            Some(capture)
        };
        Ok(Self {
            root: root.to_path_buf(),
            cut,
            capture,
            _owner: owner,
        })
    }
}

#[async_trait::async_trait]
impl FrozenCheckpoint for FilteredCheckpoint {
    fn cut(&self) -> &FrozenSnapshot {
        &self.cut
    }

    fn intent_directory(&self) -> &Path {
        &self.root
    }

    fn require_recoverable_cut(&self) -> Result<()> {
        if self.capture.is_none() {
            return Err(RestoreError::Invalid(
                "filtered checkpoint has been retired",
            ));
        }
        Ok(())
    }

    fn open_file(&self, path: &RelativePath) -> Result<Box<dyn Read + Send>> {
        let capture = self.capture.as_ref().ok_or(RestoreError::Invalid(
            "filtered checkpoint has been retired",
        ))?;
        let file = capture
            .open_file(Path::new(path.as_str()))
            .map_err(RestoreError::native_proof)?;
        Ok(Box::new(file))
    }

    async fn thaw(&mut self, snapshot_id: &Id) -> Result<()> {
        if snapshot_id != &self.cut.snapshot_id {
            return Err(RestoreError::Invalid(
                "filtered checkpoint publication changed",
            ));
        }
        drop(self.capture.take());
        retire_owned(&self.root).await
    }
}

async fn retire_owned(root: &Path) -> Result<()> {
    let root = root.to_path_buf();
    tokio::task::spawn_blocking(move || retire(&root))
        .await
        .map_err(|_| RestoreError::Invalid("retire filtered checkpoint task"))?
}

fn retire(root: &Path) -> Result<()> {
    let data = root.join(DATA);
    match local::directory(&data) {
        Ok(()) => {
            fs::remove_dir_all(data)?;
            File::open(root)?.sync_all()?;
        }
        Err(RestoreError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    Ok(())
}

async fn hash_inventory(
    capture: SharedRunCapture,
    expires: u64,
) -> Result<(SharedRunCapture, Vec<FrozenFile>)> {
    tokio::task::spawn_blocking(move || {
        let files = inventory(&capture, expires)?;
        Ok((capture, files))
    })
    .await
    .map_err(|_| RestoreError::Invalid("hash filtered checkpoint task"))?
}

fn inventory(capture: &SharedRunCapture, expires: u64) -> Result<Vec<FrozenFile>> {
    if capture.files().count() > cloud_backup_protocol::limits::NATIVE_FILES_MAX {
        return Err(RestoreError::Limit("filtered checkpoint inventory"));
    }
    let mut buffer = vec![0u8; 64 * 1024];
    let mut files = Vec::new();
    for (path, bytes) in capture.files() {
        let mut input = capture
            .open_file(path)
            .map_err(RestoreError::native_proof)?;
        let mut sha = Sha256::new();
        let mut length = 0;
        loop {
            build::require_time(expires)?;
            let n = input.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            length += n as u64;
            if length > bytes {
                return Err(RestoreError::Invalid("filtered native file grew"));
            }
            sha.update(&buffer[..n]);
        }
        if length != bytes {
            return Err(RestoreError::Invalid("filtered native file length"));
        }
        let path = path
            .to_str()
            .ok_or(RestoreError::Invalid("filtered native path"))?;
        files.push(FrozenFile {
            path: RelativePath::try_from(path.to_owned())?,
            bytes: DecimalU64::new(bytes),
            sha256: Digest::try_from(fold_db::hex::hex_lower(sha.finalize()))?,
        });
    }
    files.sort_unstable_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

fn check_root(root: &Path) -> Result<()> {
    if !root.is_absolute() {
        return Err(RestoreError::Invalid(
            "filtered checkpoint requires an absolute path",
        ));
    }
    crate::offline_home::refuse_primary(root)
        .map_err(|_| RestoreError::Invalid("filtered checkpoint targets the primary"))
}

fn owner(root: &Path, new: bool) -> Result<File> {
    local::directory(root)?;
    let owner = local::owner_file(&root.join("filtered-native.lock"), new)?;
    owner
        .try_lock()
        .map_err(|_| RestoreError::Invalid("filtered checkpoint is already owned"))?;
    Ok(owner)
}

fn uuid(id: &Id) -> Result<uuid::Uuid> {
    uuid::Uuid::parse_str(id.as_str()).map_err(|_| RestoreError::Invalid("checkpoint identifier"))
}
