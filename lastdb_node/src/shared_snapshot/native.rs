//! Publish the original files held by the disk writer at one shared cut.

mod cut;

use super::{publish, state::IntentStore, state::Phase, FrozenCheckpoint, FrozenSnapshot};
use super::{SignedSnapshotTransport, SnapshotOutcome};
use crate::byte_cloud_restore::{local, RecoveryKeys, RestoreError, Result};
use cloud_backup_protocol::v2::Destination;
use cloud_backup_protocol::{Id, RelativePath};
use fold_db::fold_db_core::mutation_manager::portable_checkpoint::PortableCheckpoint;
use fold_db::storage::disk_pack::{byte_backup::SharedRunCapture, PackRuntime};
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;

const CUT: &str = "shared-checkpoint-cut.json";
const CUT_BYTES: u64 = crate::shared_restore::recovery::MAX_MANIFEST_BYTES
    + cloud_backup_protocol::limits::REQUEST_JSON_MAX_BYTES;

pub(crate) const ABANDONED: &str = "snapshot was abandoned. Use a new workspace";

pub(crate) struct PublishedFiles {
    pub r2: usize,
    pub b2: usize,
}

pub(crate) struct NativeCheckpoint {
    workspace: PathBuf,
    runtime: Arc<PackRuntime>,
    cut: FrozenSnapshot,
    capture: Option<SharedRunCapture>,
}

impl NativeCheckpoint {
    pub(crate) async fn prepare(
        runtime: &Arc<PackRuntime>,
        source: PortableCheckpoint,
        capture: SharedRunCapture,
        workspace: &Path,
        id: Id,
        deadline_ms: u64,
        transport: &SignedSnapshotTransport,
    ) -> Result<Self> {
        let (cut, capture) = cut::prepare(source, capture, id, deadline_ms, transport).await?;
        local::atomic_json_bounded(&workspace.join(CUT), &cut, CUT_BYTES)?;
        Ok(Self {
            workspace: workspace.to_path_buf(),
            runtime: Arc::clone(runtime),
            cut,
            capture: Some(capture),
        })
    }

    /// Reopen the exact disk hold if any file still needs uploading.
    /// A submitted or terminal snapshot only needs receipt reconciliation.
    pub(crate) async fn reopen(
        runtime: &Arc<PackRuntime>,
        workspace: &Path,
        cut: FrozenSnapshot,
        keys: &RecoveryKeys,
    ) -> Result<Self> {
        let intent = IntentStore::open(workspace)?.load()?;
        let needs_files = intent.is_none_or(|intent| matches!(intent.phase, Phase::Prepared));
        let capture = if needs_files {
            let capture = runtime
                .capture_shared_checkpoint_files(
                    snapshot_uuid(&cut.snapshot_id)?,
                    keys.content_key(),
                    true,
                )
                .await
                .map_err(RestoreError::native_proof)?;
            cut::require_same_inventory(&capture, &cut)?;
            Some(capture)
        } else {
            None
        };
        Ok(Self {
            workspace: workspace.to_path_buf(),
            runtime: Arc::clone(runtime),
            cut,
            capture,
        })
    }

    pub(crate) async fn upload(
        &mut self,
        transport: &SignedSnapshotTransport,
        keys: &RecoveryKeys,
    ) -> Result<PublishedFiles> {
        match publish(self, transport, keys.content_key()).await? {
            SnapshotOutcome::Published(_) => {
                let r2 = self
                    .cut
                    .files
                    .iter()
                    .filter(|file| file.destination == Destination::R2)
                    .count();
                Ok(PublishedFiles {
                    r2,
                    b2: self.cut.files.len() - r2,
                })
            }
            SnapshotOutcome::Abandoned => Err(RestoreError::Invalid(ABANDONED)),
        }
    }
}

#[async_trait::async_trait]
impl FrozenCheckpoint for NativeCheckpoint {
    fn cut(&self) -> &FrozenSnapshot {
        &self.cut
    }

    fn intent_directory(&self) -> &Path {
        &self.workspace
    }

    fn require_hold(&self) -> Result<()> {
        self.capture
            .as_ref()
            .map(|_| ())
            .ok_or(RestoreError::Invalid("snapshot disk hold is closed"))
    }

    fn open_file(&self, path: &RelativePath) -> Result<Box<dyn Read + Send>> {
        let capture = self
            .capture
            .as_ref()
            .ok_or(RestoreError::Invalid("snapshot disk hold is closed"))?;
        let file = capture
            .open_file(Path::new(path.as_str()))
            .map_err(RestoreError::native_proof)?;
        Ok(Box::new(file))
    }

    async fn thaw(&mut self, id: &Id) -> Result<()> {
        if id != &self.cut.snapshot_id {
            return Err(RestoreError::Invalid("snapshot thaw identity"));
        }
        drop(self.capture.take());
        release(&self.runtime, id).await
    }
}

pub(crate) async fn release(runtime: &Arc<PackRuntime>, id: &Id) -> Result<()> {
    runtime
        .release_shared_checkpoint_files(snapshot_uuid(id)?)
        .await
        .map_err(RestoreError::native_proof)
}

pub(crate) fn snapshot_uuid(id: &Id) -> Result<Uuid> {
    Uuid::parse_str(id.as_str()).map_err(|_| RestoreError::Invalid("snapshot identifier"))
}

pub(crate) fn saved_cut(workspace: &Path) -> Result<Option<FrozenSnapshot>> {
    local::directory(workspace)?;
    match local::read_json(&workspace.join(CUT), CUT_BYTES) {
        Ok(cut) => Ok(Some(cut)),
        Err(RestoreError::Io(error)) if error.kind() == ErrorKind::NotFound => {
            if IntentStore::open(workspace)?.load()?.is_some() {
                return Err(RestoreError::Invalid(
                    "snapshot intent is missing its disk cut",
                ));
            }
            let pending = workspace.join(CUT).with_extension("pending");
            match local::owner_file(&pending, false) {
                Ok(_) => std::fs::remove_file(pending)?,
                Err(RestoreError::Io(error)) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

/// Close an expired, unsubmitted freeze. A request already submitted must
/// reconcile its receipt before any release.
pub(crate) async fn expire(runtime: &Arc<PackRuntime>, workspace: &Path, id: &Id) -> Result<()> {
    let Some(cut) = saved_cut(workspace)? else {
        return release(runtime, id).await;
    };
    if &cut.snapshot_id != id {
        return Err(RestoreError::Invalid(
            "saved snapshot differs from its workspace",
        ));
    }
    if fold_db::clock::unix_millis() < cut.expires_at_ms {
        return Ok(());
    }

    let store = IntentStore::open(workspace)?;
    match store.load()? {
        None => {
            std::fs::remove_file(workspace.join(CUT))?;
            std::fs::File::open(workspace)?.sync_all()?;
        }
        Some(mut intent) => {
            if &intent.manifest.snapshot_id != id {
                return Err(RestoreError::Invalid("saved snapshot intent identity"));
            }
            match intent.phase {
                Phase::Submitting => return Ok(()),
                Phase::Prepared => {
                    intent.phase = Phase::Abandoned;
                    store.save(&intent)?;
                }
                Phase::Accepted(_) | Phase::Abandoned => (),
            }
        }
    }
    release(runtime, id).await
}
