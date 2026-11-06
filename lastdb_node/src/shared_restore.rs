//! Replay a shared snapshot's exact captured tails into a private fresh home.

use crate::byte_cloud_restore::{local, publish, RestoreError, Result};
use fold_db::portable_mutation::{MutationEntry, SchemaIdentity, StreamId, StreamProgress};
use fold_db::portable_replay::{plan_replay, VerifiedLogRange};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};

mod native;
mod owner;
pub mod recovery;
pub use native::restore_from_source;
pub(crate) use native::{resolve_schemas, schema_id};
pub use owner::{restore_owner, OwnerRestoreSelection};
mod spool;
use spool::CandidateSpool;

const READY_FILE: &str = "shared-restore-ready.json";

/// Authenticated recovery selection and the replay positions read from its checkpoint.
#[derive(Clone)]
pub struct RestoreCut {
    pub captured_heads: BTreeMap<StreamId, u64>,
    pub progress: BTreeMap<StreamId, StreamProgress>,
    pub delete_reclaimed_through_ms: Option<u64>,
}

/// The writer owns native admission, validation and durability for the staged home.
#[async_trait::async_trait]
pub trait StagedReplayWriter: Send {
    fn home(&self) -> &Path;

    /// Read current live/delete stamps and commit only newer candidates.
    /// Fetch exact schema/file dependencies for the effective winners.
    async fn apply_candidates(&mut self, newest_first: &[&MutationEntry]) -> Result<()>;

    /// Persist the captured progress and fresh authoring stream, verify the
    /// resulting home, and close its native writer before returning.
    async fn finish(self, cut: &RestoreCut, local_stream: &StreamId) -> Result<()>;
}

/// Private restore workspace next to a destination that has never been opened.
pub struct SharedRestore {
    root: PathBuf,
    stage: PathBuf,
    ready: Ready,
    cut: RestoreCut,
    spool: CandidateSpool,
    _owner: File,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Ready {
    id: uuid::Uuid,
    destination: PathBuf,
}

impl SharedRestore {
    pub fn begin(destination: &Path, cut: RestoreCut) -> Result<Self> {
        validate_checkpoint(&cut)?;
        let destination = fresh_destination(destination)?;
        let id = uuid::Uuid::new_v4();
        let root = destination
            .parent()
            .ok_or(RestoreError::Invalid("destination parent"))?
            .join(format!(".shared-restore-{id}"));
        fs::DirBuilder::new().mode(0o700).create(&root)?;
        let owner = lock_workspace(&root, true)?;
        let stage = root.join("home");
        fs::DirBuilder::new().mode(0o700).create(&stage)?;
        let spool = CandidateSpool::new(&root)?;
        publish::sync_publish_parents(&root, &destination)?;
        Ok(Self {
            root,
            stage,
            ready: Ready { id, destination },
            cut,
            spool,
            _owner: owner,
        })
    }

    fn discard_unsealed(self) -> Result<()> {
        local::directory(&self.root)?;
        if self.root.join(READY_FILE).try_exists()? {
            return Err(RestoreError::Invalid("sealed restore workspace"));
        }
        fs::remove_dir_all(&self.root)?;
        Ok(())
    }

    /// Install only authenticated recoverable checkpoint files here.
    pub fn stage(&self) -> &Path {
        &self.stage
    }

    pub fn workspace(&self) -> &Path {
        &self.root
    }

    /// Admit another complete window from the captured selection. An earlier
    /// missing range must be supplied before any later window from that stream.
    pub fn ingest(&mut self, ranges: &[VerifiedLogRange]) -> Result<()> {
        for range in ranges {
            if self
                .cut
                .captured_heads
                .get(range.id().source_stream())
                .is_none_or(|head| range.id().last_sequence() > *head)
            {
                return Err(RestoreError::Invalid("range outside captured heads"));
            }
            if range
                .entries()
                .iter()
                .any(|entry| matches!(&entry.schema, SchemaIdentity::Local(_)))
            {
                return Err(RestoreError::Invalid("local schema in shared tail"));
            }
        }

        let plan = plan_replay(
            ranges,
            &self.cut.progress,
            self.cut.delete_reclaimed_through_ms,
        )
        .map_err(|_| RestoreError::Invalid("verified replay window"))?;
        for range in ranges {
            let source = range.id().source_stream();
            let covered = plan
                .progress()
                .get(source)
                .or_else(|| self.cut.progress.get(source))
                .map_or(0, |cursor| cursor.applied_through);
            if range.id().last_sequence() > covered {
                return Err(RestoreError::Invalid("gap in captured mutation tail"));
            }
        }

        self.spool.push_window(plan.candidates())?;
        self.cut.progress.extend(
            plan.progress()
                .iter()
                .map(|(source, cursor)| (source.clone(), *cursor)),
        );
        Ok(())
    }

    /// Replay globally newest first and publish the sealed home atomically.
    pub async fn finish(self, mut writer: impl StagedReplayWriter) -> Result<PathBuf> {
        require_complete(&self.cut)?;
        local::directory(&self.stage)?;
        if writer.home() != self.stage
            || self
                .stage
                .join(lastdb_identity::IDENTITY_KEY_FILE)
                .try_exists()?
            || crate::cloud::cloud_credentials_path(&self.stage).try_exists()?
        {
            return Err(RestoreError::Invalid("staged home or device credentials"));
        }

        let fresh_stream: StreamId = uuid::Uuid::new_v4()
            .to_string()
            .parse()
            .map_err(|_| RestoreError::Invalid("fresh authoring stream"))?;
        if self.cut.captured_heads.contains_key(&fresh_stream) {
            return Err(RestoreError::Invalid("fresh authoring stream collides"));
        }

        self.spool.replay(&mut writer).await?;
        writer.finish(&self.cut, &fresh_stream).await?;
        local::atomic_json(&self.stage.join(READY_FILE), &self.ready)?;
        local::atomic_json(&self.root.join(READY_FILE), &self.ready)?;
        activate_ready(&self.root, &self.ready)?;
        Ok(self.ready.destination)
    }

    /// Finish a directory publication interrupted after the staged writer sealed.
    pub fn resume_activation(workspace: &Path) -> Result<PathBuf> {
        local::directory(workspace)?;
        crate::offline_home::refuse_primary(workspace)
            .map_err(|_| RestoreError::Invalid("restore workspace"))?;
        let workspace = workspace.canonicalize()?;
        let _owner = lock_workspace(&workspace, false)?;
        let ready: Ready = local::read_json(&workspace.join(READY_FILE), 16 * 1024)?;
        activate_ready(&workspace, &ready)?;
        Ok(ready.destination)
    }
}

fn fresh_destination(destination: &Path) -> Result<PathBuf> {
    crate::offline_home::refuse_primary(destination)
        .map_err(|_| RestoreError::Invalid("destination"))?;
    let parent = destination
        .parent()
        .ok_or(RestoreError::Invalid("destination parent"))?
        .canonicalize()?;
    let name = destination
        .file_name()
        .ok_or(RestoreError::Invalid("destination name"))?;
    let destination = parent.join(name);
    match fs::symlink_metadata(&destination) {
        Ok(_) => Err(RestoreError::Invalid("destination exists")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(destination),
        Err(error) => Err(error.into()),
    }
}

fn lock_workspace(root: &Path, new: bool) -> Result<File> {
    let owner = local::owner_file(&root.join("owner.lock"), new)?;
    owner
        .try_lock()
        .map_err(|_| RestoreError::Invalid("shared restore already owned"))?;
    Ok(owner)
}

fn validate_checkpoint(cut: &RestoreCut) -> Result<()> {
    for (source, cursor) in &cut.progress {
        cursor
            .validate()
            .map_err(|_| RestoreError::Invalid("checkpoint cursor"))?;
        if cursor.uploaded_through.is_some()
            || cut
                .captured_heads
                .get(source)
                .is_none_or(|head| cursor.applied_through > *head)
        {
            return Err(RestoreError::Invalid(
                "checkpoint cursor outside recovery selection",
            ));
        }
    }
    Ok(())
}

fn require_complete(cut: &RestoreCut) -> Result<()> {
    if cut.captured_heads.iter().any(|(source, head)| {
        cut.progress
            .get(source)
            .map_or(0, |cursor| cursor.applied_through)
            != *head
    }) {
        return Err(RestoreError::Invalid("incomplete captured mutation tails"));
    }
    Ok(())
}

fn activate_ready(root: &Path, ready: &Ready) -> Result<()> {
    let root_name = format!(".shared-restore-{}", ready.id);
    if ready.id.is_nil()
        || root.file_name().and_then(|name| name.to_str()) != Some(root_name.as_str())
        || ready.destination.parent() != root.parent()
    {
        return Err(RestoreError::Invalid("ready restore selection"));
    }
    crate::offline_home::refuse_primary(&ready.destination)
        .map_err(|_| RestoreError::Invalid("destination"))?;
    let stage = root.join("home");
    match fs::symlink_metadata(&stage) {
        Ok(_) => {
            require_receipt(&stage, ready)?;
            publish::activate_owned_home(root, &ready.destination)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            require_receipt(&ready.destination, ready)?;
            publish::sync_publish_parents(root, &ready.destination)
        }
        Err(error) => Err(error.into()),
    }
}

fn require_receipt(home: &Path, ready: &Ready) -> Result<()> {
    local::directory(home)?;
    let receipt: Ready = local::read_json(&home.join(READY_FILE), 16 * 1024)?;
    if &receipt != ready {
        return Err(RestoreError::Invalid("ready home receipt"));
    }
    Ok(())
}
