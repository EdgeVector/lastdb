use super::{
    local, source_error, ByteBackupClient, RecoveryKeys, RestoreError, RestoreReport, Result,
};
use cloud_backup_protocol::v2::{
    publication::{CommitResponse, CommitStatusRequest},
    AcceptedBackup, Version,
};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Selection {
    version: u8,
    pub session_id: uuid::Uuid,
    pub accepted: AcceptedBackup,
    pub destination: PathBuf,
    pub endpoint: Option<String>,
}

/// Durable exact V2 selection. A resume never discovers or substitutes a new head.
pub struct RestoreSession {
    pub(super) root: PathBuf,
    pub(super) selection: Selection,
    _lock: File,
    pub(super) spool_bytes: AtomicU64,
}

impl RestoreSession {
    pub fn begin(destination: &Path, accepted: AcceptedBackup) -> Result<Self> {
        accepted.validate()?;
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
            Ok(_) => return Err(RestoreError::Invalid("destination exists")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let id = uuid::Uuid::new_v4();
        let root = parent.join(format!(".byte-cloud-restore-{id}"));
        fs::DirBuilder::new().mode(0o700).create(&root)?;
        let lock = local::owner_file(&root.join("owner.lock"), true)?;
        lock.try_lock()
            .map_err(|_| RestoreError::Invalid("session already owned"))?;
        let selection = Selection {
            version: 2,
            session_id: id,
            accepted,
            destination,
            endpoint: None,
        };
        local::atomic_json(&root.join("selection.json"), &selection)?;
        scaffold(&root)?;
        File::open(&root)?.sync_all()?;
        File::open(&parent)?.sync_all()?;
        Ok(Self {
            root,
            selection,
            _lock: lock,
            spool_bytes: AtomicU64::new(0),
        })
    }

    pub fn resume(root: &Path) -> Result<Self> {
        local::directory(root)?;
        crate::offline_home::refuse_primary(root)
            .map_err(|_| RestoreError::Invalid("session directory"))?;
        let root = root.canonicalize()?;
        let lock = local::owner_file(&root.join("owner.lock"), false)?;
        lock.try_lock()
            .map_err(|_| RestoreError::Invalid("session already owned"))?;
        let selection: Selection = local::read_json(&root.join("selection.json"), 256 * 1024)?;
        selection.accepted.validate()?;
        if selection.version != 2
            || selection.session_id.is_nil()
            || root.file_name().and_then(|name| name.to_str())
                != Some(&format!(".byte-cloud-restore-{}", selection.session_id))
            || selection.destination.parent() != root.parent()
            || selection
                .endpoint
                .as_ref()
                .is_some_and(|value| value.is_empty() || value.len() > 8192)
        {
            return Err(RestoreError::Invalid("saved selection"));
        }
        crate::offline_home::refuse_primary(&selection.destination)
            .map_err(|_| RestoreError::Invalid("destination"))?;
        if !selection.destination.try_exists()? {
            scaffold(&root)?;
        }
        let session = Self {
            root,
            selection,
            _lock: lock,
            spool_bytes: AtomicU64::new(0),
        };
        session.check_spool()?;
        Ok(session)
    }

    pub fn path(&self) -> &Path {
        &self.root
    }
    pub fn accepted(&self) -> &AcceptedBackup {
        &self.selection.accepted
    }
    pub(super) fn physical(&self) -> PathBuf {
        self.root.join("home/data/disk-pack")
    }

    pub async fn restore(
        &mut self,
        client: &ByteBackupClient,
        keys: &RecoveryKeys,
    ) -> Result<RestoreReport> {
        let _admission = fold_db::storage::disk_pack::NativeCloudWorkGuard::acquire()
            .map_err(|_| RestoreError::Limit("active native cloud operation"))?;
        self.bind_endpoint(client)?;
        self.check_spool()?;
        let request = CommitStatusRequest {
            version: Version::V2,
            job: self.accepted().job().clone(),
        };
        let CommitResponse::Accepted { backup } =
            client.commit_status(&request).await.map_err(source_error)?
        else {
            return Err(RestoreError::Invalid("authoritative accepted selection"));
        };
        if *backup != *self.accepted() {
            return Err(RestoreError::Invalid("authoritative accepted selection"));
        }
        super::closure::restore(self, client, keys).await
    }

    fn bind_endpoint(&mut self, client: &ByteBackupClient) -> Result<()> {
        let endpoint = client.canonical_endpoint();
        if self
            .selection
            .endpoint
            .as_ref()
            .is_some_and(|saved| *saved != endpoint)
        {
            return Err(RestoreError::Invalid("restore endpoint"));
        }
        if self.selection.endpoint.is_none() {
            self.selection.endpoint = Some(endpoint);
            local::atomic_json(&self.root.join("selection.json"), &self.selection)?;
        }
        Ok(())
    }
}

fn scaffold(root: &Path) -> Result<()> {
    for name in [
        "cache",
        "entries",
        "home",
        "home/data",
        "home/data/disk-pack",
    ] {
        let path = root.join(name);
        match fs::DirBuilder::new().mode(0o700).create(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        local::directory(&path)?;
    }
    Ok(())
}
