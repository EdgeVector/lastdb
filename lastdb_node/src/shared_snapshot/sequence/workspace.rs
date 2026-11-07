//! Durable identity and post-publication upload target for one invocation.

use crate::byte_cloud_restore::{local, RestoreError, Result};
use cloud_backup_protocol::{shared::WriterStream, Id};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::ErrorKind;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use uuid::Uuid;

const STATE: &str = "sequence.json";
const LOCK: &str = "sequence.lock";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Sequence {
    snapshot_id: Id,
    home: Uuid,
    device: Id,
    stream: WriterStream,
    tail_through: Option<u64>,
}

pub(super) struct Workspace {
    root: PathBuf,
    state: Sequence,
    _lock: File,
}

impl Workspace {
    pub(super) fn open(
        root: &Path,
        home: Uuid,
        device: &Id,
        stream: &WriterStream,
    ) -> Result<Self> {
        if !root.is_absolute() || root.file_name().is_none() {
            return Err(RestoreError::Invalid("absolute snapshot workspace"));
        }
        crate::offline_home::refuse_primary(root)
            .map_err(|_| RestoreError::Invalid("snapshot workspace targets the primary"))?;
        let parent = root
            .parent()
            .ok_or(RestoreError::Invalid("snapshot workspace parent"))?;
        local::directory(parent)?;
        match fs::DirBuilder::new().mode(0o700).create(root) {
            Ok(()) => File::open(parent)?.sync_all()?,
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        local::directory(root)?;
        let lock_path = root.join(LOCK);
        let lock = match local::owner_file(&lock_path, true) {
            Ok(lock) => lock,
            Err(RestoreError::Io(error)) if error.kind() == ErrorKind::AlreadyExists => {
                local::owner_file(&lock_path, false)?
            }
            Err(error) => return Err(error),
        };
        lock.try_lock()
            .map_err(|_| RestoreError::Invalid("snapshot workspace already owned"))?;
        let path = root.join(STATE);
        clear_pending(&path)?;
        let state = match local::read_json::<Sequence>(&path, 4096) {
            Ok(state)
                if state.home == home && state.device == *device && state.stream == *stream =>
            {
                state
            }
            Ok(_) => {
                return Err(RestoreError::Invalid(
                    "snapshot workspace belongs to a different writer",
                ))
            }
            Err(RestoreError::Io(error)) if error.kind() == ErrorKind::NotFound => {
                if fs::read_dir(root)?
                    .any(|entry| entry.map_or(true, |entry| entry.file_name() != LOCK))
                {
                    return Err(RestoreError::Invalid(
                        "snapshot workspace requires an empty directory",
                    ));
                }
                let state = Sequence {
                    snapshot_id: Id::try_from(Uuid::new_v4().to_string())?,
                    home,
                    device: device.clone(),
                    stream: stream.clone(),
                    tail_through: None,
                };
                local::atomic_json(&path, &state)?;
                state
            }
            Err(error) => return Err(error),
        };
        Ok(Self {
            root: root.to_path_buf(),
            state,
            _lock: lock,
        })
    }

    pub(super) fn snapshot_id(&self) -> &Id {
        &self.state.snapshot_id
    }

    pub(super) fn database_id(&self) -> &Id {
        &self.state.stream.database_id
    }

    pub(super) fn checkpoint(&self) -> Result<PathBuf> {
        let root = self.root.join("checkpoint");
        match fs::DirBuilder::new().mode(0o700).create(&root) {
            Ok(()) => File::open(&self.root)?.sync_all()?,
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        local::directory(&root)?;
        Ok(root)
    }

    pub(super) fn tail_through(&mut self, observed: u64) -> Result<u64> {
        if let Some(through) = self.state.tail_through {
            return Ok(through);
        }
        self.state.tail_through = Some(observed);
        local::atomic_json(&self.root.join(STATE), &self.state)?;
        Ok(observed)
    }
}

fn clear_pending(path: &Path) -> Result<()> {
    let pending = path.with_extension("pending");
    match local::owner_file(&pending, false) {
        Ok(_) => Ok(fs::remove_file(pending)?),
        Err(RestoreError::Io(error)) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}
