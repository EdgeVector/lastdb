//! Explicit target ownership, encrypted checkpoint admission, and final marker.

use super::{
    conversion_copy::{Checkpoint, Phase, CHECKPOINT_COLLECTION, CHECKPOINT_KEY},
    invalid,
    source::budget,
    StorageResult,
};
use crate::storage::disk_pack::{
    map,
    run_admission::{CONVERSION_INTENT, TYPED_NODE_BYTES},
    run_node, NODE_MARKER,
};
use laststore::disk_run::home::RunHome;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;
use std::sync::Arc;

mod admission;
mod files;

pub(super) fn stage_fence(root: &Path, job: &str) -> StorageResult<()> {
    if !std::fs::symlink_metadata(root)?.is_dir() {
        return Err(invalid("staged target root is not a real directory"));
    }
    let owner = File::open(root)?;
    owner
        .try_lock()
        .map_err(|_| invalid("another converter owns the staged target"))?;
    if !files::only_initial_markers(root)? {
        return Err(invalid(
            "staged initialization contains unexpected database bytes",
        ));
    }
    if files::admit(root, job, true)? {
        return Err(invalid("a staging home cannot be a published target"));
    }
    Ok(())
}

pub(super) struct Target {
    pub(super) homes: [Arc<RunHome>; 2],
    pub(super) checkpoint: Checkpoint,
    published: bool,
    _owner: File,
}

impl Target {
    pub(super) fn check_binding(&self, root: &Path) -> StorageResult<()> {
        let held = self._owner.metadata()?;
        let current = std::fs::symlink_metadata(root)?;
        if !current.is_dir() || held.dev() != current.dev() || held.ino() != current.ino() {
            return Err(invalid("target directory changed during conversion"));
        }
        Ok(())
    }

    pub(super) fn open(
        root: &Path,
        key: &[u8; 32],
        job: &str,
        resume: bool,
    ) -> StorageResult<Self> {
        if !std::fs::symlink_metadata(root)?.is_dir() {
            return Err(invalid("target root is not a real directory"));
        }
        let owner = File::open(root)?;
        owner
            .try_lock()
            .map_err(|_| invalid("another converter owns the target"))?;
        let published = files::admit(root, job, resume)?;
        admission::check(root, key, job, published)?;
        files::clear_staging(root)?;
        let homes = [
            open_home(&root.join(run_node::RUN_DIR), &run_node::home_key(key))?,
            open_home(&root.join(run_node::CAS_DIR), &run_node::cas_key(key))?,
        ];
        let identities = [homes[0].identity(), homes[1].identity()];
        let row = homes[0]
            .read(budget(8 * 1024 * 1024, 1))
            .get_bounded(CHECKPOINT_COLLECTION, CHECKPOINT_KEY, 1024 * 1024)
            .map_err(map)?;
        let missing = row.is_none();
        if missing && homes.iter().any(|home| home.current_root().frontier() != 0) {
            return Err(invalid("a nonempty target has no encrypted progress"));
        }
        let checkpoint: Checkpoint = match row {
            Some(row) => serde_json::from_slice(&row)
                .map_err(|_| invalid("encrypted conversion progress is invalid"))?,
            None => Checkpoint {
                version: 1,
                job: job.into(),
                homes: identities,
                phase: Phase::Main,
                after: None,
                records: [0; 2],
                logical_bytes: [0; 2],
            },
        };
        admission::validate(&checkpoint, job, identities, published)?;
        if missing {
            super::conversion_copy::commit(&homes[0], vec![checkpoint.mutation()?])?;
        }
        Ok(Self {
            homes,
            checkpoint,
            published,
            _owner: owner,
        })
    }

    pub(super) fn reopen(self, root: &Path, key: &[u8; 32]) -> StorageResult<Self> {
        let Self {
            homes,
            checkpoint,
            published,
            _owner,
        } = self;
        drop(homes);
        let homes = [
            open_home(&root.join(run_node::RUN_DIR), &run_node::home_key(key))?,
            open_home(&root.join(run_node::CAS_DIR), &run_node::cas_key(key))?,
        ];
        if [homes[0].identity(), homes[1].identity()] != checkpoint.homes {
            return Err(invalid("target identities changed across reopen"));
        }
        let row = homes[0]
            .read(budget(8 * 1024 * 1024, 1))
            .get_bounded(CHECKPOINT_COLLECTION, CHECKPOINT_KEY, 1024 * 1024)
            .map_err(map)?
            .ok_or_else(|| invalid("target lost its encrypted progress"))?;
        if serde_json::from_slice::<serde_json::Value>(&row)
            .map_err(|_| invalid("invalid target progress after reopen"))?
            != serde_json::to_value(&checkpoint).map_err(|_| invalid("invalid target progress"))?
        {
            return Err(invalid("target progress differs after reopen"));
        }
        Ok(Self {
            homes,
            checkpoint,
            published,
            _owner,
        })
    }

    pub(super) fn publish(&self, root: &Path) -> StorageResult<()> {
        self.check_binding(root)?;
        if self.checkpoint.phase != Phase::Complete {
            return Err(invalid("target verification is incomplete"));
        }
        if self.published {
            return Ok(());
        }
        files::publish_marker(root)?;
        File::open(root)?.sync_all()?;
        std::fs::remove_file(root.join(CONVERSION_INTENT))?;
        File::open(root)?.sync_all()?;
        Ok(())
    }
}

fn open_home(path: &Path, key: &[u8; 32]) -> StorageResult<Arc<RunHome>> {
    let home = match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => RunHome::open(path, key),
        Ok(_) => return Err(invalid("target native home must be a real directory")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => RunHome::create(path, key),
        Err(error) => return Err(error.into()),
    };
    home.map(Arc::new).map_err(map)
}

fn write_new(path: &Path, bytes: &[u8]) -> StorageResult<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
