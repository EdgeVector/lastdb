//! Seal local references into the snapshot's private CAS publication.

use crate::storage::disk_pack::{admit, map, physical::V2Home, run_node};
use crate::storage::error::{StorageError, StorageResult};
use crate::storage::format::CAS_COLLECTION;
use crate::storage::local_file_ref;
use laststore::disk_run::home::{HomeMutation, RunHome, RunSnapshot, LOCK, RUNS};
use laststore::space::{reserve_space, SpacePolicy, SpaceReservation};
use std::fs::{self, File};
use std::io::{self, Seek};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

const BATCH_BYTES: usize = 16 << 20;

pub(super) struct Stage {
    path: PathBuf,
    _space: SpaceReservation,
}

impl Stage {
    fn create(parent: &Path, bytes: u64) -> StorageResult<Self> {
        let space = reserve_space(parent, &SpacePolicy::default(), bytes).map_err(map)?;
        let path = parent.join(format!(".snapshot-files-{}", uuid::Uuid::new_v4()));
        admit::create_private_dir(&path)?;
        Ok(Self {
            path,
            _space: space,
        })
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for Stage {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.path) {
            tracing::warn!(%error, "could not remove the owned snapshot file staging directory");
        }
    }
}

pub(super) fn capture(
    physical: &V2Home,
    source: &RunSnapshot,
    content_key: &[u8; 32],
) -> StorageResult<Option<(RunSnapshot, Stage)>> {
    let data = physical
        .path()
        .parent()
        .ok_or_else(|| StorageError::invalid("snapshot data root"))?;
    let Some(bytes) = local_file_ref::snapshot_budget(data)? else {
        return Ok(None);
    };
    let stage = Stage::create(data, bytes)?;
    copy_publication(physical.cas.path(), source, stage.path())?;
    let home = RunHome::open(stage.path(), &run_node::cas_key(content_key)).map_err(map)?;
    let mut pending = Vec::new();
    let mut bytes = 0;
    local_file_ref::snapshot_rows(data, content_key, |path, row| {
        if !pending.is_empty() && bytes + row.len() > BATCH_BYTES {
            home.commit(std::mem::take(&mut pending)).map_err(map)?;
            bytes = 0;
        }
        bytes += row.len();
        pending.push(HomeMutation::new(CAS_COLLECTION, path, Some(row)));
        Ok(())
    })?;
    if !pending.is_empty() {
        home.commit(pending).map_err(map)?;
    }
    Ok(Some((home.snapshot().map_err(map)?, stage)))
}

fn copy_publication(root: &Path, source: &RunSnapshot, target: &Path) -> StorageResult<()> {
    admit::create_private_dir(&target.join(RUNS))?;
    admit::open_private_write(&target.join(LOCK), true)?.sync_all()?;
    for (relative, length) in source.files().map_err(map)? {
        let mut input = source.open_file(&relative).map_err(map)?;
        let before = input.metadata()?;
        if !before.is_file() || before.len() != length {
            return Err(StorageError::invalid("snapshot file publication changed"));
        }
        let destination = target.join(&relative);
        if relative.starts_with(RUNS) {
            fs::hard_link(root.join(&relative), &destination)?;
            let copied = fs::symlink_metadata(&destination)?;
            if !copied.is_file() || (before.dev(), before.ino()) != (copied.dev(), copied.ino()) {
                return Err(StorageError::invalid("snapshot file hard link changed"));
            }
        } else {
            let mut output = admit::open_private_write(&destination, true)?;
            input.rewind()?;
            if io::copy(&mut input, &mut output)? != length {
                return Err(StorageError::invalid("snapshot file publication changed"));
            }
            input.rewind()?;
            output.sync_all()?;
        }
    }
    File::open(target.join(RUNS))?.sync_all()?;
    File::open(target)?.sync_all()?;
    Ok(())
}
