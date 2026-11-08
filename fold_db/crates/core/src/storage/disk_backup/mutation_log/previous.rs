//! Remove the version 1 mutation log that the previous program left.
//!
//! Version 1 kept its log directly in the run home's log directory
//! (`mutation-log-v1`): `index.json`, one directory per epoch named by its
//! UUID, and `captures`. Version 2 keeps its log in `main-v2` inside that
//! directory and never reads version 1. Removal uses exact paths only: the
//! `captures` directory, each epoch directory that the version 1 index
//! names, then the index last, so a repeat after a crash finds the same
//! paths. Nothing is converted, and `main-v2` is never touched.

use super::{invalid, store, MutationLog, StorageResult, LOG_ROOT};
use crate::storage::disk_pack::PackRuntime;
use cloud_backup_protocol::Id;
use serde::Deserialize;
use std::fs::{self, File};
use std::path::Path;

/// The fields of a version 1 index that locate its epochs. Unknown fields
/// are ignored.
#[derive(Deserialize)]
struct PreviousIndex {
    version: u16,
    epochs: Vec<Id>,
    cleanup: Vec<Id>,
}

impl MutationLog {
    fn retire_version_one(&self) -> StorageResult<bool> {
        let directory = self
            .root
            .parent()
            .ok_or_else(|| invalid("mutation log directory parent"))?;
        if !directory.try_exists()? {
            return Ok(false);
        }
        store::directory(directory)?;
        let index: Option<PreviousIndex> = store::read(&directory.join("index.json"))?;
        let mut removed = remove_tree(&directory.join("captures"))?;
        if let Some(index) = &index {
            if index.version != 1 {
                return Err(invalid("previous mutation log version"));
            }
            for epoch in index.epochs.iter().chain(&index.cleanup) {
                if uuid::Uuid::parse_str(epoch.as_str()).is_err() || epoch.as_str() == LOG_ROOT {
                    return Err(invalid("previous mutation log epoch name"));
                }
                removed |= remove_tree(&store::epoch(directory, epoch))?;
            }
        }
        for name in ["index.pending", "index.json"] {
            let path = directory.join(name);
            if fs::symlink_metadata(&path).is_ok() {
                store::remove_metadata(&path)?;
                removed = true;
            }
        }
        if removed {
            File::open(directory)?.sync_all()?;
        }
        Ok(removed)
    }
}

/// Remove one exact private log directory and everything in it. A symlink or
/// a directory with another owner or open permissions is refused; an absent
/// directory is already removed.
fn remove_tree(path: &Path) -> StorageResult<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => store::directory(path)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    }
    fs::remove_dir_all(path)?;
    Ok(true)
}

impl PackRuntime {
    /// Remove the version 1 mutation log of the previous program from a
    /// personal home. Returns whether anything was removed. It runs on the
    /// caller's thread: the automatic backup tick calls it once, while it
    /// retires the previous automatic policy and before it writes the new one.
    pub fn retire_previous_mutation_log(&self) -> StorageResult<bool> {
        self.require_ordinary_backup_home()?;
        let _admission = super::super::NativeCloudWorkGuard::acquire()?;
        self.mutation_log.retire_version_one()
    }
}
