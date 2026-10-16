//! Atomic generation capture and durable references for bounded file reclamation.
//! The separate writer lease serializes publications; this lock excludes deletion
//! for every active read, including readers in other processes.

use super::{artifact, corrupt, Current};
use crate::{durability, Result};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

mod inventory;
mod reference;

pub use inventory::{generation_inventory, GenerationFile, GenerationInventory};
pub(super) use reference::list_unlocked;
pub use reference::{
    list_references, release_reference, DurableReference, ReferenceInfo, ReferencePurpose,
};

/// One active call's protection. It holds no map pages, data, or dependency list.
/// Release occurs on drop, after all file access in that call ends.
pub struct GenerationHold {
    current: Current,
    root: PathBuf,
    _file: File,
}

impl GenerationHold {
    /// Acquire read protection before the atomic CURRENT capture.
    pub fn capture(root: &Path) -> Result<(Current, Self)> {
        let file = shared(root)?;
        let current = artifact::current(root)?;
        let hold = Self {
            current: current.clone(),
            root: root.to_path_buf(),
            _file: file,
        };
        Ok((current, hold))
    }

    /// Protect an explicitly retained generation. A retired root fails closed.
    pub fn acquire(root: &Path, current: &Current) -> Result<Self> {
        let file = shared(root)?;
        let held = super::generation::load_current(root, current.generation)?;
        if held != *current {
            return Err(corrupt("held generation differs from its receipt"));
        }
        Ok(Self {
            current: current.clone(),
            root: root.to_path_buf(),
            _file: file,
        })
    }

    /// Complete root identity selected before any dependent file access.
    pub fn current(&self) -> &Current {
        &self.current
    }

    /// Enumerate exact bounded dependencies while this call prevents deletion.
    pub fn inventory(&self) -> Result<GenerationInventory> {
        inventory::held_inventory(&self.root, &self.current)
    }
}

fn lock_file(root: &Path) -> Result<File> {
    let path = root.join("LIFETIME_LOCK");
    match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
    {
        Ok(file) => {
            durability::sync_dirty_file(&file)?;
            durability::sync_directory(&File::open(root)?)?;
            Ok(file)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            Ok(OpenOptions::new().read(true).write(true).open(path)?)
        }
        Err(error) => Err(error.into()),
    }
}

pub(super) fn shared(root: &Path) -> Result<File> {
    let file = lock_file(root)?;
    rustix::fs::flock(&file, rustix::fs::FlockOperation::LockShared)
        .map_err(std::io::Error::from)?;
    Ok(file)
}

/// The writer already owns the publication lease. Readers make reclaim defer.
pub(super) fn exclusive(root: &Path) -> Result<Option<File>> {
    let file = lock_file(root)?;
    match rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => Ok(Some(file)),
        Err(error) if error == rustix::io::Errno::WOULDBLOCK => Ok(None),
        Err(error) => Err(std::io::Error::from(error).into()),
    }
}
