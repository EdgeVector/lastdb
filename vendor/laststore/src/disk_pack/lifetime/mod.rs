//! Atomic generation capture and durable references for bounded file reclamation.
//! Standalone reads use a shared deletion lock. Writer-owned read capabilities
//! retain the exclusive writer lease and an atomic local read/reclaim gate.

use super::{artifact, corrupt, Current};
use crate::read_receipt_io::MeasuredFile;
use crate::{durability, Result};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

mod inventory;
mod reference;
mod writer_read;

pub use inventory::{generation_inventory, GenerationFile, GenerationInventory};
pub(super) use reference::list_unlocked;
pub use reference::{
    list_references, release_reference, DurableReference, ReferenceInfo, ReferencePurpose,
};
pub use writer_read::WriterReadLease;
pub(in crate::disk_pack) use writer_read::WriterReadState;

/// One active call's protection. It holds no map pages, data, or dependency list.
/// Release occurs on drop, after all file access in that call ends.
pub struct GenerationHold {
    current: Current,
    root: PathBuf,
    _guard: HoldGuard,
}

enum HoldGuard {
    File { _file: MeasuredFile },
    Writer { _read: writer_read::WriterReadGuard },
}

impl GenerationHold {
    /// Acquire read protection before the atomic CURRENT capture.
    pub fn capture(root: &Path) -> Result<(Current, Self)> {
        let file = shared(root)?;
        let current = artifact::current(root)?;
        let hold = Self {
            current: current.clone(),
            root: root.to_path_buf(),
            _guard: HoldGuard::File { _file: file },
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
            _guard: HoldGuard::File { _file: file },
        })
    }

    /// Protect a writer-selected root without file access. The caller captures
    /// its root under the same view lock that excludes maintenance handoff.
    pub fn from_writer(lease: &WriterReadLease, root: &Path, current: &Current) -> Result<Self> {
        let read = lease.hold(root, current)?;
        Ok(Self {
            current: current.clone(),
            root: root.to_path_buf(),
            _guard: HoldGuard::Writer { _read: read },
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

fn lock_file(root: &Path) -> Result<MeasuredFile> {
    let path = root.join("LIFETIME_LOCK");
    match crate::read_receipt_io::open(&path) {
        Ok(file) => return Ok(file),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
    {
        Ok(file) => {
            durability::sync_dirty_file(&file)?;
            durability::sync_directory(&File::open(root)?)?;
            Ok(crate::read_receipt_io::open(path)?)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            Ok(crate::read_receipt_io::open(path)?)
        }
        Err(error) => Err(error.into()),
    }
}

pub(super) fn initialize(root: &Path) -> Result<()> {
    drop(lock_file(root)?);
    Ok(())
}

pub(super) fn shared(root: &Path) -> Result<MeasuredFile> {
    let file = lock_file(root)?;
    rustix::fs::flock(&*file, rustix::fs::FlockOperation::LockShared)
        .map_err(std::io::Error::from)?;
    Ok(file)
}

/// The writer already owns the publication lease. Readers make reclaim defer.
pub(super) fn exclusive(root: &Path) -> Result<Option<MeasuredFile>> {
    let file = lock_file(root)?;
    match rustix::fs::flock(&*file, rustix::fs::FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => Ok(Some(file)),
        Err(error) if error == rustix::io::Errno::WOULDBLOCK => Ok(None),
        Err(error) => Err(std::io::Error::from(error).into()),
    }
}
