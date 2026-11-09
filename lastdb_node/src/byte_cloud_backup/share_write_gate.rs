//! Applications hold new publication cuts while a multi-request edit is incomplete.
use super::connection::files::Directory;
use rustix::fs::{flock, FlockOperation};
use std::{fs::File, io, path::Path};

const LOCK: &str = "share-edit.lock";
const HOLD: &str = "share-edit.pending";
const MARKER: &[u8] = b"1\n";

pub struct ShareEdit {
    directory: Directory,
    _lock: File,
}

/// Serialize app edits and persist the hold before the first database write.
/// An interrupted edit leaves its hold for the app to recover.
pub fn begin_edit(home: &Path) -> Result<ShareEdit, String> {
    let directory = Directory::open(home).map_err(|e| e.to_string())?;
    let lock = directory.open_lock(LOCK).map_err(|e| e.to_string())?;
    flock(&lock, FlockOperation::NonBlockingLockExclusive)
        .map_err(|_| "a share edit or snapshot capture is active; retry the file import")?;
    match directory.read(HOLD).map_err(|e| e.to_string())? {
        Some(current) if current.as_slice() == MARKER => {}
        Some(_) => return Err("invalid pending share edit".into()),
        None => directory
            .publish(HOLD, None, MARKER)
            .map_err(|e| e.to_string())?,
    }
    Ok(ShareEdit {
        directory,
        _lock: lock,
    })
}

impl ShareEdit {
    /// Allow new snapshots after the app commits or recovers its whole edit.
    pub fn finish(self) -> Result<(), String> {
        let result = self
            .directory
            .remove_exact(HOLD, MARKER)
            .map_err(|e| e.to_string());
        drop(self);
        result
    }
}

/// Hold the app gate until a new native cut is captured. An existing exact
/// capture resumes without taking this gate.
pub(crate) fn try_capture(home: &Path) -> Result<Option<File>, String> {
    let directory = Directory::open(home).map_err(|e| e.to_string())?;
    let lock = directory.open_lock(LOCK).map_err(|e| e.to_string())?;
    match flock(&lock, FlockOperation::NonBlockingLockShared) {
        Err(error) if io::Error::from(error).kind() == io::ErrorKind::WouldBlock => {
            return Ok(None)
        }
        Err(_) => return Err("cannot lock share publication capture".into()),
        Ok(()) => {}
    }
    if directory.read(HOLD).map_err(|e| e.to_string())?.is_some() {
        return Ok(None);
    }
    Ok(Some(lock))
}
