//! Exclusive edit hold for one share file import.

use crate::offline_home::Directory;
use rustix::fs::{flock, FlockOperation};
use std::{fs::File, path::Path};

const LOCK: &str = "share-edit.lock";
const HOLD: &str = "share-edit.pending";
const MARKER: &[u8] = b"1\n";

pub struct ShareEdit {
    directory: Directory,
    _lock: File,
}

fn open_gate(home: &Path) -> Result<(Directory, File), String> {
    let dir = Directory::open(home).map_err(|e| e.to_string())?;
    let lock = dir.open_lock(LOCK).map_err(|e| e.to_string())?;
    Ok((dir, lock))
}

pub fn begin_edit(home: &Path) -> Result<ShareEdit, String> {
    let (directory, _lock) = open_gate(home)?;
    flock(&_lock, FlockOperation::NonBlockingLockExclusive)
        .map_err(|_| "a share edit is active; retry the file import")?;
    match directory.read(HOLD).map_err(|e| e.to_string())? {
        Some(cur) if cur.as_slice() == MARKER => {}
        Some(_) => return Err("invalid pending share edit".into()),
        None => directory
            .publish(HOLD, None, MARKER)
            .map_err(|e| e.to_string())?,
    }
    Ok(ShareEdit { directory, _lock })
}

impl ShareEdit {
    pub fn finish(self) -> Result<(), String> {
        self.directory
            .remove_exact(HOLD, MARKER)
            .map_err(|e| e.to_string())
    }
}
