//! Sealed files of spool entries: one file per entry that names a file, in one folder beside
//! the main home. A file is written before the commit of its entry, as an ordered write, so the
//! commit cannot reach the drive before it.

use super::Error;
use crate::storage::disk_pack::admit;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

pub(super) fn path(dir: &Path, sequence: u64) -> PathBuf {
    dir.join(format!("{sequence:020}.bin"))
}

/// Create the folder if it is missing. A folder that is not private is refused.
pub(super) fn ensure_dir(dir: &Path) -> Result<(), Error> {
    if dir.symlink_metadata().is_err() {
        admit::create_private_dir(dir).map_err(|_| Error::Unavailable)?;
        let parent = dir.parent().ok_or(Error::Unavailable)?;
        std::fs::File::open(parent)
            .and_then(|folder| folder.sync_all())
            .map_err(|_| Error::Unavailable)?;
    }
    match admit::private_dir_status(dir) {
        Ok(Some(true)) => Ok(()),
        _ => Err(Error::Unavailable),
    }
}

/// Write the sealed file of entry `sequence`. A file left by a commit that failed is replaced.
pub(super) fn write(dir: &Path, sequence: u64, bytes: &[u8]) -> Result<PathBuf, Error> {
    let target = path(dir, sequence);
    remove(&target)?;
    laststore::write_new_file_ordered(&target, bytes).map_err(|_| Error::Unavailable)?;
    Ok(target)
}

pub(super) fn remove(target: &Path) -> Result<(), Error> {
    match std::fs::remove_file(target) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(Error::Unavailable),
    }
}

/// The sealed file of an entry, checked against the length its entry records.
pub(super) fn check(target: &Path, bytes: u64) -> Result<(), Error> {
    let held = match std::fs::symlink_metadata(target) {
        Ok(held) => held,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Err(Error::Gap),
        Err(_) => return Err(Error::Unavailable),
    };
    if !held.is_file() || !admit::is_private_meta(&held) || held.nlink() != 1 || held.len() != bytes
    {
        return Err(Error::Gap);
    }
    Ok(())
}

/// Remove every file whose entry is not in the spool: at or below `acked`, or above `head`.
/// A crash between a commit and the removal of its files leaves such files.
pub(super) fn sweep(dir: &Path, acked: u64, head: u64) -> Result<(), Error> {
    let listing = std::fs::read_dir(dir).map_err(|_| Error::Unavailable)?;
    for item in listing {
        let item = item.map_err(|_| Error::Unavailable)?;
        let name = item.file_name();
        let sequence = name
            .to_str()
            .and_then(|name| name.strip_suffix(".bin"))
            .filter(|digits| digits.len() == 20 && digits.bytes().all(|b| b.is_ascii_digit()))
            .and_then(|digits| digits.parse::<u64>().ok());
        match sequence {
            Some(sequence) if sequence > acked && sequence <= head => {}
            Some(_) => remove(&item.path())?,
            // An ordered write leaves no partial name, so anything else is not the spool's.
            None => return Err(Error::Gap),
        }
    }
    Ok(())
}
