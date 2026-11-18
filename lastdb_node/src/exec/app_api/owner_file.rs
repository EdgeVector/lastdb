//! The folder an owner names with `file_dir` on a read.
//!
//! The folder must already exist. The node does not create it and does not
//! change its mode. An app cannot name one: its folder comes from its grant.

use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use super::bad;
use crate::exec::reply::HostError;

const MAX_DIR_LEN: usize = 4096;

pub(super) fn accept(dir: &str) -> Result<PathBuf, HostError> {
    let path = Path::new(dir);
    if dir.is_empty() || !path.is_absolute() {
        return Err(bad("file_dir must be an absolute path"));
    }
    if dir.len() > MAX_DIR_LEN {
        return Err(bad("file_dir is too long"));
    }
    // Refuse a symlink at the leaf before canonicalize follows a parent.
    real_directory(path)?;
    let canonical =
        fs::canonicalize(path).map_err(|_| bad("file_dir must be an existing directory"))?;
    confirm(&canonical)?;
    Ok(canonical)
}

pub(super) fn confirm(path: &Path) -> Result<(), HostError> {
    let meta = real_directory(path)?;
    if meta.uid() != unsafe { libc::geteuid() } {
        return Err(bad("file_dir must be owned by the calling user"));
    }
    if meta.mode() & 0o022 != 0 {
        return Err(bad("file_dir must not be group or world writable"));
    }
    Ok(())
}

fn real_directory(path: &Path) -> Result<fs::Metadata, HostError> {
    let meta =
        fs::symlink_metadata(path).map_err(|_| bad("file_dir must be an existing directory"))?;
    if !meta.is_dir() || meta.file_type().is_symlink() {
        return Err(bad("file_dir must be a directory and not a symlink"));
    }
    Ok(meta)
}
