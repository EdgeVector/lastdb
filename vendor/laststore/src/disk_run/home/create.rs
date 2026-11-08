//! Install the owner lock with the authenticated empty home in one rename.

use super::*;
use crate::durability::sync_dirty_file;
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};

/// Every published home can enter read-only admission before a writer opens.
pub(super) fn build_empty(dir: &Path, key: &[u8; 32]) -> Result<()> {
    // `create_dir` uses 0o777 masked by umask. A 0o022 umask leaves this
    // directory group-readable, and the restore proof rejects that. The
    // parent home is already 0o700.
    fs::DirBuilder::new().mode(0o700).create(dir.join(RUNS))?;
    let lock = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(dir.join(LOCK))?;
    sync_dirty_file(&lock)?;
    let identity = Uuid::new_v4();
    let mut descriptor = [0u8; DESCRIPTOR_BYTES];
    descriptor[..8].copy_from_slice(DESCRIPTOR_MAGIC);
    descriptor[8..12].copy_from_slice(&1u32.to_le_bytes());
    descriptor[16..32].copy_from_slice(identity.as_bytes());
    let crc = crc32fast::hash(&descriptor[..60]);
    descriptor[60..].copy_from_slice(&crc.to_le_bytes());
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(dir.join(RUN_HOME_MARKER))?;
    file.write_all(&descriptor)?;
    sync_dirty_file(&file)?;
    let staged = Manifest::empty(identity).stage(dir, key)?;
    Manifest::replace(dir, &staged)?;
    sync_directory(&File::open(dir.join(RUNS))?)
}
