//! Platform durability barriers.
//!
//! Every data file and directory barrier of a disk format v2 home goes
//! through this module: `F_FULLFSYNC` on Apple, `fsync` elsewhere.

use crate::Result;
use std::fs::File;

#[cfg(target_os = "macos")]
fn barrier(file: &File) -> Result<()> {
    // Rust's sync_data also issues F_FULLFSYNC on Apple. Use the explicit
    // strongest barrier once and propagate errors; never fall back to fsync.
    let _timing = crate::commit_timing::stage("device_flush_us");
    crate::commit_timing::count("device_flushes", 1);
    rustix::fs::fcntl_fullfsync(file).map_err(std::io::Error::from)?;
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn barrier(file: &File) -> Result<()> {
    file.sync_data()?;
    Ok(())
}

/// Make a data file durable.
pub(crate) fn sync_dirty_file(file: &File) -> Result<()> {
    barrier(file)
}

/// Make a directory entry durable.
pub(crate) fn sync_directory(dir: &File) -> Result<()> {
    #[cfg(target_os = "macos")]
    return barrier(dir);
    #[cfg(not(target_os = "macos"))]
    {
        dir.sync_all()?;
        Ok(())
    }
}
