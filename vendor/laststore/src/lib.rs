//! Disk format v2 engine ([`disk_run`]): immutable, sorted, encrypted run files and `CURRENT`.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use serde_json as _;
use std::fmt;

pub(crate) mod bloom;
pub mod disk_run;
mod frame;
pub mod space;

/// True when isolated storage diagnostics (`LASTDB_SNAPSHOT_TIMING=1`) are enabled.
pub fn diagnostics_enabled() -> bool {
    std::env::var("LASTDB_SNAPSHOT_TIMING").is_ok_and(|v| matches!(v.trim(), "1" | "true" | "on"))
}

/// Result alias for Last Store operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors returned by the disk format v2 engine ([`crate::disk_run`]).
#[derive(Debug)]
pub enum Error {
    /// Underlying filesystem or IO failure.
    Io(std::io::Error),
    /// On-disk format is truncated or inconsistent.
    Corrupt(String),
    /// AEAD authentication failed while decrypting a frame.
    AeadAuthFail,
    /// Invalid configuration or caller input.
    Config(String),
    /// Another owner currently holds the home lock.
    HomeBusy(String),
    /// A disk-run call exhausted its explicit work or memory budget.
    ReadBudgetExceeded,
    /// A mutation cannot reserve its bounded write resources.
    WriteCapacity(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io: {e}"),
            Self::Corrupt(s) => write!(f, "corrupt: {s}"),
            Self::AeadAuthFail => f.write_str("aead authentication failed"),
            Self::Config(s) => write!(f, "config: {s}"),
            Self::HomeBusy(s) => write!(f, "home busy: {s}"),
            Self::ReadBudgetExceeded => f.write_str("read resource limit exceeded"),
            Self::WriteCapacity(s) => write!(f, "write capacity unavailable: {s}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

pub(crate) mod durability {
    use crate::Result;
    use std::fs::File;
    use std::path::Path;

    pub(crate) fn sync_dirty_file(file: &File) -> Result<()> {
        #[cfg(target_os = "macos")]
        rustix::fs::fcntl_fullfsync(file).map_err(std::io::Error::from)?;
        #[cfg(not(target_os = "macos"))]
        file.sync_data()?;
        Ok(())
    }

    pub(crate) fn sync_directory(dir: &Path) -> Result<()> {
        let f = File::open(dir)?;
        #[cfg(target_os = "macos")]
        return sync_dirty_file(&f);
        #[cfg(not(target_os = "macos"))]
        Ok(f.sync_all()?)
    }
}
