//! Cross-environment endpoint registry and `LASTDB_HOME` / tilde path helpers.
#![warn(missing_docs)]

use std::path::{Path, PathBuf};

pub mod endpoints;

/// Node-home path resolution and tilde expansion.
pub mod paths {
    pub use crate::profile::{expand_tilde_path, folddb_home, LASTDB_DIR, LASTDB_HOME_ENV};
}

/// Directory name of the default node home.
pub const LASTDB_DIR: &str = ".lastdb";
/// Node-home override environment variable.
pub const LASTDB_HOME_ENV: &str = "LASTDB_HOME";

/// Resolve the node-home directory: `LASTDB_HOME` when set, else `~/.lastdb`.
pub fn folddb_home() -> Result<PathBuf, String> {
    if let Ok(home) = std::env::var(LASTDB_HOME_ENV) {
        return expand_tilde_path(&home);
    }
    let home = dirs::home_dir().ok_or_else(|| "Cannot determine home directory".to_string())?;
    Ok(home.join(LASTDB_DIR))
}

/// Tilde-expand a `PathBuf`/`Path` input.
pub fn expand_tilde_path(path: impl AsRef<Path>) -> Result<PathBuf, String> {
    let path = path.as_ref();
    if let Some(raw) = path.to_str() {
        if raw == "~" || raw.starts_with("~/") {
            let home = std::env::var("HOME").map_err(|_| {
                format!("Cannot expand ~ in path \"{raw}\": HOME environment variable not set")
            })?;
            return Ok(PathBuf::from(home).join(raw.strip_prefix("~/").unwrap_or("")));
        }
    }
    Ok(path.to_path_buf())
}
