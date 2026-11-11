//! Cross-environment endpoint registry and `LASTDB_HOME` / tilde path helpers.
#![warn(missing_docs)]

use std::path::{Path, PathBuf};

pub mod endpoints;

/// Node-home path resolution and tilde expansion.
pub mod paths {
    pub use crate::profile::{
        expand_tilde, expand_tilde_path, folddb_home, LASTDB_DIR, LASTDB_HOME_ENV,
    };
}

/// Directory name of the default node home.
pub const LASTDB_DIR: &str = ".lastdb";
/// Node-home override.
pub const LASTDB_HOME_ENV: &str = "LASTDB_HOME";

/// Resolve the node-home directory: `LASTDB_HOME` when set, else `~/.lastdb`.
pub fn folddb_home() -> Result<PathBuf, String> {
    if let Ok(home) = std::env::var(LASTDB_HOME_ENV) {
        return expand_tilde(&home);
    }
    let home = dirs::home_dir().ok_or_else(|| "Cannot determine home directory".to_string())?;
    Ok(home.join(LASTDB_DIR))
}

/// Expand a leading `~` or `~/` to the current user's home directory.
pub fn expand_tilde(raw: &str) -> Result<PathBuf, String> {
    if raw == "~" || raw.starts_with("~/") {
        match std::env::var("HOME") {
            Ok(home) => {
                let rest = raw.strip_prefix("~/").unwrap_or("");
                Ok(PathBuf::from(home).join(rest))
            }
            Err(_) => Err(format!(
                "Cannot expand ~ in path \"{raw}\": HOME environment variable not set"
            )),
        }
    } else {
        Ok(PathBuf::from(raw))
    }
}

/// Tilde-expand a `PathBuf`/`Path` input.
pub fn expand_tilde_path(path: impl AsRef<Path>) -> Result<PathBuf, String> {
    let path = path.as_ref();
    if let Some(s) = path.to_str() {
        if s == "~" || s.starts_with("~/") {
            return expand_tilde(s);
        }
    }
    Ok(path.to_path_buf())
}
