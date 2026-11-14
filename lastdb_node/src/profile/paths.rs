//! Default `.lastdb` home resolution and leading-tilde expansion.

use std::path::{Path, PathBuf};

pub const LASTDB_DIR: &str = ".lastdb";
pub const LASTDB_HOME_ENV: &str = "LASTDB_HOME";

pub fn folddb_home() -> Result<PathBuf, String> {
    if let Ok(home) = std::env::var(LASTDB_HOME_ENV) {
        return expand_tilde_path(&home);
    }
    dirs::home_dir()
        .map(|h| h.join(LASTDB_DIR))
        .ok_or_else(|| "Cannot determine home directory".into())
}

pub fn expand_tilde_path(path: impl AsRef<Path>) -> Result<PathBuf, String> {
    let p = path.as_ref();
    if let Some(raw) = p.to_str().filter(|r| *r == "~" || r.starts_with("~/")) {
        let home = std::env::var("HOME").map_err(|_| {
            format!("Cannot expand ~ in path \"{raw}\": HOME environment variable not set")
        })?;
        return Ok(PathBuf::from(home).join(raw.strip_prefix("~/").unwrap_or("")));
    }
    Ok(p.to_path_buf())
}
