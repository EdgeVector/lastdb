//! Primary-home guard for storage recovery.

use std::path::{Path, PathBuf};

/// Refuse `path` when it is, or lies under, the primary home `~/.lastdb`.
pub fn refuse_primary(path: &Path) -> Result<(), String> {
    let user_home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| "HOME unset".to_string())?;
    let abs = canonicalize_lenient(path);
    let primary_abs = canonicalize_lenient(&user_home.join(crate::profile::paths::LASTDB_DIR));
    if abs.starts_with(&primary_abs) {
        return Err(format!(
            "refusing primary path {}: use a home outside ~/.lastdb",
            abs.display()
        ));
    }
    Ok(())
}

fn canonicalize_lenient(path: &Path) -> PathBuf {
    let mut tail = Vec::new();
    let mut cursor = path;
    loop {
        if let Ok(base) = std::fs::canonicalize(cursor) {
            return tail.iter().rev().fold(base, |acc, part| acc.join(part));
        }
        match (cursor.parent(), cursor.file_name()) {
            (Some(parent), Some(name)) => {
                tail.push(name.to_os_string());
                cursor = parent;
            }
            _ => return path.to_path_buf(),
        }
    }
}
