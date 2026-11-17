//! One publisher operation at a time per shared database.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

const DIRECTORY: &str = "share-members";

/// Hold the database's member lock until the returned file is dropped.
pub(super) fn hold(home: &Path, database_id: &str) -> Result<fs::File, String> {
    let directory = home.join(DIRECTORY);
    ensure_dir(&directory)?;
    let dir = crate::offline_home::Directory::open(&directory).map_err(|e| e.to_string())?;
    let file = dir
        .open_lock(&format!("{database_id}.lock"))
        .map_err(|e| e.to_string())?;
    file.try_lock()
        .map_err(|_| "a member update is in progress")?;
    Ok(file)
}

fn ensure_dir(path: &Path) -> Result<(), String> {
    fs::create_dir_all(path).map_err(|_| "cannot create a private directory")?;
    let metadata = fs::symlink_metadata(path).map_err(|_| "cannot inspect a private directory")?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err("private directory must be a real directory".into());
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|_| "cannot protect a private directory".into())
}
