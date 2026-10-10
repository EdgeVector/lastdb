//! Durable private artifacts; exact plan locations stay outside the home.

use super::*;
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};

pub(super) fn create_dir(path: &Path, home: &Path, root: &Path) -> Result<(), String> {
    if path.exists() {
        return Err("the plan directory already exists".into());
    }
    let parent =
        std::fs::canonicalize(path.parent().ok_or("plan directory has no parent")?).map_err(err)?;
    if parent.starts_with(home) || parent.starts_with(root) {
        return Err("the plan directory must be outside the home".into());
    }
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(path)
        .map_err(err)?;
    File::open(parent)
        .and_then(|file| file.sync_all())
        .map_err(err)
}

pub(super) fn validate_location(dir: &Path, home: &Path, root: &Path) -> Result<(), String> {
    let dir = std::fs::canonicalize(dir).map_err(err)?;
    if dir.starts_with(home) || dir.starts_with(root) {
        return Err("the plan directory must be outside the home".into());
    }
    Ok(())
}

pub(super) fn write<T: Serialize>(dir: &Path, name: &str, value: &T) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(value).map_err(err)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(dir.join(name))
        .map_err(err)?;
    file.write_all(&bytes)
        .and_then(|_| file.sync_all())
        .map_err(err)?;
    File::open(dir)
        .and_then(|file| file.sync_all())
        .map_err(err)
}

pub(super) fn load_plan(dir: &Path) -> Result<model::Plan, String> {
    let path = dir.join(model::PLAN_FILE);
    let dir_meta = std::fs::symlink_metadata(dir).map_err(err)?;
    let file_meta = std::fs::symlink_metadata(&path).map_err(err)?;
    if !dir_meta.is_dir()
        || !file_meta.is_file()
        || dir_meta.permissions().mode() & 0o077 != 0
        || file_meta.permissions().mode() & 0o077 != 0
    {
        return Err("the plan requires a private directory and regular private file".into());
    }
    let plan: model::Plan =
        serde_json::from_slice(&std::fs::read(path).map_err(err)?).map_err(err)?;
    if plan.format != model::FORMAT {
        return Err("unsupported target atom plan format".into());
    }
    Ok(plan)
}
