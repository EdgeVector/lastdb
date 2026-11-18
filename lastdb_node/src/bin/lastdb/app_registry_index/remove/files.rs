//! Delete the command links and the install tree.
//!
//! The caller has already decided the app can be removed. A symlink is
//! removed only when it points at the `lastdb` launcher and this app lists
//! that command, or when it points inside this app's directory. A regular
//! file is left in place. `current` is unlinked only after the other install
//! files, so a failure before that still looks installed.

use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use super::super::command_names;
use super::super::str_err;

pub(super) fn unlink_commands(
    app: &str,
    root: &Path,
    commands: &BTreeSet<String>,
) -> Result<Vec<String>, String> {
    let bin = super::super::managed_manifest::bin_root()?;
    if bin.symlink_metadata().is_err() {
        return Ok(Vec::new());
    }
    let launcher = bin.join("lastdb");
    let mut removed = BTreeSet::new();
    for command in commands {
        if unlinked(&bin.join(command), app, root, &launcher, command)? {
            removed.insert(command.clone());
        }
        let prefix = format!("{command}.new-");
        for entry in fs::read_dir(&bin).map_err(str_err)? {
            let entry = entry.map_err(str_err)?;
            let Some(name) = entry.file_name().into_string().ok() else {
                continue;
            };
            if name.starts_with(&prefix) && unlinked(&entry.path(), app, root, &launcher, command)?
            {
                removed.insert(name);
            }
        }
    }
    Ok(removed.into_iter().collect())
}

fn unlinked(
    path: &Path,
    app: &str,
    root: &Path,
    launcher: &Path,
    command: &str,
) -> Result<bool, String> {
    let meta = match path.symlink_metadata() {
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(str_err(error)),
        Ok(meta) => meta,
    };
    if !meta.file_type().is_symlink() {
        return Ok(false);
    }
    let target = fs::read_link(path).map_err(str_err)?;
    let ours = if target.as_path() == launcher {
        !command_names::listed_by_other(command, app)?
    } else {
        command_names::inside_app(&target, root)
    };
    if !ours {
        return Ok(false);
    }
    fs::remove_file(path).map_err(str_err)?;
    Ok(true)
}

pub(super) fn delete_tree(root: &Path, data: &str) -> Result<(), String> {
    if data == "removed" {
        let path = root.join("data");
        match fs::remove_dir_all(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(format!("could not remove {}: {error}", path.display())),
        }
    }
    remove_symlink(&root.join("previous"))?;
    remove_named(root, ".new-", false)?;
    remove_file_if_present(&root.join("rejected-source"))?;
    remove_file_if_present(&root.join("last-checked"))?;
    remove_named(root, "stage-", true)?;
    let keep = current_target(root)?;
    remove_version_children(root, keep.as_deref())?;
    remove_symlink(&root.join("current"))?;
    remove_version_children(root, None)?;
    match fs::remove_dir(root.join("versions")) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(str_err(error)),
    }
    // Last mutation while the lock is held, so a new install cannot start early.
    fs::remove_file(root.join("install.lock")).map_err(str_err)?;
    let _ = fs::remove_dir(root);
    Ok(())
}

fn current_target(root: &Path) -> Result<Option<PathBuf>, String> {
    let path = root.join("current");
    match path.symlink_metadata() {
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(str_err(error)),
        Ok(meta) if meta.file_type().is_symlink() => Ok(fs::canonicalize(&path).ok()),
        Ok(_) => Err(format!(
            "refusing to remove {}: it is not a symlink",
            path.display()
        )),
    }
}

fn remove_version_children(root: &Path, skip: Option<&Path>) -> Result<(), String> {
    let versions = root.join("versions");
    let meta = match versions.symlink_metadata() {
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(str_err(error)),
        Ok(meta) => meta,
    };
    if meta.file_type().is_symlink() || !meta.is_dir() {
        return Err(format!(
            "refusing to remove {}: it is not a directory",
            versions.display()
        ));
    }
    for entry in fs::read_dir(&versions).map_err(str_err)? {
        let entry = entry.map_err(str_err)?;
        let path = entry.path();
        if skip.is_some_and(|keep| same_dir(&path, keep)) {
            continue;
        }
        let kind = entry.file_type().map_err(str_err)?;
        if kind.is_symlink() {
            return Err(format!(
                "refusing to remove {}: it is a symlink",
                path.display()
            ));
        } else if kind.is_dir() {
            fs::remove_dir_all(&path).map_err(str_err)?;
        } else {
            fs::remove_file(&path).map_err(str_err)?;
        }
    }
    Ok(())
}

fn same_dir(path: &Path, keep: &Path) -> bool {
    match (fs::canonicalize(path), fs::canonicalize(keep)) {
        (Ok(path), Ok(keep)) => path == keep,
        _ => path == keep,
    }
}

fn remove_symlink(path: &Path) -> Result<(), String> {
    match path.symlink_metadata() {
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(str_err(error)),
        Ok(meta) if meta.file_type().is_symlink() => fs::remove_file(path).map_err(str_err),
        Ok(_) => Err(format!(
            "refusing to remove {}: it is not a symlink",
            path.display()
        )),
    }
}

fn remove_file_if_present(path: &Path) -> Result<(), String> {
    match path.symlink_metadata() {
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(str_err(error)),
        Ok(meta) if meta.is_dir() && !meta.file_type().is_symlink() => Err(format!(
            "refusing to remove {}: it is a directory",
            path.display()
        )),
        Ok(_) => fs::remove_file(path).map_err(str_err),
    }
}

fn remove_named(root: &Path, mark: &str, stage: bool) -> Result<(), String> {
    let Ok(entries) = fs::read_dir(root) else {
        return Ok(());
    };
    for entry in entries {
        let entry = entry.map_err(str_err)?;
        let Some(name) = entry.file_name().into_string().ok() else {
            continue;
        };
        let hit = if stage {
            name.starts_with(mark)
        } else {
            name.contains(mark)
        };
        if !hit {
            continue;
        }
        let path = entry.path();
        let kind = entry.file_type().map_err(str_err)?;
        if kind.is_symlink() || !kind.is_dir() {
            fs::remove_file(&path).map_err(str_err)?;
        } else if stage {
            fs::remove_dir_all(&path).map_err(str_err)?;
        } else {
            return Err(format!(
                "refusing to remove {}: it is a directory",
                path.display()
            ));
        }
    }
    Ok(())
}
