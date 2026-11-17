//! A command name belongs to one installed app.
//!
//! Install refuses a name another app already lists, and it refuses to replace
//! a path in `~/.local/bin` that this app did not install. The check runs
//! before activation moves `current`, so the manifest being installed cannot
//! claim a name by itself.

use std::collections::HashSet;
use std::fs;
use std::path::{Component, Path, PathBuf};

use super::managed;
use super::managed_manifest::{self, InstallPlan};
use super::{str_err, valid_app_id};

/// Commands the active install and the retained previous install already list.
pub(super) fn owned_commands(root: &Path, app: &str) -> Result<HashSet<String>, String> {
    let mut names = HashSet::new();
    if root.join("current").symlink_metadata().is_ok() {
        let dir = installed_version(root, "current").ok_or_else(|| {
            format!("installed app '{app}' current is outside its version directory")
        })?;
        add_listed(&dir, app, &mut names)?;
    }
    // `previous` is retained history. A broken copy must not block an upgrade
    // of the active install.
    if let Some(dir) = installed_version(root, "previous") {
        add_listed(&dir, app, &mut names).ok();
    }
    Ok(names)
}

/// Refuse when another installed app lists a command, or when its path in
/// `~/.local/bin` is not a link this app installed.
pub(super) fn ensure_available(
    root: &Path,
    app: &str,
    plan: &InstallPlan,
    restricted: bool,
    owned: &HashSet<String>,
) -> Result<(), String> {
    let bin = managed_manifest::bin_root()?;
    for link in &plan.links {
        if let Some(other) = other_owner(&link.command, app)? {
            return Err(format!(
                "command '{}' belongs to installed app '{other}'",
                link.command
            ));
        }
        ensure_replaceable(&bin, root, &link.command, restricted, owned)?;
    }
    Ok(())
}

/// Refuse to replace one command path this app did not install.
pub(super) fn ensure_replaceable(
    bin: &Path,
    root: &Path,
    command: &str,
    restricted: bool,
    owned: &HashSet<String>,
) -> Result<(), String> {
    let path = bin.join(command);
    match path.symlink_metadata() {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(str_err(error)),
        Ok(meta) if meta.file_type().is_symlink() => {
            if installed_by_app(&path, root, bin, command, restricted, owned)? {
                return Ok(());
            }
        }
        Ok(_) => {}
    }
    Err(format!(
        "refusing to replace {}: it is not a command link installed by this app",
        path.display()
    ))
}

fn installed_version(root: &Path, name: &str) -> Option<PathBuf> {
    let versions = fs::canonicalize(root.join("versions")).ok()?;
    let dir = fs::canonicalize(root.join(name)).ok()?;
    dir.starts_with(versions).then_some(dir)
}

fn add_listed(dir: &Path, app: &str, names: &mut HashSet<String>) -> Result<(), String> {
    let plan = InstallPlan::load(&dir.join("source"), app)
        .map_err(|error| format!("cannot read commands for installed app '{app}': {error}"))?;
    for link in plan.links {
        names.insert(link.command);
    }
    Ok(())
}

fn other_owner(command: &str, app: &str) -> Result<Option<String>, String> {
    let apps = managed::apps_root()?;
    let entries = match fs::read_dir(&apps) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(str_err(error)),
    };
    let mut found: Option<String> = None;
    for entry in entries {
        let entry = entry.map_err(str_err)?;
        let Some(name) = entry.file_name().into_string().ok() else {
            continue;
        };
        if name == app || !valid_app_id(&name) {
            continue;
        }
        if !lists_command(&entry.path().join("current"), &name, command) {
            continue;
        }
        found = Some(match found {
            Some(prev) if prev < name => prev,
            _ => name,
        });
    }
    Ok(found)
}

fn lists_command(current: &Path, app: &str, command: &str) -> bool {
    let Ok(receipt) = managed::receipt(current) else {
        return false;
    };
    if receipt.app_id != app {
        return false;
    }
    InstallPlan::load(&current.join("source"), app)
        .ok()
        .is_some_and(|plan| plan.links.iter().any(|link| link.command == command))
}

fn installed_by_app(
    path: &Path,
    root: &Path,
    bin: &Path,
    command: &str,
    restricted: bool,
    owned: &HashSet<String>,
) -> Result<bool, String> {
    let target = fs::read_link(path).map_err(str_err)?;
    if inside_app(&target, root) {
        return Ok(true);
    }
    Ok(restricted && owned.contains(command) && target == bin.join("lastdb"))
}

fn inside_app(target: &Path, root: &Path) -> bool {
    if !target.is_absolute()
        || target
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        return false;
    }
    if let (Ok(target), Ok(root)) = (fs::canonicalize(target), fs::canonicalize(root)) {
        return target.starts_with(root);
    }
    target.starts_with(root)
}
