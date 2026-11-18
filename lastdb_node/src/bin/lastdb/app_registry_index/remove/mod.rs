//! Remove one installed app from this machine.
//!
//! `post_install` is the only install step that runs after the app is on the
//! machine. `InstallPlan` rejects unknown fields, so the manifest has no
//! uninstall step, and the receipt does not record which services a step
//! started. `routines` and `remote` retarget LaunchAgents from `post_install`,
//! and those agents point at `<apps root>/<app>/current/source`. Deleting that
//! tree would leave them running against a missing program. The labels live
//! in the app's script, not in the manifest, so this command cannot name what
//! is still running. It refuses the app instead of guessing.
//!
//! `lastdb app upgrade --all` installs only apps whose `current` receipt is a
//! file (`installed_app_ids`). Removing that link is what keeps the registry
//! poll from installing the app again.

use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};

use super::managed::{self, ManagedReceipt};
use super::managed_manifest::InstallPlan;
use super::{str_err, RECEIPT_FILE};

mod files;

/// Connect failures use this text. A timeout says `client timeout after` and
/// must not be treated as a stopped node.
const NODE_DOWN: &str = "failed to connect to daemon socket";

struct Held {
    receipt: Option<ManagedReceipt>,
    plan: InstallPlan,
}

enum Grant {
    Removed,
    Absent,
    NotRemoved(String),
}

pub(crate) fn remove(app: &str, home: &Path, delete_data: bool) -> Result<Value, String> {
    let root = managed::app_root(app)?;
    refuse_root_symlink(&root)?;
    if !installed(&root) && !has_artifacts(&root) {
        return finish_grant(app, home, &root);
    }
    let _lock = managed::lock(&root)?;
    if !installed(&root) && !has_artifacts(&root) {
        let _ = fs::remove_file(root.join("install.lock"));
        let _ = fs::remove_dir(&root);
        return finish_grant(app, home, &root);
    }
    let strict = installed(&root);
    let held = collect(&root, app, strict)?;
    block_services(app, &held)?;
    let data = data_state(&root, delete_data)?;
    refuse_odd_files(&root)?;
    let commands = command_set(&held);
    // A hard node error stops here, before any command link or version is deleted.
    let grant = drop_grant(app, home)?;
    let removed = files::unlink_commands(app, &root, &commands)?;
    files::delete_tree(&root, data)?;
    Ok(report(app, grant, data, removed))
}

fn installed(root: &Path) -> bool {
    root.join("current").join(RECEIPT_FILE).is_file()
}

fn finish_grant(app: &str, home: &Path, root: &Path) -> Result<Value, String> {
    // The files are already gone. `--delete-data` applied on the first run.
    match drop_grant(app, home)? {
        Grant::Removed => Ok(report(app, Grant::Removed, data_word(root), Vec::new())),
        Grant::Absent => Err(format!("app '{app}' is not installed")),
        Grant::NotRemoved(_) => Err(format!(
            "app '{app}' is not installed. The node is not running, so a leftover grant was not removed. Run `lastdb app remove {app}` again when the node is up."
        )),
    }
}

fn drop_grant(app: &str, home: &Path) -> Result<Grant, String> {
    match super::register_grant::unregister(home, app) {
        Ok(true) => Ok(Grant::Removed),
        Ok(false) => Ok(Grant::Absent),
        Err(error) if error.contains(NODE_DOWN) => Ok(Grant::NotRemoved(error)),
        Err(error) => Err(error),
    }
}

fn report(app: &str, grant: Grant, data: &str, commands: Vec<String>) -> Value {
    let grant_text = match &grant {
        Grant::Removed => "removed",
        Grant::Absent => "absent",
        Grant::NotRemoved(_) => "not_removed",
    };
    let mut body = json!({
        "app": app,
        "state": "removed",
        "grant": grant_text,
        "data": data,
        "commands_removed": commands,
    });
    if let Grant::NotRemoved(error) = grant {
        let message = format!(
            "The app files were removed. The grant was not removed because the node is not running ({error}). Run `lastdb app remove {app}` again when the node is up."
        );
        eprintln!("{message}");
        body["message"] = json!(message);
    }
    body
}

fn refuse_root_symlink(root: &Path) -> Result<(), String> {
    match root.symlink_metadata() {
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(str_err(error)),
        Ok(meta) if meta.file_type().is_symlink() => Err(format!(
            "refusing to remove {}: the app directory is a symlink",
            root.display()
        )),
        Ok(_) => Ok(()),
    }
}

fn has_artifacts(root: &Path) -> bool {
    if root.symlink_metadata().is_err() {
        return false;
    }
    for name in [
        "current",
        "previous",
        "versions",
        "rejected-source",
        "last-checked",
    ] {
        if root.join(name).symlink_metadata().is_ok() {
            return true;
        }
    }
    let Ok(entries) = fs::read_dir(root) else {
        return false;
    };
    entries.flatten().any(|entry| {
        entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with("stage-") || name.contains(".new-"))
    })
}

fn collect(root: &Path, app: &str, strict: bool) -> Result<Vec<Held>, String> {
    let mut seen = BTreeSet::new();
    let mut held = Vec::new();
    real_versions(root)?;
    for name in ["current", "previous"] {
        if let Some(dir) = resolved_link(root, name)? {
            consider(&dir, app, strict, &mut seen, &mut held)?;
        }
    }
    let versions = root.join("versions");
    if versions.symlink_metadata().is_ok() {
        for entry in fs::read_dir(&versions).map_err(str_err)? {
            let entry = entry.map_err(str_err)?;
            let path = entry.path();
            let kind = entry.file_type().map_err(str_err)?;
            if kind.is_symlink() {
                return Err(format!(
                    "refusing to remove {}: it is a symlink",
                    path.display()
                ));
            }
            if kind.is_dir() {
                consider(&path, app, strict, &mut seen, &mut held)?;
            }
        }
    }
    Ok(held)
}

fn real_versions(root: &Path) -> Result<(), String> {
    let path = root.join("versions");
    match path.symlink_metadata() {
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(str_err(error)),
        Ok(meta) if meta.file_type().is_symlink() => Err(format!(
            "refusing to remove {}: it is a symlink",
            path.display()
        )),
        Ok(meta) if meta.is_dir() => Ok(()),
        Ok(_) => Err(format!(
            "refusing to remove {}: it is not a directory",
            path.display()
        )),
    }
}

fn resolved_link(root: &Path, name: &str) -> Result<Option<PathBuf>, String> {
    let path = root.join(name);
    let meta = match path.symlink_metadata() {
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(str_err(error)),
        Ok(meta) => meta,
    };
    if !meta.file_type().is_symlink() {
        return Err(format!(
            "refusing to remove {}: it is not a symlink",
            path.display()
        ));
    }
    let Ok(dir) = fs::canonicalize(&path) else {
        return Ok(None);
    };
    let versions = root.join("versions");
    let base = fs::canonicalize(&versions).unwrap_or(versions);
    if !dir.starts_with(&base) {
        return Err(format!(
            "refusing to remove {}: it points outside {}",
            path.display(),
            root.join("versions").display()
        ));
    }
    Ok(Some(dir))
}

fn consider(
    dir: &Path,
    app: &str,
    strict: bool,
    seen: &mut BTreeSet<PathBuf>,
    held: &mut Vec<Held>,
) -> Result<(), String> {
    let dir = fs::canonicalize(dir).map_err(str_err)?;
    if !seen.insert(dir.clone()) {
        return Ok(());
    }
    let receipt = match managed::receipt(&dir) {
        Ok(receipt) => Some(receipt),
        Err(_) if dir.join(RECEIPT_FILE).is_file() => {
            return Err(format!(
                "app '{app}' was not removed. The install receipt in {} could not be read, so this command cannot tell whether its steps start a service.",
                dir.display()
            ));
        }
        Err(_) => None,
    };
    let manifest = dir.join("source/lastdb-app.json");
    if !manifest.is_file() {
        if strict && receipt.is_some() {
            return Err(format!(
                "app '{app}' was not removed. {} has a receipt and no install manifest.",
                dir.display()
            ));
        }
        return Ok(());
    }
    let plan = match InstallPlan::load(&dir.join("source"), app) {
        Ok(plan) => plan,
        Err(error) if strict || receipt.is_some() => {
            return Err(format!(
                "app '{app}' was not removed. The install manifest in {} could not be read ({error}), so this command cannot tell whether its steps start a service.",
                dir.display()
            ));
        }
        Err(_) => return Ok(()),
    };
    if receipt.is_none() && !plan.post_install.is_empty() {
        return Err(format!(
            "app '{app}' was not removed. {} has post_install steps and no receipt, so this command cannot tell whether those steps ran.",
            dir.display()
        ));
    }
    held.push(Held { receipt, plan });
    Ok(())
}

fn block_services(app: &str, held: &[Held]) -> Result<(), String> {
    let mut lines = Vec::new();
    for item in held {
        let inactive = item
            .receipt
            .as_ref()
            .is_some_and(|receipt| receipt.inactive);
        if inactive || item.plan.post_install.is_empty() {
            continue;
        }
        let sha = item
            .receipt
            .as_ref()
            .map(|receipt| receipt.sha.as_str())
            .unwrap_or("unreceipted");
        let steps = item
            .plan
            .post_install
            .iter()
            .map(|step| step.argv.join(" "))
            .collect::<Vec<_>>()
            .join("; ");
        lines.push(format!("  {sha} {steps}"));
    }
    if lines.is_empty() {
        return Ok(());
    }
    Err(format!(
        "app '{app}' was not removed. These versions have post_install steps, and the manifest has no uninstall step:\n{}\nStop what those steps left running. This command will not remove the app while those steps remain.",
        lines.join("\n")
    ))
}

fn command_set(held: &[Held]) -> BTreeSet<String> {
    held.iter()
        .flat_map(|item| item.plan.links.iter().map(|link| link.command.clone()))
        .collect()
}

fn data_state(root: &Path, delete_data: bool) -> Result<&'static str, String> {
    let path = root.join("data");
    match path.symlink_metadata() {
        Err(error) if error.kind() == ErrorKind::NotFound => Ok("absent"),
        Err(error) => Err(str_err(error)),
        Ok(meta) if meta.file_type().is_symlink() || !meta.is_dir() => {
            if delete_data {
                Err(format!(
                    "refusing to remove {}: the app data folder must be a directory and not a symlink",
                    path.display()
                ))
            } else {
                Ok("kept")
            }
        }
        Ok(_) if delete_data => Ok("removed"),
        Ok(_) => Ok("kept"),
    }
}

fn data_word(root: &Path) -> &'static str {
    match root.join("data").symlink_metadata() {
        Ok(_) => "kept",
        Err(_) => "absent",
    }
}

fn refuse_odd_files(root: &Path) -> Result<(), String> {
    for name in ["rejected-source", "last-checked"] {
        refuse_directory(&root.join(name))?;
    }
    let Ok(entries) = fs::read_dir(root) else {
        return Ok(());
    };
    for entry in entries {
        let entry = entry.map_err(str_err)?;
        let Some(name) = entry.file_name().into_string().ok() else {
            continue;
        };
        if name.contains(".new-") {
            refuse_directory(&entry.path())?;
        }
    }
    Ok(())
}

fn refuse_directory(path: &Path) -> Result<(), String> {
    match path.symlink_metadata() {
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(str_err(error)),
        Ok(meta) if meta.is_dir() && !meta.file_type().is_symlink() => Err(format!(
            "refusing to remove {}: it is a directory",
            path.display()
        )),
        Ok(_) => Ok(()),
    }
}
