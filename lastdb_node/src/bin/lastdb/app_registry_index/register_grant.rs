//! Tell the running node which access the current version of an app declared.
//! A long-running process of that app is then served under the new lists.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{json, Value};

use super::managed::ManagedReceipt;
use super::managed_manifest::InstallPlan;

const TIMEOUT: Duration = Duration::from_secs(5);

/// Register `receipt`'s access. An app that declares none is left unchanged.
pub(super) fn register(home: &Path, receipt: &ManagedReceipt) -> Result<(), String> {
    let Some(access) = receipt.listing.access.as_ref() else {
        return Ok(());
    };
    let data = app_data_dir(&receipt.app_id)?;
    let body = json!({
        "app": receipt.app_id,
        "access": {
            "read": access.read.keys().collect::<Vec<_>>(),
            "write": access.write.keys().collect::<Vec<_>>(),
            "extensions": &access.extensions,
        },
        "data": data,
    });
    let socket = lastdb_node::lastdb_uds::socket_path(&home.join("data"));
    let raw =
        crate::uds_client::post_json_with_timeout(&socket, "/api/apps/session", &body, TIMEOUT)?;
    let reply = crate::uds_client::parse_json_response(&raw, "app access")?;
    if reply.get("session").and_then(Value::as_str) != Some(receipt.app_id.as_str()) {
        return Err(format!(
            "the node did not register app '{}'",
            receipt.app_id
        ));
    }
    Ok(())
}

/// Put the previous version back and register its access again.
pub(super) fn undo_activation(
    root: &Path,
    home: &Path,
    old: Option<&Path>,
    owned: &HashSet<String>,
    snap: &[(PathBuf, Option<PathBuf>)],
) -> Result<(), String> {
    let Some(target) = old else {
        let _ = std::fs::remove_file(root.join("current"));
        return super::managed_manifest::restore(snap);
    };
    super::managed_manifest::flip(&root.join("current"), target)?;
    let old_receipt = super::managed::receipt(&root.join("current"))?;
    let plan = InstallPlan::load(&root.join("current/source"), &old_receipt.app_id)?;
    super::managed::apply_links(root, home, &old_receipt, &plan, owned)?;
    super::managed_manifest::restore(snap)?;
    register(home, &old_receipt)
}

fn app_data_dir(app: &str) -> Result<String, String> {
    let app_data = super::managed::app_root(app)?.join("data");
    let data_text = app_data
        .to_str()
        .ok_or_else(|| {
            format!(
                "the app data folder must be an absolute path: {}",
                app_data.display()
            )
        })?
        .to_owned();
    if !app_data.is_absolute() {
        return Err(format!(
            "the app data folder must be an absolute path: {data_text}"
        ));
    }
    if std::fs::symlink_metadata(&app_data)
        .is_ok_and(|meta| meta.file_type().is_symlink() || !meta.is_dir())
    {
        return Err("the app data folder must be a directory and not a symlink".into());
    }
    let temp = app_data.join("tmp");
    std::fs::create_dir_all(&temp)
        .map_err(|error| format!("could not create {}: {error}", temp.display()))?;
    Ok(data_text)
}
