//! Steps of `install_app`: vet the registry record, prepare the directory, clone, record.

use super::*;

/// Refuse a registry record that must not be installed; return its tier label
/// and the source to clone.
pub(super) fn vet_app_lookup(
    lookup: &schema_service_client::AppLookup,
    app_id: &str,
    allow_sandbox: bool,
) -> Result<(&'static str, String), String> {
    if lookup.revoked {
        return Err(format!(
            "install rejected: app '{app_id}' publisher is revoked"
        ));
    }
    let tier = match lookup.tier {
        AppTier::Live => "live",
        AppTier::Sandbox => "sandbox",
    };
    if lookup.tier != AppTier::Live && !allow_sandbox {
        return Err(format!(
            "install rejected: app '{app_id}' is {tier}; pass --allow-sandbox to install sandbox records"
        ));
    }
    if lookup.owner_dev_pubkey.trim().is_empty() {
        return Err(format!(
            "install rejected: app '{app_id}' registry record has no publisher key"
        ));
    }
    let source = lookup.source.clone().ok_or_else(|| {
        if lookup.artifact.is_some() {
            format!(
                "install rejected: app '{app_id}' only has an artifact pointer; signed artifact download is not implemented yet"
            )
        } else {
            format!("install rejected: app '{app_id}' has no source or artifact pointer")
        }
    })?;
    Ok((tier, source))
}

/// Create an empty install directory, replacing an existing one only with `force`.
pub(super) fn prepare_install_dir(install_dir: &Path, force: bool) -> Result<(), String> {
    if install_dir.exists() {
        if !force {
            return Err(format!(
                "install directory already exists: {} (pass --force to replace it)",
                install_dir.display()
            ));
        }
        std::fs::remove_dir_all(install_dir).map_err(|e| {
            format!(
                "failed to remove existing install directory {}: {e}",
                install_dir.display()
            )
        })?;
    }
    if let Some(parent) = install_dir.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            format!(
                "failed to create install parent directory {}: {e}",
                parent.display()
            )
        })?;
    }
    std::fs::create_dir_all(install_dir).map_err(|e| {
        format!(
            "failed to create install directory {}: {e}",
            install_dir.display()
        )
    })
}

pub(super) fn clone_app_source(
    app_id: &str,
    source: &str,
    install_dir: &Path,
    checkout_path: &Path,
) -> Result<(), String> {
    let clone_status = ProcessCommand::new("git")
        .arg("clone")
        .arg("--depth=1")
        .arg(source)
        .arg(checkout_path)
        .status()
        .map_err(|e| format!("failed to run git clone: {e}"))?;
    if !clone_status.success() {
        let _ = std::fs::remove_dir_all(install_dir);
        return Err(format!(
            "git clone failed for app '{app_id}' from {source} into {}",
            checkout_path.display()
        ));
    }
    Ok(())
}

pub(super) fn write_install_record(
    install_dir: &Path,
    schema_service_url: &str,
    lookup: &schema_service_client::AppLookup,
    source: &str,
    tier: &str,
) -> Result<(), String> {
    let install_record = json!({
        "app_id": lookup.app_id,
        "version": lookup.version,
        "installed_at": chrono::Utc::now().to_rfc3339(),
        "schema_service_url": schema_service_url,
        "source": source,
        "tier": tier,
        "owner_dev_pubkey": lookup.owner_dev_pubkey,
        "metadata": lookup.metadata,
        "code_signature": lookup.code_signature,
        "artifact": lookup.artifact,
        "uses": lookup.uses,
    });
    let record_path = install_dir.join("lastdb-app-install.json");
    let record_bytes = serde_json::to_vec_pretty(&install_record)
        .map_err(|e| format!("failed to encode install record: {e}"))?;
    std::fs::write(&record_path, record_bytes)
        .map_err(|e| format!("failed to write {}: {e}", record_path.display()))
}
