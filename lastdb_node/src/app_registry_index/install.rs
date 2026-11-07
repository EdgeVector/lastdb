//! Install by proof: clone the proved commit and write the install receipt.

use std::path::Path;
use std::process::Command as ProcessCommand;

use serde::Serialize;
use serde_json::json;

use super::resolve::Resolution;

/// The receipt `lastdb app install` leaves in the install directory.
pub const RECEIPT_FILE: &str = "lastdb-app-install.json";

/// What a pinned install produced.
#[derive(Debug, Clone, Serialize)]
pub struct PinnedInstallOutcome {
    pub app_id: String,
    pub app_version: String,
    pub sha: String,
    pub lastdb_version: String,
    pub channel: String,
    pub source: String,
    pub install_dir: String,
    pub checkout_path: String,
    pub proof_run: String,
}

/// Clone `resolution.source` and check out `resolution.sha` under
/// `install_dir/source`, then write the receipt.
///
/// The checkout is verified by `git rev-parse HEAD`; a clone that lands on a
/// different commit is removed, not installed.
///
/// # Errors
/// Filesystem and git failures, and a HEAD that is not the proved commit.
pub fn install_pinned(
    resolution: &Resolution,
    install_dir: &Path,
) -> Result<PinnedInstallOutcome, String> {
    if !resolution.sha.chars().all(|c| c.is_ascii_hexdigit()) || resolution.sha.len() != 40 {
        return Err(format!(
            "compat row sha '{}' is not a git commit",
            resolution.sha
        ));
    }
    let checkout_path = install_dir.join("source");
    prepare_install_dir(install_dir)?;

    let cleanup = |why: String| -> String {
        let _ = std::fs::remove_dir_all(install_dir);
        why
    };

    let head = clone_and_checkout(
        &checkout_path,
        &resolution.source,
        &resolution.sha,
        &resolution.app_id,
        &cleanup,
    )?;

    let receipt = json!({
        "app_id": resolution.app_id,
        "version": resolution.app_version,
        "sha": head,
        "lastdb_version": resolution.lastdb_version,
        "channel": resolution.channel,
        "source": resolution.source,
        "proved_at": resolution.proved_at,
        "proof_run": resolution.proof_run,
        "index": resolution.index,
        "trust": resolution.trust,
        "listing": resolution.list_entry,
        "installed_at": chrono::Utc::now().to_rfc3339(),
    });
    let receipt_path = install_dir.join(RECEIPT_FILE);
    let bytes = serde_json::to_vec_pretty(&receipt)
        .map_err(|e| cleanup(format!("failed to encode install receipt: {e}")))?;
    std::fs::write(&receipt_path, bytes)
        .map_err(|e| cleanup(format!("failed to write {}: {e}", receipt_path.display())))?;

    Ok(PinnedInstallOutcome {
        app_id: resolution.app_id.clone(),
        app_version: resolution.app_version.clone(),
        sha: head,
        lastdb_version: resolution.lastdb_version.clone(),
        channel: resolution.channel.clone(),
        source: resolution.source.clone(),
        install_dir: install_dir.display().to_string(),
        checkout_path: checkout_path.display().to_string(),
        proof_run: resolution.proof_run.clone(),
    })
}

/// Clone and checkout a proved commit. Helper for install_pinned.
fn clone_and_checkout<F>(
    checkout_path: &Path,
    source: &str,
    sha: &str,
    app_id: &str,
    cleanup: &F,
) -> Result<String, String>
where
    F: Fn(String) -> String,
{
    let clone = ProcessCommand::new("git")
        .arg("clone")
        .arg("--quiet")
        .arg("--no-checkout")
        .arg(source)
        .arg(checkout_path)
        .status()
        .map_err(|e| cleanup(format!("failed to run git clone: {e}")))?;
    if !clone.success() {
        return Err(cleanup(format!(
            "git clone failed for app '{app_id}' from {source}"
        )));
    }
    let checkout = ProcessCommand::new("git")
        .arg("-C")
        .arg(checkout_path)
        .arg("checkout")
        .arg("--quiet")
        .arg("--detach")
        .arg(sha)
        .status()
        .map_err(|e| cleanup(format!("failed to run git checkout: {e}")))?;
    if !checkout.success() {
        return Err(cleanup(format!(
            "git checkout {sha} failed for app '{app_id}': the proved commit is not in {source}"
        )));
    }
    let head = ProcessCommand::new("git")
        .arg("-C")
        .arg(checkout_path)
        .arg("rev-parse")
        .arg("HEAD")
        .output()
        .map_err(|e| cleanup(format!("failed to run git rev-parse: {e}")))?;
    let head = String::from_utf8_lossy(&head.stdout).trim().to_string();
    if head != sha {
        return Err(cleanup(format!(
            "checkout HEAD is {head} but the proved commit is {sha}"
        )));
    }
    Ok(head)
}

/// Reject an existing install directory, then create it.
fn prepare_install_dir(install_dir: &Path) -> Result<(), String> {
    if install_dir.exists() {
        return Err(format!(
            "install directory already exists: {}",
            install_dir.display()
        ));
    }
    std::fs::create_dir_all(install_dir).map_err(|e| {
        format!(
            "failed to create install directory {}: {e}",
            install_dir.display()
        )
    })?;

    Ok(())
}
