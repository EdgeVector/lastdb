//! Install by proof: clone the proved commit and write the install receipt.

use std::path::Path;
use std::process::Command as ProcessCommand;

use serde::{Deserialize, Serialize};
use serde_json::json;

use super::resolve::Resolution;
use super::version_constraint;

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

/// Outcome of `lastdb app upgrade` on the proved path.
#[derive(Debug, Clone, Serialize)]
pub struct PinnedUpgradeOutcome {
    pub app_id: String,
    pub installed_sha: String,
    pub resolved_sha: String,
    pub installed_version: String,
    pub resolved_version: String,
    pub upgraded: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub install: Option<PinnedInstallOutcome>,
}

#[derive(Debug, Deserialize)]
struct PinnedReceipt {
    #[serde(default)]
    sha: String,
    #[serde(default)]
    version: String,
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
    force: bool,
) -> Result<PinnedInstallOutcome, String> {
    if !resolution.sha.chars().all(|c| c.is_ascii_hexdigit()) || resolution.sha.len() < 7 {
        return Err(format!(
            "compat row sha '{}' is not a git commit",
            resolution.sha
        ));
    }
    let checkout_path = install_dir.join("source");
    prepare_install_dir(install_dir, force)?;

    let cleanup = |why: String| -> String {
        let _ = std::fs::remove_dir_all(install_dir);
        why
    };

    let clone = ProcessCommand::new("git")
        .arg("clone")
        .arg("--quiet")
        .arg("--no-checkout")
        .arg(&resolution.source)
        .arg(&checkout_path)
        .status()
        .map_err(|e| cleanup(format!("failed to run git clone: {e}")))?;
    if !clone.success() {
        return Err(cleanup(format!(
            "git clone failed for app '{}' from {}",
            resolution.app_id, resolution.source
        )));
    }
    let checkout = ProcessCommand::new("git")
        .arg("-C")
        .arg(&checkout_path)
        .arg("checkout")
        .arg("--quiet")
        .arg("--detach")
        .arg(&resolution.sha)
        .status()
        .map_err(|e| cleanup(format!("failed to run git checkout: {e}")))?;
    if !checkout.success() {
        return Err(cleanup(format!(
            "git checkout {} failed for app '{}': the proved commit is not in {}",
            resolution.sha, resolution.app_id, resolution.source
        )));
    }
    let head = ProcessCommand::new("git")
        .arg("-C")
        .arg(&checkout_path)
        .arg("rev-parse")
        .arg("HEAD")
        .output()
        .map_err(|e| cleanup(format!("failed to run git rev-parse: {e}")))?;
    let head = String::from_utf8_lossy(&head.stdout).trim().to_string();
    if !head.starts_with(&resolution.sha) && !resolution.sha.starts_with(&head) {
        return Err(cleanup(format!(
            "checkout HEAD is {head} but the proved commit is {}",
            resolution.sha
        )));
    }

    // Check version constraints in manifest (if present)
    check_version_constraint(
        &checkout_path,
        &resolution.app_id,
        &resolution.lastdb_version,
    )
    .map_err(cleanup)?;

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

/// Check if the installed app's manifest declares a lastdb version constraint,
/// and verify the running LastDB build satisfies it.
///
/// Looks for common manifest files: `lastdb-app.json`, `folddb.toml`, `*.app.json`.
/// If no manifest is found or no constraint is declared, this is a no-op (apps
/// without constraints install unconditionally).
fn check_version_constraint(
    checkout_path: &Path,
    app_id: &str,
    lastdb_version: &str,
) -> Result<(), String> {
    // Try common manifest paths
    let manifest_candidates = [
        checkout_path.join("lastdb-app.json"),
        checkout_path.join("folddb.toml"),
    ];

    // Also try <app_id>.app.json
    let app_json = checkout_path.join(format!("{app_id}.app.json"));

    for path in manifest_candidates.iter().chain(std::iter::once(&app_json)) {
        if !path.exists() {
            continue;
        }

        let Ok(raw) = std::fs::read_to_string(path) else {
            continue;
        };

        // Try parsing as JSON first
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&raw) {
            if let Some(constraint_str) = json.get("lastdb").and_then(|v| v.as_str()) {
                return version_constraint::check_constraint(
                    app_id,
                    constraint_str,
                    lastdb_version,
                );
            }
        }

        // Try parsing as TOML (for folddb.toml)
        if path.extension().and_then(|s| s.to_str()) == Some("toml") {
            // Simple line-by-line search for lastdb = "..."
            for line in raw.lines() {
                let line = line.trim();
                if let Some(rest) = line.strip_prefix("lastdb") {
                    let rest = rest.trim();
                    if let Some(rest) = rest.strip_prefix('=') {
                        let rest = rest.trim().trim_matches('"').trim_matches('\'');
                        if !rest.is_empty() {
                            return version_constraint::check_constraint(
                                app_id,
                                rest,
                                lastdb_version,
                            );
                        }
                    }
                }
            }
        }
    }

    // No manifest or no constraint found - allow install
    Ok(())
}

/// Remove (with `force`) or reject an existing install directory, then create it.
fn prepare_install_dir(install_dir: &Path, force: bool) -> Result<(), String> {
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
    std::fs::create_dir_all(install_dir).map_err(|e| {
        format!(
            "failed to create install directory {}: {e}",
            install_dir.display()
        )
    })?;

    Ok(())
}

/// Reinstall only when the resolved commit differs from the installed one.
///
/// # Errors
/// A corrupt receipt or any [`install_pinned`] failure.
pub fn upgrade_pinned(
    resolution: &Resolution,
    install_dir: &Path,
) -> Result<PinnedUpgradeOutcome, String> {
    let receipt_path = install_dir.join(RECEIPT_FILE);
    if !receipt_path.exists() {
        let install = install_pinned(resolution, install_dir, false)?;
        return Ok(PinnedUpgradeOutcome {
            app_id: resolution.app_id.clone(),
            installed_sha: String::new(),
            resolved_sha: install.sha.clone(),
            installed_version: String::new(),
            resolved_version: resolution.app_version.clone(),
            upgraded: true,
            install: Some(install),
        });
    }
    let raw = std::fs::read_to_string(&receipt_path)
        .map_err(|e| format!("failed to read {}: {e}", receipt_path.display()))?;
    let receipt: PinnedReceipt = serde_json::from_str(&raw)
        .map_err(|e| format!("invalid install receipt {}: {e}", receipt_path.display()))?;
    let same = !receipt.sha.is_empty()
        && (receipt.sha.starts_with(&resolution.sha) || resolution.sha.starts_with(&receipt.sha));
    if same {
        return Ok(PinnedUpgradeOutcome {
            app_id: resolution.app_id.clone(),
            installed_sha: receipt.sha,
            resolved_sha: resolution.sha.clone(),
            installed_version: receipt.version,
            resolved_version: resolution.app_version.clone(),
            upgraded: false,
            install: None,
        });
    }

    // Stage and check the new commit in a temporary directory first, so a rejected
    // upgrade (version constraint failure) leaves the working install untouched.
    let temp_dir = install_dir.with_file_name(format!(
        "{}.upgrade-stage",
        install_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("app")
    ));

    let cleanup_temp = || {
        let _ = std::fs::remove_dir_all(&temp_dir);
    };

    // Stage the new version in temp_dir
    let staged = install_pinned(resolution, &temp_dir, true);
    if let Err(e) = staged {
        cleanup_temp();
        return Err(e);
    }

    // Version constraint check passed; replace the old install with the staged one
    std::fs::remove_dir_all(install_dir).map_err(|e| {
        format!(
            "failed to remove old install {}: {e}",
            install_dir.display()
        )
    })?;
    std::fs::rename(&temp_dir, install_dir).map_err(|e| {
        format!(
            "failed to move staged install from {} to {}: {e}",
            temp_dir.display(),
            install_dir.display()
        )
    })?;

    let install = staged.unwrap();
    Ok(PinnedUpgradeOutcome {
        app_id: resolution.app_id.clone(),
        installed_sha: receipt.sha,
        resolved_sha: install.sha.clone(),
        installed_version: receipt.version,
        resolved_version: resolution.app_version.clone(),
        upgraded: true,
        install: Some(install),
    })
}
