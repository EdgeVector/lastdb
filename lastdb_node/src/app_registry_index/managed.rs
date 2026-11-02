//! Registry-owned app trees, atomic activation and retained rollback versions.

use super::{managed_manifest::InstallPlan, managed_process::steps, Resolution, RECEIPT_FILE};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedReceipt {
    pub app_id: String,
    pub sha: String,
    pub version: String,
    pub channel: String,
    pub index: String,
    pub trust: String,
    pub publisher: String,
    pub consent_hash: String,
    pub inactive: bool,
    pub installed_at: String,
    pub listing: super::ListEntry,
}

#[derive(Default, Clone, Copy)]
pub struct ActivationOptions {
    pub accept_install: bool,
    pub inactive: bool,
}

pub fn apps_root() -> Result<PathBuf, String> {
    std::env::var_os("LASTDB_APPS_ROOT")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|p| PathBuf::from(p).join(".local/share/lastdb-apps"))
        })
        .ok_or_else(|| "HOME is required for managed apps".into())
}

pub fn app_root(app: &str) -> Result<PathBuf, String> {
    if !super::valid_app_id(app) {
        return Err("invalid app id".into());
    }
    Ok(apps_root()?.join(app))
}

pub fn receipt(version: &Path) -> Result<ManagedReceipt, String> {
    let bytes = fs::read(version.join(RECEIPT_FILE)).map_err(|e| e.to_string())?;
    serde_json::from_slice(&bytes).map_err(|e| e.to_string())
}

pub fn lock(root: &Path) -> Result<File, String> {
    fs::create_dir_all(root).map_err(|e| e.to_string())?;
    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join("install.lock"))
        .map_err(|e| e.to_string())?;
    file.try_lock()
        .map_err(|e| format!("app install is locked: {e}"))?;
    Ok(file)
}

pub fn install(
    resolution: &Resolution,
    home: &Path,
    options: ActivationOptions,
) -> Result<ManagedReceipt, String> {
    let root = app_root(&resolution.app_id)?;
    let _lock = lock(&root)?;
    let prior = if root.join("current").symlink_metadata().is_ok() {
        Some(receipt(&root.join("current"))?)
    } else {
        None
    };
    if prior
        .as_ref()
        .is_some_and(|r| r.sha == resolution.sha && r.inactive == options.inactive)
    {
        let current = prior.ok_or("missing current receipt")?;
        verify_active(&root, home, &current)?;
        return Ok(current);
    }
    let version = root.join("versions").join(format!(
        "{}-{}",
        resolution.sha,
        chrono::Utc::now().timestamp_micros()
    ));
    let stage = root.join(format!("stage-{}", std::process::id()));
    if stage.exists() {
        return Err(format!(
            "retained install staging directory: {}",
            stage.display()
        ));
    }
    let outcome = stage_version(resolution, &stage, home, prior.as_ref(), &options);
    let result = match outcome {
        Ok(new) => {
            fs::create_dir_all(root.join("versions")).map_err(|e| e.to_string())?;
            fs::rename(&stage, &version).map_err(|e| e.to_string())?;
            activate(&root, &version, home, &new)?;
            prune(&root)?;
            Ok(new)
        }
        Err(error) => Err(error),
    };
    if stage.exists() {
        let _ = fs::remove_dir_all(stage);
    }
    result
}

fn stage_version(
    resolution: &Resolution,
    stage: &Path,
    home: &Path,
    prior: Option<&ManagedReceipt>,
    options: &ActivationOptions,
) -> Result<ManagedReceipt, String> {
    let entry = resolution
        .list_entry
        .as_ref()
        .ok_or("managed install requires a publisher-signed interface listing")?;
    if !resolution.trust.starts_with("pinned:") {
        return Err("managed activation requires the pinned registry key".into());
    }
    super::install_pinned(resolution, stage, false)?;
    let source = stage.join("source");
    let plan = InstallPlan::load(&source, &resolution.app_id)?;
    let hash = plan.consent_hash()?;
    let changed = prior.is_none_or(|r| {
        r.consent_hash != hash
            || r.publisher != entry.publisher_key
            || r.inactive != options.inactive
    });
    if prior.is_some_and(|r| r.publisher != entry.publisher_key) {
        return Err("publisher key changed; explicit owner rotation is required".into());
    }
    if changed && !options.accept_install {
        eprintln!(
            "Install consent for {} (publisher {}): {}",
            resolution.app_id,
            entry.publisher_key,
            serde_json::to_string_pretty(&plan).map_err(|e| e.to_string())?
        );
        return Err("review install instructions and repeat with --accept-install".into());
    }
    for app in &plan.requires {
        if !app_root(app)?.join("current").exists() {
            return Err(format!("install required app '{app}' first"));
        }
    }
    steps(&source, home, &resolution.app_id, &plan.prepare)?;
    super::managed_links::validate_sources(&source, &plan)?;
    if !options.inactive {
        steps(&source, home, &resolution.app_id, &plan.probe)?;
    }
    let new = ManagedReceipt {
        app_id: resolution.app_id.clone(),
        sha: resolution.sha.clone(),
        version: resolution.app_version.clone(),
        channel: resolution.channel.clone(),
        index: resolution
            .index
            .trim_end_matches(&format!(
                "/{}/api/{}.json",
                entry.channel, entry.api_version
            ))
            .to_string(),
        trust: resolution.trust.clone(),
        publisher: entry.publisher_key.clone(),
        consent_hash: hash,
        inactive: options.inactive,
        installed_at: chrono::Utc::now().to_rfc3339(),
        listing: entry.clone(),
    };
    fs::write(
        stage.join(RECEIPT_FILE),
        serde_json::to_vec_pretty(&new).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    Ok(new)
}

pub fn verify_active(root: &Path, home: &Path, current: &ManagedReceipt) -> Result<(), String> {
    verify_state(root, current)?;
    let source = root.join("current/source");
    let plan = InstallPlan::load(&source, &current.app_id)?;
    super::managed_links::verify(root, &plan)?;
    if !current.inactive {
        steps(&source, home, &current.app_id, &plan.probe)?;
    }
    Ok(())
}

pub fn verify_state(root: &Path, current: &ManagedReceipt) -> Result<(), String> {
    let source = root.join("current/source");
    let plan = InstallPlan::load(&source, &current.app_id)?;
    if plan.consent_hash()? != current.consent_hash {
        return Err("install manifest changed since consent".into());
    }
    super::managed_links::verify(root, &plan)?;
    let head = std::process::Command::new("git")
        .arg("-C")
        .arg(&source)
        .args(["rev-parse", "HEAD"])
        .output()
        .map_err(|e| e.to_string())?;
    if !head.status.success() || String::from_utf8_lossy(&head.stdout).trim() != current.sha {
        return Err("installed source commit drift".into());
    }
    let clean = std::process::Command::new("git")
        .arg("-C")
        .arg(&source)
        .args(["diff", "--quiet", "HEAD", "--"])
        .status()
        .map_err(|e| e.to_string())?;
    if !clean.success() {
        return Err("installed tracked source changed".into());
    }
    Ok(())
}

pub fn activate(
    root: &Path,
    version: &Path,
    home: &Path,
    new: &ManagedReceipt,
) -> Result<(), String> {
    let old = fs::read_link(root.join("current")).ok();
    let plan = InstallPlan::load(&version.join("source"), &new.app_id)?;
    let links_before = super::managed_links::snapshot(&plan)?;
    super::managed_links::flip(&root.join("current"), version)?;
    let result = super::managed_links::install(root, &plan).and_then(|()| {
        if !new.inactive {
            steps(
                &root.join("current/source"),
                home,
                &new.app_id,
                &plan.post_install,
            )?;
        }
        verify_active(root, home, new)
    });
    if let Err(error) = result {
        if let Some(ref target) = old {
            super::managed_links::flip(&root.join("current"), target)?;
            let old_receipt = receipt(&root.join("current"))?;
            let old_plan = InstallPlan::load(&root.join("current/source"), &old_receipt.app_id)?;
            super::managed_links::install(root, &old_plan)?;
            if !old_receipt.inactive {
                steps(
                    &root.join("current/source"),
                    home,
                    &old_receipt.app_id,
                    &old_plan.post_install,
                )?;
            }
        } else {
            let _ = fs::remove_file(root.join("current"));
        }
        super::managed_links::restore(&links_before)?;
        return Err(format!(
            "activation failed; restored prior version: {error}"
        ));
    }
    if let Some(target) = old {
        super::managed_links::flip(&root.join("previous"), &target)?;
    }
    Ok(())
}

pub fn rollback(app: &str, home: &Path) -> Result<ManagedReceipt, String> {
    let root = app_root(app)?;
    let _lock = lock(&root)?;
    let target = fs::canonicalize(root.join("previous")).map_err(|e| e.to_string())?;
    if !target.starts_with(fs::canonicalize(root.join("versions")).map_err(|e| e.to_string())?) {
        return Err("rollback target is outside app versions".into());
    }
    let prior = receipt(&target)?;
    let plan = InstallPlan::load(&target.join("source"), app)?;
    if !prior.inactive {
        steps(&target.join("source"), home, app, &plan.probe)?;
    }
    activate(&root, &target, home, &prior)?;
    Ok(prior)
}

fn prune(root: &Path) -> Result<(), String> {
    let current = fs::canonicalize(root.join("current")).ok();
    let previous = fs::canonicalize(root.join("previous")).ok();
    let mut versions = Vec::new();
    for entry in fs::read_dir(root.join("versions")).map_err(|e| e.to_string())? {
        let path = entry.map_err(|e| e.to_string())?.path();
        if let Ok(r) = receipt(&path) {
            versions.push((r.installed_at, path));
        }
    }
    versions.sort_by(|a, b| b.0.cmp(&a.0));
    let mut kept = 0;
    for (_, path) in versions {
        if current.as_ref() == Some(&path) || previous.as_ref() == Some(&path) {
            continue;
        }
        if kept == 0 {
            kept += 1;
        } else {
            fs::remove_dir_all(path).map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}
