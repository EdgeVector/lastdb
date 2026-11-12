//! Registry-owned app trees, atomic activation, and retained rollback versions.

use std::fs::{self, File};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::managed_manifest::{self, InstallPlan};
use super::managed_poll::steps;
use super::{ListEntry, Resolution, RECEIPT_FILE};

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
    pub listing: ListEntry,
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
    super::valid_app_id(app)
        .then(|| apps_root().map(|r| r.join(app)))
        .ok_or_else(|| "invalid app id".to_string())?
}

pub fn receipt(version: &Path) -> Result<ManagedReceipt, String> {
    serde_json::from_slice(&fs::read(version.join(RECEIPT_FILE)).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())
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

fn is_rejected(root: &Path, sha: &str) -> bool {
    fs::read_to_string(root.join("rejected-source")).is_ok_and(|s| s.trim() == sha)
}

pub fn install(
    resolution: &Resolution,
    home: &Path,
    options: ActivationOptions,
) -> Result<ManagedReceipt, String> {
    let entry = &resolution.entry;
    let root = app_root(&entry.app_id)?;
    let _lock = lock(&root)?;
    let prior = root
        .join("current")
        .symlink_metadata()
        .ok()
        .map(|_| receipt(&root.join("current")))
        .transpose()?;
    if !options.accept_install && is_rejected(&root, &entry.sha) {
        if let Some(cur) = &prior {
            verify_or_restore(&root, home, cur)?;
        }
        return Err(
            "source held after rollback; a newer source or explicit install consent is required"
                .into(),
        );
    }
    if let Some(cur) = prior.as_ref().filter(|r| {
        r.sha == entry.sha && r.inactive == options.inactive && r.publisher == entry.publisher_key
    }) {
        verify_or_restore(&root, home, cur)?;
        return Ok(cur.clone());
    }
    let version = root.join("versions").join(format!(
        "{}-{}",
        entry.sha,
        chrono::Utc::now().timestamp_micros()
    ));
    let stage = root.join(format!("stage-{}", std::process::id()));
    if stage.exists() {
        return Err(format!(
            "retained install staging directory: {}",
            stage.display()
        ));
    }
    let result =
        stage_version(resolution, &stage, home, prior.as_ref(), &options).and_then(|new| {
            fs::create_dir_all(root.join("versions")).map_err(|e| e.to_string())?;
            fs::rename(&stage, &version).map_err(|e| e.to_string())?;
            let activation = activate(&root, &version, home, &new);
            prune(&root)?;
            activation?;
            if root.join("rejected-source").exists() {
                fs::remove_file(root.join("rejected-source")).map_err(|e| e.to_string())?;
            }
            Ok(new)
        });
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
    let entry = &resolution.entry;
    if !resolution.trust.starts_with("pinned:") {
        return Err("managed activation requires the pinned registry key".into());
    }
    fs::create_dir_all(stage).map_err(|e| {
        format!(
            "failed to create install directory {}: {e}",
            stage.display()
        )
    })?;
    let source = stage.join("source");
    managed_manifest::clone_source(&source, entry)?;
    let plan = InstallPlan::load(&source, &entry.app_id)?;
    let hash = plan.consent_hash()?;
    if prior.is_some_and(|r| {
        r.publisher != entry.publisher_key
            && !managed_manifest::authorized_transition(r, resolution)
    }) {
        return Err("publisher key changed; explicit owner rotation is required".into());
    }
    if prior.is_none_or(|r| r.consent_hash != hash || r.inactive != options.inactive)
        && !options.accept_install
    {
        eprintln!(
            "Install consent for {} (publisher {}): {}",
            entry.app_id,
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
    steps(&source, home, &entry.app_id, &plan.prepare)?;
    managed_manifest::validate_sources(&source, &plan)?;
    if !options.inactive {
        steps(&source, home, &entry.app_id, &plan.probe)?;
    }
    let suffix = format!("/{}/api/{}.json", entry.channel, entry.api_version);
    let new = ManagedReceipt {
        app_id: entry.app_id.clone(),
        sha: entry.sha.clone(),
        version: entry.app_version.clone(),
        channel: entry.channel.clone(),
        index: resolution.index.trim_end_matches(&suffix).to_string(),
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
    if !current.inactive {
        let src = root.join("current/source");
        let plan = InstallPlan::load(&src, &current.app_id)?;
        steps(&src, home, &current.app_id, &plan.probe)?;
    }
    Ok(())
}

pub fn verify_or_restore(root: &Path, home: &Path, current: &ManagedReceipt) -> Result<(), String> {
    if let Err(error) = verify_active(root, home, current) {
        restore_previous(root, home)?;
        fs::write(root.join("rejected-source"), &current.sha).map_err(|e| e.to_string())?;
        return Err(format!(
            "current app failed its health check; restored previous version and held source: {error}"
        ));
    }
    Ok(())
}

pub fn verify_state(root: &Path, current: &ManagedReceipt) -> Result<(), String> {
    let src = root.join("current/source");
    let plan = InstallPlan::load(&src, &current.app_id)?;
    if plan.consent_hash()? != current.consent_hash {
        return Err("install manifest changed since consent".into());
    }
    managed_manifest::verify(root, &plan)?;
    managed_manifest::verify_git_source(&src, &current.sha)
}

fn apply_current_plan(
    root: &Path,
    home: &Path,
    current: &ManagedReceipt,
    plan: &InstallPlan,
) -> Result<(), String> {
    managed_manifest::install(root, plan)?;
    if !current.inactive {
        steps(
            &root.join("current/source"),
            home,
            &current.app_id,
            &plan.post_install,
        )?;
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
    let links_before = managed_manifest::snapshot(&plan)?;
    managed_manifest::flip(&root.join("current"), version)?;
    if let Err(error) =
        apply_current_plan(root, home, new, &plan).and_then(|()| verify_active(root, home, new))
    {
        if let Some(ref target) = old {
            managed_manifest::flip(&root.join("current"), target)?;
            let old_r = receipt(&root.join("current"))?;
            let old_p = InstallPlan::load(&root.join("current/source"), &old_r.app_id)?;
            apply_current_plan(root, home, &old_r, &old_p)?;
        } else {
            let _ = fs::remove_file(root.join("current"));
        }
        managed_manifest::restore(&links_before)?;
        return Err(format!(
            "activation failed; restored prior version: {error}"
        ));
    }
    if let Some(target) = old {
        managed_manifest::flip(&root.join("previous"), &target)?;
    }
    Ok(())
}

pub fn rollback(app: &str, home: &Path) -> Result<ManagedReceipt, String> {
    let root = app_root(app)?;
    let _lock = lock(&root)?;
    let rejected = receipt(&root.join("current"))?;
    let restored = restore_previous(&root, home)?;
    fs::write(root.join("rejected-source"), rejected.sha).map_err(|e| e.to_string())?;
    Ok(restored)
}

fn restore_previous(root: &Path, home: &Path) -> Result<ManagedReceipt, String> {
    let target = fs::canonicalize(root.join("previous")).map_err(|e| e.to_string())?;
    if !target.starts_with(fs::canonicalize(root.join("versions")).map_err(|e| e.to_string())?) {
        return Err("rollback target is outside app versions".into());
    }
    let prior = receipt(&target)?;
    if is_rejected(root, &prior.sha) {
        return Err(
            "previous source is held after rollback; explicit install consent is required".into(),
        );
    }
    let (_, _, node) =
        super::running_node(&crate::lastdb_uds::uds::socket_path(&home.join("data")));
    if prior.listing.api_version != node.api_version
        || prior
            .listing
            .outbox_format_version
            .is_some_and(|v| v != node.outbox_format_version)
    {
        return Err("previous app does not match the running node interface".into());
    }
    let plan = InstallPlan::load(&target.join("source"), &prior.app_id)?;
    if !prior.inactive {
        steps(&target.join("source"), home, &prior.app_id, &plan.probe)?;
    }
    activate(root, &target, home, &prior)?;
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
    for (_, path) in versions
        .into_iter()
        .filter(|(_, p)| current.as_ref() != Some(p) && previous.as_ref() != Some(p))
        .skip(1)
    {
        fs::remove_dir_all(path).map_err(|e| e.to_string())?;
    }
    Ok(())
}
