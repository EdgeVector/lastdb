//! Registry-owned app trees, atomic activation, and retained rollback versions.

use std::collections::HashSet;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::managed_manifest::{self, InstallPlan};
use super::managed_steps::steps;
use super::{str_err, ListEntry, Resolution, RECEIPT_FILE};

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

pub fn apps_root() -> Result<PathBuf, String> {
    let root = std::env::var_os("LASTDB_APPS_ROOT").map(PathBuf::from);
    let root = root.or_else(|| {
        std::env::var_os("HOME").map(|p| PathBuf::from(p).join(".local/share/lastdb-apps"))
    });
    root.ok_or_else(|| "HOME is required for managed apps".into())
}

pub fn app_root(app: &str) -> Result<PathBuf, String> {
    if !super::valid_app_id(app) {
        return Err("invalid app id".into());
    }
    Ok(apps_root()?.join(app))
}

pub fn receipt(version: &Path) -> Result<ManagedReceipt, String> {
    let raw = fs::read(version.join(RECEIPT_FILE)).map_err(str_err)?;
    serde_json::from_slice(&raw).map_err(str_err)
}

pub fn lock(root: &Path) -> Result<File, String> {
    fs::create_dir_all(root).map_err(str_err)?;
    let mut opts = File::options();
    opts.read(true).write(true).create(true).truncate(false);
    let file = opts.open(root.join("install.lock")).map_err(str_err)?;
    file.try_lock()
        .map_err(|e| format!("app install is locked: {e}"))?;
    Ok(file)
}

pub fn install(
    resolution: &Resolution,
    home: &Path,
    accept_install: bool,
    inactive: bool,
) -> Result<ManagedReceipt, String> {
    let entry = &resolution.entry;
    let root = app_root(&entry.app_id)?;
    let _lock = lock(&root)?;
    let cur = root.join("current");
    let prior = if cur.symlink_metadata().is_ok() {
        Some(receipt(&cur)?)
    } else {
        None
    };
    let rej = root.join("rejected-source");
    if !accept_install && fs::read_to_string(&rej).is_ok_and(|s| s.trim() == entry.sha) {
        if let Some(c) = &prior {
            verify_or_restore(&root, home, c)?;
        }
        return Err(
            "source held after rollback; a newer source or explicit install consent is required"
                .into(),
        );
    }
    if let Some(c) = prior.as_ref().filter(|r| {
        r.sha == entry.sha && r.inactive == inactive && r.publisher == entry.publisher_key
    }) {
        verify_or_restore(&root, home, c)?;
        return Ok(c.clone());
    }
    let stage = root.join(format!("stage-{}", std::process::id()));
    if stage.exists() {
        return Err(format!(
            "retained install staging directory: {}",
            stage.display()
        ));
    }
    let ts = chrono::Utc::now().timestamp_micros();
    let version = root.join("versions").join(format!("{}-{ts}", entry.sha));
    let res = stage_version(
        resolution,
        &stage,
        home,
        prior.as_ref(),
        accept_install,
        inactive,
    )
    .and_then(|new| {
        fs::create_dir_all(root.join("versions")).map_err(str_err)?;
        fs::rename(&stage, &version).map_err(str_err)?;
        let act = activate(&root, &version, home, &new);
        prune(&root)?;
        act?;
        if rej.exists() {
            fs::remove_file(&rej).map_err(str_err)?;
        }
        Ok(new)
    });
    let _ = fs::remove_dir_all(&stage);
    res
}

fn stage_version(
    resolution: &Resolution,
    stage: &Path,
    home: &Path,
    prior: Option<&ManagedReceipt>,
    accept_install: bool,
    inactive: bool,
) -> Result<ManagedReceipt, String> {
    let entry = &resolution.entry;
    if !resolution.trust.starts_with("pinned:") {
        return Err("managed activation requires the pinned registry key".into());
    }
    // A publisher cannot take an app out of its restriction with a later version.
    if prior.is_some_and(|r| r.listing.access.is_some()) && entry.access.is_none() {
        return Err(format!(
            "app '{}' runs restricted; a version that declares no LastDB access is refused",
            entry.app_id
        ));
    }
    if let Some(access) = &entry.access {
        let unknown = access.unknown_extensions();
        if !unknown.is_empty() {
            return Err(format!(
                "app '{}' names app API extensions this node does not have: {}",
                entry.app_id,
                unknown.join(", ")
            ));
        }
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
        r.publisher != entry.publisher_key && !super::authorized_transition(r, resolution)
    }) {
        return Err("publisher key changed; explicit owner rotation is required".into());
    }
    if prior.is_none_or(|r| r.consent_hash != hash || r.inactive != inactive) && !accept_install {
        let pretty = serde_json::to_string_pretty(&plan).map_err(str_err)?;
        eprintln!(
            "Install consent for {} (publisher {}): {pretty}",
            entry.app_id, entry.publisher_key
        );
        return Err("review install instructions and repeat with --accept-install".into());
    }
    for app in &plan.requires {
        if !app_root(app)?.join("current").exists() {
            return Err(format!("install required app '{app}' first"));
        }
    }
    let restricted = entry.access.is_some();
    steps(&source, home, &entry.app_id, &plan.prepare, restricted)?;
    managed_manifest::validate_sources(&source, &plan)?;
    // A restricted app is probed after activation places its schemas.
    if entry.access.is_none() && !inactive {
        steps(&source, home, &entry.app_id, &plan.probe, restricted)?;
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
        inactive,
        installed_at: chrono::Utc::now().to_rfc3339(),
        listing: entry.clone(),
    };
    let raw = serde_json::to_vec_pretty(&new).map_err(str_err)?;
    fs::write(stage.join(RECEIPT_FILE), raw).map_err(str_err)?;
    Ok(new)
}

pub fn verify(
    root: &Path,
    active_home: Option<&Path>,
    current: &ManagedReceipt,
) -> Result<(), String> {
    let src = root.join("current/source");
    let plan = InstallPlan::load(&src, &current.app_id)?;
    if plan.consent_hash()? != current.consent_hash {
        return Err("install manifest changed since consent".into());
    }
    managed_manifest::verify(root, &plan, current.listing.access.is_some())?;
    managed_manifest::verify_git_source(&src, &current.sha)?;
    if let Some(home) = active_home.filter(|_| !current.inactive) {
        let restricted = current.listing.access.is_some();
        steps(&src, home, &current.app_id, &plan.probe, restricted)?;
    }
    Ok(())
}

pub fn verify_or_restore(root: &Path, home: &Path, current: &ManagedReceipt) -> Result<(), String> {
    if let Err(error) = verify(root, Some(home), current) {
        restore_previous(root, home)?;
        fs::write(root.join("rejected-source"), &current.sha).map_err(str_err)?;
        return Err(format!("current app failed its health check; restored previous version and held source: {error}"));
    }
    Ok(())
}

fn apply_links(
    root: &Path,
    home: &Path,
    r: &ManagedReceipt,
    p: &InstallPlan,
    owned: &HashSet<String>,
) -> Result<(), String> {
    managed_manifest::install(root, p, r.listing.access.is_some(), owned)?;
    if !r.inactive {
        steps(
            &root.join("current/source"),
            home,
            &r.app_id,
            &p.post_install,
            r.listing.access.is_some(),
        )?;
    }
    Ok(())
}

fn activate(root: &Path, version: &Path, home: &Path, new: &ManagedReceipt) -> Result<(), String> {
    let old = fs::read_link(root.join("current")).ok();
    let plan = InstallPlan::load(&version.join("source"), &new.app_id)?;
    let owned = super::command_names::owned_commands(root, &new.app_id)?;
    let restricted = new.listing.access.is_some();
    super::command_names::ensure_available(root, &new.app_id, &plan, restricted, &owned)?;
    let snap = managed_manifest::snapshot(&plan)?;
    managed_manifest::flip(&root.join("current"), version)?;
    let declared = if restricted {
        super::place_schemas::place(&version.join("source"), &new.app_id, home)
    } else {
        Ok(())
    };
    if let Err(error) = declared
        .and_then(|()| apply_links(root, home, new, &plan, &owned))
        .and_then(|()| verify(root, Some(home), new))
    {
        if let Some(ref target) = old {
            managed_manifest::flip(&root.join("current"), target)?;
            let old_r = receipt(&root.join("current"))?;
            let old_p = InstallPlan::load(&root.join("current/source"), &old_r.app_id)?;
            apply_links(root, home, &old_r, &old_p, &owned)?;
        } else {
            let _ = fs::remove_file(root.join("current"));
        }
        managed_manifest::restore(&snap)?;
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
    fs::write(root.join("rejected-source"), rejected.sha).map_err(str_err)?;
    Ok(restored)
}

fn restore_previous(root: &Path, home: &Path) -> Result<ManagedReceipt, String> {
    let target = fs::canonicalize(root.join("previous")).map_err(str_err)?;
    if !target.starts_with(fs::canonicalize(root.join("versions")).map_err(str_err)?) {
        return Err("rollback target is outside app versions".into());
    }
    let prior = receipt(&target)?;
    if fs::read_to_string(root.join("rejected-source")).is_ok_and(|s| s.trim() == prior.sha) {
        return Err(
            "previous source is held after rollback; explicit install consent is required".into(),
        );
    }
    let (_, _, api, outbox, min_api) = super::running_node(home);
    if !(min_api..=api).contains(&prior.listing.api_version)
        || prior
            .listing
            .outbox_format_version
            .is_some_and(|v| v != outbox)
    {
        return Err("previous app does not match the running node interface".into());
    }
    let source = target.join("source");
    let plan = InstallPlan::load(&source, &prior.app_id)?;
    let restricted = prior.listing.access.is_some();
    if !prior.inactive {
        steps(&source, home, &prior.app_id, &plan.probe, restricted)?;
    }
    activate(root, &target, home, &prior)?;
    Ok(prior)
}

fn prune(root: &Path) -> Result<(), String> {
    let cur = fs::canonicalize(root.join("current")).ok();
    let prev = fs::canonicalize(root.join("previous")).ok();
    let mut versions = Vec::new();
    for e in fs::read_dir(root.join("versions")).map_err(str_err)? {
        let p = e.map_err(str_err)?.path();
        if let Ok(r) = receipt(&p) {
            versions.push((r.installed_at, p));
        }
    }
    versions.sort_by(|a, b| b.0.cmp(&a.0));
    let keep = |p: &Path| cur.as_deref() == Some(p) || prev.as_deref() == Some(p);
    for (_, p) in versions.into_iter().filter(|(_, p)| !keep(p)).skip(1) {
        fs::remove_dir_all(p).map_err(str_err)?;
    }
    Ok(())
}

#[cfg(target_os = "macos")]
pub fn install_poll() -> Result<(), String> {
    use std::process::Command;
    if std::env::var_os("LASTDB_APPS_ROOT").is_some() {
        return Ok(());
    }
    let home = PathBuf::from(std::env::var_os("HOME").ok_or("HOME is required")?);
    let agents = home.join("Library/LaunchAgents");
    for d in [&agents, &home.join(".local/state/lastdb-app-registry")] {
        fs::create_dir_all(d).map_err(str_err)?;
    }
    let label = "com.edgevector.lastdb-app-registry";
    let plist = agents.join(format!("{label}.plist"));
    let h = home
        .display()
        .to_string()
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");
    let off = std::env::var("LAST_STACK_APPS_OFF").unwrap_or_else(|_| "0".into());
    if off != "0" && off != "1" {
        return Err("LAST_STACK_APPS_OFF must be 0 or 1".into());
    }
    let data = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict>\n<key>Label</key><string>{label}</string>\n<key>ProgramArguments</key><array><string>{h}/.local/bin/lastdb</string><string>app</string><string>upgrade</string><string>--all</string></array>\n<key>StartInterval</key><integer>60</integer>\n<key>EnvironmentVariables</key><dict><key>HOME</key><string>{h}</string><key>PATH</key><string>{h}/.local/bin:{h}/.bun/bin:{h}/.cargo/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string><key>LASTDB_APP_REGISTRY_POLL</key><string>1</string><key>LAST_STACK_APPS_OFF</key><string>{off}</string></dict>\n<key>StandardOutPath</key><string>{h}/.local/state/lastdb-app-registry/upgrade.log</string>\n<key>StandardErrorPath</key><string>{h}/.local/state/lastdb-app-registry/upgrade.err</string></dict></plist>\n"
    );
    if fs::read_to_string(&plist).ok().as_deref() == Some(&data) {
        return Ok(());
    }
    fs::write(&plist, data).map_err(str_err)?;
    let uid = Command::new("id").arg("-u").output().map_err(str_err)?;
    let dom = format!("gui/{}", String::from_utf8_lossy(&uid.stdout).trim());
    let _ = Command::new("launchctl")
        .args(["bootout", &format!("{dom}/{label}")])
        .output();
    if !Command::new("launchctl")
        .args(["bootstrap", &dom])
        .arg(&plist)
        .output()
        .map_err(str_err)?
        .status
        .success()
    {
        return Err("could not load registry app updater".into());
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
pub fn install_poll() -> Result<(), String> {
    Ok(())
}
