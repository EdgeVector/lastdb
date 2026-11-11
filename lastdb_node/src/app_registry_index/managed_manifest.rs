//! Install instructions, atomic command links, and publisher transitions.

use std::collections::HashSet;
use std::fs;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{managed::ManagedReceipt, ListEntry, Resolution};
use crate::profile::endpoints::{
    registry_ci_publisher_keys_for, registry_index_url_for, Environment,
};

fn default_timeout() -> u64 {
    30
}

fn default_exits() -> Vec<i32> {
    vec![0]
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallStep {
    pub argv: Vec<String>,
    #[serde(default = "default_timeout")]
    pub timeout_s: u64,
    #[serde(default = "default_exits")]
    pub accept_exit: Vec<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallLink {
    pub source: String,
    pub command: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallPlan {
    #[serde(default, skip_deserializing)]
    pub uses: Vec<String>,
    #[serde(default)]
    pub prepare: Vec<InstallStep>,
    #[serde(default)]
    pub post_install: Vec<InstallStep>,
    pub probe: Vec<InstallStep>,
    pub links: Vec<InstallLink>,
    #[serde(default)]
    pub requires: Vec<String>,
}

fn relative(path: &str) -> Result<PathBuf, String> {
    let p = Path::new(path);
    if p.as_os_str().is_empty()
        || p.is_absolute()
        || p.components().any(|c| !matches!(c, Component::Normal(_)))
    {
        return Err(format!(
            "install path must stay inside the source tree: {path}"
        ));
    }
    Ok(p.to_path_buf())
}

impl InstallPlan {
    pub fn load(source: &Path, app: &str) -> Result<Self, String> {
        let raw = fs::read(source.join("lastdb-app.json")).map_err(|e| e.to_string())?;
        let value: serde_json::Value = serde_json::from_slice(&raw).map_err(|e| e.to_string())?;
        if value["app_id"].as_str() != Some(app) {
            return Err("source manifest app_id differs from signed listing".into());
        }
        let mut plan: Self = serde_json::from_value(value["install"].clone())
            .map_err(|e| format!("app needs an install manifest: {e}"))?;
        plan.uses = serde_json::from_value(
            value
                .get("uses")
                .cloned()
                .unwrap_or_else(|| serde_json::json!([])),
        )
        .map_err(|_| "manifest uses must be an array of strings")?;
        plan.validate()?;
        Ok(plan)
    }

    fn validate(&self) -> Result<(), String> {
        if self.probe.is_empty() || self.links.is_empty() {
            return Err("install needs links and at least one probe".into());
        }
        let mut commands = HashSet::new();
        for link in &self.links {
            relative(&link.source)?;
            if link.command.is_empty()
                || link.command.starts_with('-')
                || !link
                    .command
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
                || !commands.insert(&link.command)
            {
                return Err("invalid or duplicate install command".into());
            }
        }
        for step in self
            .prepare
            .iter()
            .chain(&self.post_install)
            .chain(&self.probe)
        {
            if step.argv.is_empty()
                || step.argv[0].is_empty()
                || !(1..=600).contains(&step.timeout_s)
                || step.accept_exit.is_empty()
            {
                return Err("invalid install step or timeout (1..600 seconds)".into());
            }
        }
        if self.requires.iter().any(|app| !super::valid_app_id(app)) {
            return Err("invalid required app".into());
        }
        Ok(())
    }

    pub fn consent_hash(&self) -> Result<String, String> {
        let value = serde_json::to_value(self).map_err(|e| e.to_string())?;
        let bytes = app_identity_crypto::canonicalize(&value).map_err(|e| e.to_string())?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

fn bin_root() -> Result<PathBuf, String> {
    std::env::var_os("HOME")
        .map(|p| PathBuf::from(p).join(".local/bin"))
        .ok_or_else(|| "HOME is required".into())
}

pub(crate) fn validate_sources(source: &Path, plan: &InstallPlan) -> Result<(), String> {
    let source = fs::canonicalize(source).map_err(|e| e.to_string())?;
    for link in &plan.links {
        if ["lastdb", "lastdbd", "folddb", "host-track"].contains(&link.command.as_str()) {
            return Err("app links cannot replace the primary program pair or host-track".into());
        }
        let path =
            fs::canonicalize(source.join(relative(&link.source)?)).map_err(|e| e.to_string())?;
        if !path.starts_with(&source) || !path.is_file() {
            return Err("app command escapes source tree".into());
        }
    }
    Ok(())
}

pub(crate) fn flip(link: &Path, target: &Path) -> Result<(), String> {
    let temporary = link.with_extension(format!("new-{}", std::process::id()));
    #[cfg(unix)]
    std::os::unix::fs::symlink(target, &temporary).map_err(|e| e.to_string())?;
    #[cfg(not(unix))]
    return Err("managed app links require a Unix host".into());
    fs::rename(&temporary, link).map_err(|e| {
        let _ = fs::remove_file(&temporary);
        e.to_string()
    })
}

pub(crate) fn install(root: &Path, plan: &InstallPlan) -> Result<(), String> {
    let source_dir = root.join("current/source");
    validate_sources(&source_dir, plan)?;
    let bin = bin_root()?;
    fs::create_dir_all(&bin).map_err(|e| e.to_string())?;
    for link in &plan.links {
        let target = bin.join(&link.command);
        if target
            .symlink_metadata()
            .is_ok_and(|m| !m.file_type().is_symlink())
        {
            return Err(format!(
                "refusing to replace a regular file: {}",
                target.display()
            ));
        }
        flip(&target, &source_dir.join(&link.source))?;
    }
    Ok(())
}

pub(crate) fn verify(root: &Path, plan: &InstallPlan) -> Result<(), String> {
    let source_dir = root.join("current/source");
    validate_sources(&source_dir, plan)?;
    let bin = bin_root()?;
    for link in &plan.links {
        let actual = fs::read_link(bin.join(&link.command)).map_err(|e| e.to_string())?;
        if actual != source_dir.join(&link.source) {
            return Err(format!("command link drift: {}", link.command));
        }
    }
    Ok(())
}

pub(crate) fn snapshot(plan: &InstallPlan) -> Result<Vec<(PathBuf, Option<PathBuf>)>, String> {
    let bin = bin_root()?;
    let mut result = Vec::with_capacity(plan.links.len());
    for link in &plan.links {
        let path = bin.join(&link.command);
        if path
            .symlink_metadata()
            .is_ok_and(|m| !m.file_type().is_symlink())
        {
            return Err(format!(
                "refusing to replace a regular file: {}",
                path.display()
            ));
        }
        let prior = fs::read_link(&path).ok();
        result.push((path, prior));
    }
    Ok(result)
}

pub(crate) fn restore(snapshot: &[(PathBuf, Option<PathBuf>)]) -> Result<(), String> {
    for (path, target) in snapshot {
        if let Some(target) = target {
            flip(path, target)?;
        } else if path.symlink_metadata().is_ok() {
            fs::remove_file(path).map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

pub(crate) fn authorized_transition(
    prior: &ManagedReceipt,
    resolution: &Resolution,
    entry: &ListEntry,
) -> bool {
    if !prior.trust.starts_with("pinned:")
        || !resolution.trust.starts_with("pinned:")
        || prior.app_id != resolution.app_id
        || prior.publisher != prior.listing.publisher_key
        || prior.listing.source != entry.source
    {
        return false;
    }
    [Environment::Dev, Environment::Prod]
        .into_iter()
        .any(|env| {
            let source = match (env, resolution.app_id.as_str()) {
                (Environment::Prod, "brain" | "lastsecrets" | "search") => {
                    format!("https://github.com/EdgeVector/{}.git", resolution.app_id)
                }
                (Environment::Dev, "registry-daily-dev") => {
                    "https://github.com/EdgeVector/lastdb-app-registry.git".to_owned()
                }
                _ => return false,
            };
            let base = registry_index_url_for(env);
            let (owner, delegate) = registry_ci_publisher_keys_for(env);
            let allowed_keys = (prior.publisher == owner && entry.publisher_key == delegate)
                || (prior.publisher == delegate && entry.publisher_key == owner);
            !delegate.is_empty()
                && !owner.is_empty()
                && allowed_keys
                && entry.source == source
                && prior.index == base
                && resolution.index
                    == format!("{base}/{}/api/{}.json", entry.channel, entry.api_version)
        })
}
