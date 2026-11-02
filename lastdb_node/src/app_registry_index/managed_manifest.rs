//! Install instructions belong to the publisher's pinned source commit.

use serde::{Deserialize, Serialize};
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallStep {
    pub argv: Vec<String>,
    #[serde(default = "default_timeout")]
    pub timeout_s: u64,
    #[serde(default = "default_exits")]
    pub accept_exit: Vec<i32>,
}

fn default_timeout() -> u64 {
    30
}
fn default_exits() -> Vec<i32> {
    vec![0]
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallLink {
    pub source: String,
    /// A command name; links are confined to the user's ~/.local/bin.
    pub command: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallPlan {
    #[serde(default)]
    pub prepare: Vec<InstallStep>,
    #[serde(default)]
    pub post_install: Vec<InstallStep>,
    pub probe: Vec<InstallStep>,
    pub links: Vec<InstallLink>,
    #[serde(default)]
    pub requires: Vec<String>,
}

pub fn relative(path: &str) -> Result<PathBuf, String> {
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
        let raw = std::fs::read(source.join("lastdb-app.json")).map_err(|e| e.to_string())?;
        let value: serde_json::Value = serde_json::from_slice(&raw).map_err(|e| e.to_string())?;
        if value["app_id"].as_str() != Some(app) {
            return Err("source manifest app_id differs from signed listing".into());
        }
        let plan: Self = serde_json::from_value(value["install"].clone())
            .map_err(|e| format!("app needs an install manifest: {e}"))?;
        plan.validate()?;
        Ok(plan)
    }

    fn validate(&self) -> Result<(), String> {
        if self.probe.is_empty() || self.links.is_empty() {
            return Err("install needs links and at least one probe".into());
        }
        let mut commands = std::collections::HashSet::new();
        for link in &self.links {
            relative(&link.source)?;
            if link.command.starts_with('-')
                || !link
                    .command
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
                || link.command.is_empty()
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
        for app in &self.requires {
            if !super::valid_app_id(app) {
                return Err("invalid required app".into());
            }
        }
        Ok(())
    }

    pub fn consent_hash(&self) -> Result<String, String> {
        use sha2::{Digest, Sha256};
        let value = serde_json::to_value(self).map_err(|e| e.to_string())?;
        let bytes = app_identity_crypto::jcs::canonicalize(&value).map_err(|e| e.to_string())?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}
