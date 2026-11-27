//! The signed node release list: contract sections 2 to 4. A reader refuses the whole
//! list when any rule fails. The signature is checked on the exact downloaded bytes
//! before they are parsed.

use serde::{Deserialize, Serialize};

const ARCHIVE_PREFIXES: [&str; 2] = [
    "https://github.com/EdgeVector/lastdb/releases/download/",
    "https://github.com/EdgeVector/homebrew-lastdb/releases/download/",
];
const TARGETS: [&str; 3] = [
    "aarch64-apple-darwin",
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NodeList {
    pub(super) list_version: u32,
    pub(super) kind: String,
    pub(super) channel: String,
    pub(super) target: String,
    pub(super) generated_at: String,
    pub(super) seq: u64,
    pub(super) releases: Vec<Release>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Release {
    pub(super) version: String,
    pub(super) source_git_oid: String,
    pub(super) published_at: String,
    pub(super) archive_url: String,
    pub(super) archive_sha256: String,
    pub(super) archive_bytes: u64,
    pub(super) binaries: Vec<Binary>,
    pub(super) signing: String,
    pub(super) rollout: Rollout,
    pub(super) status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) withdrawn_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) withdrawn_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Binary {
    pub(super) name: String,
    pub(super) sha256: String,
    pub(super) size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Rollout {
    pub(super) percent: u64,
    pub(super) started_at: String,
}

impl Release {
    pub(super) fn active(&self) -> bool {
        self.status == "active"
    }
}

/// The target of this machine, as the list paths name it.
pub(super) fn host_target() -> Result<&'static str, String> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => Ok("aarch64-apple-darwin"),
        ("linux", "x86_64") => Ok("x86_64-unknown-linux-gnu"),
        ("linux", "aarch64") => Ok("aarch64-unknown-linux-gnu"),
        (os, arch) => Err(format!("no LastDB release exists for {os} on {arch}")),
    }
}

fn is_hex(text: &str, len: usize) -> bool {
    text.len() == len && text.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

fn digits(text: &str) -> bool {
    !text.is_empty() && text.len() <= 9 && text.bytes().all(|b| b.is_ascii_digit())
}

fn stable_version(version: &str) -> bool {
    let parts: Vec<&str> = version.split('.').collect();
    parts.len() == 3 && parts.iter().all(|p| digits(p))
}

/// `X.Y.Z-nightly.YYYYMMDD.g<9 hex>`: the last nine characters are the start of the
/// source commit, the rule `nightly_package.py` applies.
fn nightly_version(version: &str, oid: &str) -> bool {
    let Some((base, rest)) = version.split_once("-nightly.") else {
        return false;
    };
    let Some((date, commit)) = rest.split_once(".g") else {
        return false;
    };
    stable_version(base)
        && date.len() == 8
        && date.bytes().all(|b| b.is_ascii_digit())
        && is_hex(commit, 9)
        && oid.starts_with(commit)
}

pub(super) fn parse_time(text: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|t| t.with_timezone(&chrono::Utc))
}

impl NodeList {
    /// The contract's section 3 rules. Any failure refuses the whole list.
    pub(super) fn validate(&self, channel: &str, target: &str) -> Result<(), String> {
        if self.list_version != 1 || self.kind != "lastdb-node" {
            return Err("the list is not a LastDB node list version 1".into());
        }
        if !TARGETS.contains(&target) || self.channel != channel || self.target != target {
            return Err("the list is for another channel or target".into());
        }
        if self.seq == 0 || parse_time(&self.generated_at).is_none() {
            return Err("the list has no valid seq or generated_at".into());
        }
        if self.releases.is_empty() || self.releases.len() > 20 {
            return Err("the list must hold 1 to 20 releases".into());
        }
        let mut newest: Option<chrono::DateTime<chrono::Utc>> = None;
        for (i, release) in self.releases.iter().enumerate() {
            release.validate(channel, target)?;
            if self.releases[..i]
                .iter()
                .any(|r| r.version == release.version)
            {
                return Err(format!("the list names version {} twice", release.version));
            }
            let at =
                parse_time(&release.published_at).ok_or("a release has no valid published_at")?;
            if newest.is_some_and(|previous| at > previous) {
                return Err("the releases are not ordered newest first".into());
            }
            newest = Some(at);
        }
        Ok(())
    }
}

impl Release {
    fn validate(&self, channel: &str, target: &str) -> Result<(), String> {
        let v = &self.version;
        let oid_ok = is_hex(&self.source_git_oid, 40);
        let version_ok = if channel == "stable" {
            stable_version(v)
        } else {
            nightly_version(v, &self.source_git_oid)
        };
        if !oid_ok || !version_ok || !super::state::valid_version_dir(v) {
            return Err(format!(
                "release {v} has an invalid version or source commit"
            ));
        }
        if parse_time(&self.published_at).is_none() {
            return Err(format!("release {v} has no valid published_at"));
        }
        let suffix = format!("/lastdb-{target}.tar.gz");
        if !ARCHIVE_PREFIXES
            .iter()
            .any(|p| self.archive_url.starts_with(p))
            || !self.archive_url.ends_with(&suffix)
            || self.archive_url.contains(['?', '#', ' '])
        {
            return Err(format!(
                "release {v} names an archive outside the release hosts"
            ));
        }
        if !is_hex(&self.archive_sha256, 64) || self.archive_bytes == 0 {
            return Err(format!("release {v} has an invalid archive hash or size"));
        }
        let names: Vec<&str> = self.binaries.iter().map(|b| b.name.as_str()).collect();
        if names.len() != 2 || !names.contains(&"lastdb") || !names.contains(&"lastdbd") {
            return Err(format!("release {v} must name exactly lastdb and lastdbd"));
        }
        if self
            .binaries
            .iter()
            .any(|b| !is_hex(&b.sha256, 64) || b.size == 0)
        {
            return Err(format!("release {v} has an invalid binary hash or size"));
        }
        let signing = if target.ends_with("apple-darwin") {
            "developer-id"
        } else {
            "none"
        };
        if self.signing != signing {
            return Err(format!(
                "release {v} must be signed '{signing}' on {target}"
            ));
        }
        if self.rollout.percent > 100 || parse_time(&self.rollout.started_at).is_none() {
            return Err(format!("release {v} has an invalid rollout"));
        }
        match self.status.as_str() {
            "active" if self.withdrawn_reason.is_none() && self.withdrawn_at.is_none() => Ok(()),
            "withdrawn" => {
                let reason = self.withdrawn_reason.as_deref().unwrap_or("");
                let at_ok = self.withdrawn_at.as_deref().and_then(parse_time).is_some();
                if reason.is_empty()
                    || reason.chars().count() > 200
                    || reason.contains('\n')
                    || !at_ok
                {
                    Err(format!(
                        "release {v} is withdrawn without a valid reason or time"
                    ))
                } else {
                    Ok(())
                }
            }
            _ => Err(format!("release {v} has an invalid status")),
        }
    }
}
