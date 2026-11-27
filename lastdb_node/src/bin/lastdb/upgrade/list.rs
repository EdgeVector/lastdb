//! The signed node release list: contract sections 2 to 4. A reader refuses the whole
//! list when any rule fails. The signature is checked on the exact downloaded bytes
//! before they are parsed.

use std::fs;
use std::time::Duration;

use app_identity_crypto::{key_id, verifying_key_from_base64, VerifyingKey};
use base64::Engine as _;
use lastdb_node::profile::endpoints::{node_index_url_for, node_release_keys_for, Environment};
use serde::{Deserialize, Serialize};

use super::state::Paths;

/// A reader takes at most 256 KiB per object.
const MAX_OBJECT_BYTES: u64 = 262_144;
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    alg: String,
    key_id: String,
    payload_sha256: String,
    sig: String,
}

/// How a verification can fail. A mismatch of the two objects is the writer's gap
/// between uploading `.json` and `.sig`, so the reader retries once.
pub(super) enum VerifyError {
    Mismatch(String),
    Bad(String),
}

/// The pinned keys of an environment. There is no override.
pub(super) fn pinned_keys(env: Environment) -> Result<Vec<VerifyingKey>, String> {
    let keys: Result<Vec<VerifyingKey>, String> = node_release_keys_for(env)
        .iter()
        .map(|raw| verifying_key_from_base64(raw).map_err(|e| format!("pinned key: {e}")))
        .collect();
    let keys = keys?;
    if keys.is_empty() || keys.len() > 2 {
        return Err("this build pins no node release key".into());
    }
    Ok(keys)
}

/// Verify the envelope over the exact bytes, then parse and validate them.
pub(super) fn verify_list(
    keys: &[VerifyingKey],
    bytes: &[u8],
    signature: &[u8],
    channel: &str,
    target: &str,
) -> Result<NodeList, VerifyError> {
    if bytes.len() as u64 > MAX_OBJECT_BYTES || signature.len() as u64 > MAX_OBJECT_BYTES {
        return Err(VerifyError::Bad("the list is larger than 256 KiB".into()));
    }
    let envelope: Envelope = serde_json::from_slice(signature)
        .map_err(|e| VerifyError::Bad(format!("invalid list signature: {e}")))?;
    if envelope.alg != "ed25519" {
        return Err(VerifyError::Bad("the list signature is not ed25519".into()));
    }
    let key = keys
        .iter()
        .find(|k| key_id(k) == envelope.key_id)
        .ok_or_else(|| {
            VerifyError::Bad(format!(
                "the list is signed by key {}, which this build does not pin",
                envelope.key_id
            ))
        })?;
    let digest = fold_db::hex::sha256_hex(bytes);
    if envelope.payload_sha256 != digest {
        return Err(VerifyError::Mismatch(
            "the list and its signature do not describe the same bytes".into(),
        ));
    }
    let raw = base64::engine::general_purpose::STANDARD
        .decode(envelope.sig.trim())
        .map_err(|_| VerifyError::Bad("the list signature is not base64".into()))?;
    let sig: [u8; 64] = raw
        .try_into()
        .map_err(|_| VerifyError::Bad("the list signature is not 64 bytes".into()))?;
    app_identity_crypto::verify(key, &sig, digest.as_bytes())
        .map_err(|_| VerifyError::Bad("the list signature does not verify".into()))?;
    let list: NodeList = serde_json::from_slice(bytes)
        .map_err(|e| VerifyError::Bad(format!("invalid release list: {e}")))?;
    list.validate(channel, target).map_err(VerifyError::Bad)?;
    Ok(list)
}

pub(super) enum ListError {
    /// The network or the bucket did not answer.
    Unreachable(String),
    /// A second mismatch of the list and its signature: no update this check.
    NoUpdate(String),
    /// A list that fails a rule, or a `seq` that went down.
    Refused(String),
}

impl ListError {
    pub(super) fn message(&self) -> &str {
        match self {
            Self::Unreachable(m) | Self::NoUpdate(m) | Self::Refused(m) => m,
        }
    }
}

fn read_cache(
    paths: &Paths,
    keys: &[VerifyingKey],
    channel: &str,
    target: &str,
) -> Option<NodeList> {
    let (json, sig) = paths.cache_files(channel, target);
    let (bytes, signature) = (fs::read(json).ok()?, fs::read(sig).ok()?);
    verify_list(keys, &bytes, &signature, channel, target).ok()
}

/// The last verified list, with no network. `None` when none was fetched.
pub(super) fn cached_list(paths: &Paths, env: Environment, channel: &str) -> Option<NodeList> {
    let target = host_target().ok()?;
    let keys = pinned_keys(env).ok()?;
    read_cache(paths, &keys, channel, target)
}

fn write_cache(
    paths: &Paths,
    channel: &str,
    target: &str,
    bytes: &[u8],
    sig: &[u8],
) -> Result<(), String> {
    let (json, sigfile) = paths.cache_files(channel, target);
    super::state::atomic_write(&json, bytes, 0o600)?;
    super::state::atomic_write(&sigfile, sig, 0o600)
}

/// Fetch, verify and cache the list of `channel` for this machine. The reader
/// refuses a `seq` below the cached one, and equal `seq` must be equal bytes.
pub(super) fn load_list(
    paths: &Paths,
    env: Environment,
    channel: &str,
) -> Result<NodeList, ListError> {
    let target = host_target().map_err(ListError::Refused)?;
    let keys = pinned_keys(env).map_err(ListError::Refused)?;
    let base = node_index_url_for(env).trim_end_matches('/').to_owned();
    if !base.starts_with("https://") {
        return Err(ListError::Refused(
            "the release list URL must use HTTPS".into(),
        ));
    }
    let mut mismatch = String::new();
    for attempt in 0..2 {
        if attempt > 0 {
            std::thread::sleep(Duration::from_secs(2));
        }
        let (bytes, sig) = crate::block_on("release list", fetch_pair(&base, channel, target))
            .map_err(ListError::Unreachable)?;
        match verify_list(&keys, &bytes, &sig, channel, target) {
            Ok(list) => {
                let cached = read_cache(paths, &keys, channel, target);
                if let Some(old) = cached {
                    if list.seq < old.seq {
                        return Err(ListError::Refused(format!(
                            "the list has seq {}, below the {} this install already verified",
                            list.seq, old.seq
                        )));
                    }
                    if list.seq == old.seq {
                        let (json, _) = paths.cache_files(channel, target);
                        if fs::read(json).ok().as_deref() != Some(bytes.as_slice()) {
                            return Err(ListError::Refused(format!(
                                "two different lists carry seq {}",
                                list.seq
                            )));
                        }
                    }
                }
                write_cache(paths, channel, target, &bytes, &sig).map_err(ListError::Refused)?;
                return Ok(list);
            }
            Err(VerifyError::Mismatch(m)) => mismatch = m,
            Err(VerifyError::Bad(m)) => return Err(ListError::Refused(m)),
        }
    }
    Err(ListError::NoUpdate(mismatch))
}

async fn fetch_pair(base: &str, channel: &str, target: &str) -> Result<(Vec<u8>, Vec<u8>), String> {
    let client = crate::app_registry_index::registry_http_client()?;
    let url = format!("{base}/{channel}/{target}.json");
    let shown = format!("node list {channel}/{target}");
    let bytes = crate::app_registry_index::fetch_url_capped(client, &url, &shown, MAX_OBJECT_BYTES)
        .await
        .map_err(|e| {
            e.replacen(
                crate::app_registry_index::ABSENT,
                "the release list is absent",
                1,
            )
        })?;
    let sig = crate::app_registry_index::fetch_url_capped(
        client,
        &format!("{url}.sig"),
        &format!("{shown} signature"),
        MAX_OBJECT_BYTES,
    )
    .await?;
    Ok((bytes, sig))
}
