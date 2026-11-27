//! Fetching and verifying the signed list: contract sections 2 and 4. The signature is
//! checked on the exact downloaded bytes before they are parsed.

use std::fs;
use std::time::Duration;

use app_identity_crypto::{key_id, verifying_key_from_base64, VerifyingKey};
use base64::Engine as _;
use lastdb_node::profile::endpoints::{node_index_url_for, node_release_keys_for, Environment};
use serde::Deserialize;

use super::list::{host_target, NodeList};
use super::state::Paths;

/// A reader takes at most 256 KiB per object.
const MAX_OBJECT_BYTES: u64 = 262_144;

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
