//! The app registry's signed interface lists: grammar and verification.
//!
//! One list names every app listed for one channel and one node API version.
//! The registry signs the exact list bytes with a detached `.sig` file, and
//! each entry carries the publisher's own Ed25519 signature over its RFC 8785
//! fields. The LastDB node (app install) and Schema Service (catalog feed)
//! both read lists through this module, so both accept exactly the same
//! lists and refuse exactly the same ones.

use std::collections::HashSet;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::{
    canonicalize, hex_lower, key_id, sign, verify, verifying_key_from_base64, SigningKey,
    VerifyingKey, SIGNATURE_LEN,
};

/// Registry KMS Ed25519 public key. The private half never leaves AWS KMS.
pub const REGISTRY_LIST_PUBKEY_B64: &str = "3xySKyIR6nD2GjBXq15Jp6UtkasK2+OAhCUEvBd60sw=";

/// The channels a registry list can name.
pub const LIST_CHANNELS: [&str; 2] = ["stable", "next"];

/// The largest list (or list signature) a reader accepts, in bytes.
pub const MAX_LIST_BYTES: usize = 2_097_152;

/// The most apps one list may name.
pub const MAX_LIST_APPS: usize = 500;

/// The detached signature file next to a list (or a legacy index).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexSignature {
    /// Signature algorithm. Only `ed25519` is accepted.
    pub alg: String,
    /// Lowercase hex SHA-256 of the signing public key.
    pub key_id: String,
    /// Lowercase hex SHA-256 of the exact list bytes.
    pub payload_sha256: String,
    /// Base64 Ed25519 signature over the ASCII bytes of `payload_sha256`.
    pub sig: String,
}

/// Sign the exact bytes of a list or index file.
///
/// The digest covers the bytes; the signature covers the digest. A reader
/// hashes what it downloaded and refuses on either mismatch, so a signature
/// cannot be moved onto a different file.
#[must_use]
pub fn sign_index_bytes(key: &SigningKey, index_bytes: &[u8]) -> IndexSignature {
    let payload_sha256 = sha256_hex(index_bytes);
    let sig = sign(key, payload_sha256.as_bytes());
    IndexSignature {
        alg: "ed25519".to_string(),
        key_id: key_id(&key.verifying_key()),
        payload_sha256,
        sig: BASE64.encode(sig),
    }
}

/// Verify a detached signature against the exact bytes under `trust`.
///
/// # Errors
/// Names the first check that fails: algorithm, key id, digest, or signature.
pub fn verify_index_bytes(
    trust: &VerifyingKey,
    index_bytes: &[u8],
    signature: &IndexSignature,
) -> Result<(), String> {
    if signature.alg != "ed25519" {
        return Err(format!(
            "index signature alg is '{}', expected ed25519",
            signature.alg
        ));
    }
    let expected_key_id = key_id(trust);
    if signature.key_id != expected_key_id {
        return Err(format!(
            "index is signed by key {} but this binary trusts {}",
            signature.key_id, expected_key_id
        ));
    }
    let digest = sha256_hex(index_bytes);
    if signature.payload_sha256 != digest {
        return Err(format!(
            "index bytes hash to {digest} but the signature names {}",
            signature.payload_sha256
        ));
    }
    let raw = BASE64
        .decode(signature.sig.trim())
        .map_err(|e| format!("index signature is not valid base64: {e}"))?;
    let sig: [u8; SIGNATURE_LEN] = raw
        .try_into()
        .map_err(|_| "index signature is not 64 bytes".to_string())?;
    verify(trust, &sig, digest.as_bytes())
        .map_err(|_| "index signature does not verify".to_string())
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex_lower(&Sha256::digest(bytes))
}

/// One publisher's signed statement. The registry adds the owner and listing state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListEntry {
    /// The app id: a lowercase path segment, see [`valid_app_id`].
    pub app_id: String,
    /// The channel this entry is listed on.
    pub channel: String,
    /// The publisher's version label for this app commit.
    pub app_version: String,
    /// Public GitHub checkout URL, see [`valid_source`].
    pub source: String,
    /// Full lowercase git commit of the app source.
    pub sha: String,
    /// The node HTTP API version this entry targets.
    pub api_version: u32,
    /// The node build the publisher proved this commit with.
    pub lastdb_build: String,
    /// The outbox format version the app reads, when it reads the outbox.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outbox_format_version: Option<u32>,
    /// The registry account that owns this app id.
    pub publisher_account: String,
    /// Base64 Ed25519 public key of the publisher.
    pub publisher_key: String,
    /// Base64 Ed25519 signature over [`ListEntry::signed_fields`].
    pub publisher_sig: String,
    /// Listing state. A valid list carries only `listed` entries.
    pub status: String,
    /// RFC 3339 UTC time of the publish.
    pub published_at: String,
    /// `public` (the default when absent) or `private`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audience: Option<String>,
}

impl ListEntry {
    /// RFC 8785 input, shared by publish and list verification.
    #[must_use]
    pub fn signed_fields(&self) -> Value {
        let mut fields = json!({
            "app_id": self.app_id, "channel": self.channel, "app_version": self.app_version,
            "source": self.source, "sha": self.sha, "api_version": self.api_version,
            "lastdb_build": self.lastdb_build,
        });
        if let Some(version) = self.outbox_format_version {
            fields["outbox_format_version"] = json!(version);
        }
        if let Some(audience) = &self.audience {
            fields["audience"] = json!(audience);
        }
        fields
    }

    /// True when the publisher limited this entry to a private audience.
    #[must_use]
    pub fn is_private(&self) -> bool {
        self.audience.as_deref() == Some("private")
    }

    /// Verify the publisher signature over [`ListEntry::signed_fields`].
    ///
    /// # Errors
    /// A malformed publisher key or signature, or a signature that does not verify.
    pub fn verify(&self) -> Result<(), String> {
        let key = verifying_key_from_base64(&self.publisher_key)
            .map_err(|e| format!("invalid publisher key for {}: {e}", self.app_id))?;
        let raw = BASE64
            .decode(&self.publisher_sig)
            .map_err(|e| e.to_string())?;
        let signature: [u8; SIGNATURE_LEN] = raw
            .try_into()
            .map_err(|_| "publisher signature must be 64 bytes")?;
        let bytes = canonicalize(&self.signed_fields()).map_err(|e| e.to_string())?;
        verify(&key, &signature, &bytes)
            .map_err(|_| format!("publisher signature does not verify for {}", self.app_id))
    }
}

/// The only install input for one channel and API version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterfaceList {
    /// List grammar version. Only `1` is accepted.
    pub list_version: u32,
    /// The channel this list covers.
    pub channel: String,
    /// The node HTTP API version this list covers.
    pub api_version: u32,
    /// RFC 3339 UTC time the registry generated the list.
    pub generated_at: String,
    /// Monotonic sequence number. A reader refuses a rollback.
    pub seq: u64,
    /// The listed apps, one entry per app id.
    pub apps: Vec<ListEntry>,
}

impl InterfaceList {
    /// Authenticate the exact bytes before interpreting any listing.
    ///
    /// # Errors
    /// A registry or publisher signature failure, invalid grammar, or a different interface.
    pub fn load(
        bytes: &[u8],
        signature_bytes: &[u8],
        trust: &VerifyingKey,
        channel: &str,
        api_version: u32,
    ) -> Result<Self, String> {
        if bytes.len() > MAX_LIST_BYTES {
            return Err("registry list exceeds 2 MiB".into());
        }
        let signature: IndexSignature =
            serde_json::from_slice(signature_bytes).map_err(|e| e.to_string())?;
        verify_index_bytes(trust, bytes, &signature)?;
        let list: Self =
            serde_json::from_slice(bytes).map_err(|e| format!("invalid registry list: {e}"))?;
        list.validate(channel, api_version)?;
        Ok(list)
    }

    fn validate(&self, channel: &str, api_version: u32) -> Result<(), String> {
        if self.list_version != 1
            || self.channel != channel
            || self.api_version != api_version
            || api_version == 0
        {
            return Err(
                "registry list does not match the requested channel and API version".into(),
            );
        }
        if self.apps.len() > MAX_LIST_APPS {
            return Err("registry list exceeds 500 apps".into());
        }
        let mut seen = HashSet::new();
        for entry in &self.apps {
            if entry.channel != channel
                || entry.api_version != api_version
                || entry.status != "listed"
                || !valid_app_id(&entry.app_id)
                || !seen.insert(&entry.app_id)
                || !valid_sha(&entry.sha)
                || !valid_source(&entry.source)
                || entry.outbox_format_version == Some(0)
                || entry.publisher_account.is_empty()
                || entry
                    .audience
                    .as_deref()
                    .is_some_and(|v| v != "public" && v != "private")
            {
                return Err("registry list has an invalid or duplicate app entry".into());
            }
            entry.verify()?;
        }
        Ok(())
    }
}

fn valid_sha(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

/// A safe path segment for app installation.
#[must_use]
pub fn valid_app_id(value: &str) -> bool {
    (1..=40).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// Public GitHub source only; also prevents git option injection.
#[must_use]
pub fn valid_source(value: &str) -> bool {
    github_repo(value).is_some()
}

/// The `(owner, repo)` pair of a valid GitHub source URL.
///
/// Returns `None` for anything [`valid_source`] refuses.
#[must_use]
pub fn github_repo(value: &str) -> Option<(&str, &str)> {
    let repo = value
        .strip_prefix("https://github.com/")
        .and_then(|s| s.strip_suffix(".git"))?;
    let (owner, name) = repo.split_once('/')?;
    let segment_ok = |p: &str| {
        !p.is_empty()
            && p.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
    };
    (segment_ok(owner) && segment_ok(name)).then_some((owner, name))
}

/// A channel name is one of [`LIST_CHANNELS`].
///
/// # Errors
/// Returns a message naming the offending channel.
pub fn validate_channel(channel: &str) -> Result<(), String> {
    if LIST_CHANNELS.contains(&channel) {
        Ok(())
    } else {
        Err(format!(
            "invalid channel '{channel}' (expected stable or next)"
        ))
    }
}

/// The path of one interface list under a list base: `<channel>/api/<api>.json`.
#[must_use]
pub fn interface_list_path(channel: &str, api_version: u32) -> String {
    format!("{channel}/api/{api_version}.json")
}
