//! Shared Ed25519, JCS (RFC 8785), signed registry-list helpers, and the
//! schema push statement for `lastdb_node` and Schema Service.
#![warn(missing_docs)]

use std::collections::HashSet;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use ed25519_dalek::{Signature, Signer};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;

mod access;
mod schema_push;

pub use access::{Access, KNOWN_EXTENSIONS};
pub use ed25519_dalek::{SigningKey, VerifyingKey};
pub use schema_push::{schema_push_statement, verify_schema_push};

const PUBLIC_KEY_LEN: usize = ed25519_dalek::PUBLIC_KEY_LENGTH;
const SIGNATURE_LEN: usize = ed25519_dalek::SIGNATURE_LENGTH;

/// The channels a registry list can name.
pub const LIST_CHANNELS: [&str; 2] = ["stable", "next"];

/// The largest list (or list signature) a reader accepts, in bytes.
pub const MAX_LIST_BYTES: usize = 2_097_152;

const MAX_LIST_APPS: usize = 500;

/// Failure parsing a base64-encoded Ed25519 public key into a [`VerifyingKey`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum KeyParseError {
    #[error("public key is not valid base64")]
    /// The public key text is not standard base64.
    NotBase64,
    #[error("public key is not 32 bytes")]
    /// The decoded public key is not 32 bytes.
    WrongLength,
    #[error("public key bytes are not a valid Ed25519 point")]
    /// The 32 bytes are not a canonical Ed25519 point.
    InvalidPoint,
    #[error("public key is a small-order (weak) Ed25519 point and cannot verify any signature")]
    /// The point is a small-order Ed25519 key and cannot verify a signature.
    WeakKey,
}

/// Encode `bytes` as lowercase hex (two characters per byte).
#[must_use]
pub fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex_lower(&Sha256::digest(bytes))
}

/// Canonicalize a JSON value per RFC 8785 (JCS).
pub fn canonicalize(value: &Value) -> Result<Vec<u8>, serde_json::Error> {
    json_canon::to_vec(value)
}

/// Parse a base64 (standard alphabet, padded) Ed25519 public key into a
/// [`VerifyingKey`], rejecting non-canonical y-coordinates and small-order points.
pub fn verifying_key_from_base64(b64: &str) -> Result<VerifyingKey, KeyParseError> {
    let bytes = BASE64
        .decode(b64.as_bytes())
        .map_err(|_| KeyParseError::NotBase64)?;
    let arr: [u8; PUBLIC_KEY_LEN] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| KeyParseError::WrongLength)?;
    if !is_canonical_y(&arr) {
        return Err(KeyParseError::InvalidPoint);
    }
    let vk = VerifyingKey::from_bytes(&arr).map_err(|_| KeyParseError::InvalidPoint)?;
    if vk.is_weak() {
        return Err(KeyParseError::WeakKey);
    }
    Ok(vk)
}

fn is_canonical_y(bytes: &[u8; PUBLIC_KEY_LEN]) -> bool {
    for i in (0..PUBLIC_KEY_LEN).rev() {
        let y_byte = if i == 31 { bytes[i] & 0x7F } else { bytes[i] };
        let p_byte = match i {
            0 => 0xED,
            31 => 0x7F,
            _ => 0xFF,
        };
        if y_byte != p_byte {
            return y_byte < p_byte;
        }
    }
    false
}

/// Sign `payload` with `signing_key`. Returns the raw 64-byte signature.
pub fn sign(signing_key: &SigningKey, payload: &[u8]) -> [u8; SIGNATURE_LEN] {
    signing_key.sign(payload).to_bytes()
}

/// Verify that `sig` is a strict Ed25519 signature over `payload` under `verifying_key`.
pub fn verify(
    verifying_key: &VerifyingKey,
    sig: &[u8; SIGNATURE_LEN],
    payload: &[u8],
) -> Result<(), ed25519_dalek::SignatureError> {
    let signature = Signature::from_bytes(sig);
    verifying_key.verify_strict(payload, &signature)
}

/// Envelope `key_id` for a verifying key: lowercase hex SHA-256 of its 32 bytes.
#[must_use]
pub fn key_id(verifying_key: &VerifyingKey) -> String {
    sha256_hex(&verifying_key.to_bytes())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct IndexSignature {
    alg: String,
    key_id: String,
    payload_sha256: String,
    sig: String,
}

fn verify_index_bytes(
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
    /// Base64 Ed25519 signature over the entry's canonical signed fields.
    pub publisher_sig: String,
    /// Listing state. A valid list carries only `listed` entries.
    pub status: String,
    /// RFC 3339 UTC time of the publish.
    pub published_at: String,
    /// `public` (the default when absent) or `private`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audience: Option<String>,
    /// The LastDB access the app declares. Listings from before API 37 carry none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<Access>,
}

impl ListEntry {
    fn signed_fields(&self) -> Value {
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
        if let Some(access) = &self.access {
            fields["access"] = json!(access);
        }
        fields
    }

    fn verify(&self) -> Result<(), String> {
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
    // Inline so the JSON parse compiles in the calling crate, which already
    // holds a byte-slice parser; an out-of-line copy here adds a second one
    // to the Schema Service Lambda (15 MiB zip bar).
    #[inline]
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
                || entry.access.as_ref().is_some_and(|a| !a.well_formed())
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

/// The `(owner, repo)` pair of a valid GitHub source URL, or `None` when refused.
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
