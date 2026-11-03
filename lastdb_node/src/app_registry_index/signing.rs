//! Legacy index types, verified index load, and trust-key resolution.
//! Detached signature sign/verify lives in `app_identity_crypto`.

use std::path::Path;

use app_identity_crypto::{verifying_key_from_base64, VerifyingKey};
use serde::{Deserialize, Serialize};

use super::{
    verify_index_bytes, IndexSignature, INDEX_VERSION, RELEASE_INDEX_PUBKEY_B64, TRUST_KEY_ENV,
};

// ─── Index types ──────────────────────────────────────────────────────────

/// One proved pair: this app version at this commit, proved with this node
/// build by this proof run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompatRow {
    pub app_version: String,
    /// Full git commit of the app source that was proved.
    pub sha: String,
    /// Optional signed artifact pointer; `None` means install from source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<String>,
    /// The node build string (`lastdbd --version`, `/api/version` `build`).
    pub lastdb_version: String,
    /// The node's HTTP API contract version at proof time, when the proof
    /// recorded it. Governs the request-grammar version reported by `GET
    /// /api/version`. Apps using the HTTP API (Brain, LastSecrets) check this
    /// before install.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lastdb_api_version: Option<u32>,
    /// The node's outbox format contract version at proof time, when the proof
    /// recorded it. Governs the IndexChangeBatch JSON schema written to the
    /// search inbox. Apps consuming the outbox (lastseek, Search) check this
    /// before install.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outbox_format_version: Option<u32>,
    /// RFC 3339 UTC timestamp of the proof.
    pub proved_at: String,
    /// The proof run id (routine + run id, or a path the operator can open).
    pub proof_run: String,
}

/// One app on the shelf, with every proved pair the channel carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexApp {
    pub app_id: String,
    /// Public git checkout URL. Install clones this and checks out `sha`.
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub compat: Vec<CompatRow>,
}

/// One channel's index file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistryIndex {
    pub index_version: u32,
    pub channel: String,
    pub generated_at: String,
    #[serde(default)]
    pub apps: Vec<IndexApp>,
}

// ─── Verify ───────────────────────────────────────────────────────────────

/// Parse and verify an index from its bytes and its detached signature.
///
/// # Errors
/// A bad signature is reported before a parse error: an unsigned file is
/// never read for content.
pub fn load_verified_index(
    trust: &VerifyingKey,
    index_bytes: &[u8],
    signature_bytes: &[u8],
) -> Result<RegistryIndex, String> {
    let signature: IndexSignature = serde_json::from_slice(signature_bytes)
        .map_err(|e| format!("index signature file does not parse: {e}"))?;
    verify_index_bytes(trust, index_bytes, &signature)?;
    parse_index(index_bytes)
}

/// Parse an index without a signature check. Only for `index show` and the
/// signer itself; every install path goes through [`load_verified_index`].
///
/// # Errors
/// Returns the parse error or an unsupported `index_version`.
pub fn parse_index(index_bytes: &[u8]) -> Result<RegistryIndex, String> {
    let index: RegistryIndex =
        serde_json::from_slice(index_bytes).map_err(|e| format!("index does not parse: {e}"))?;
    if index.index_version != INDEX_VERSION {
        return Err(format!(
            "index_version {} is not supported by this lastdb (reads {INDEX_VERSION}); run `brew upgrade lastdb`",
            index.index_version
        ));
    }
    Ok(index)
}

// ─── Trust ────────────────────────────────────────────────────────────────

/// Where the trusted verifying key came from, so the CLI can say so.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrustSource {
    Pinned,
    Flag,
    Env,
}

/// Resolve the verifying key: `--trust-key`, then `$LASTDB_REGISTRY_TRUST_KEY`,
/// then the pinned release key. Each override may be a base64 key or a path
/// to a file that holds one.
///
/// # Errors
/// Returns a parse error for a malformed override or a corrupt pinned key.
pub fn resolve_trust_key(flag: Option<&str>) -> Result<(VerifyingKey, TrustSource), String> {
    if let Some(raw) = flag {
        return Ok((parse_trust_key(raw, "--trust-key")?, TrustSource::Flag));
    }
    if let Ok(raw) = std::env::var(TRUST_KEY_ENV) {
        if !raw.trim().is_empty() {
            return Ok((parse_trust_key(&raw, TRUST_KEY_ENV)?, TrustSource::Env));
        }
    }
    let key = verifying_key_from_base64(RELEASE_INDEX_PUBKEY_B64)
        .map_err(|e| format!("pinned release index key is corrupt: {e:?}"))?;
    Ok((key, TrustSource::Pinned))
}

/// Resolve the KMS list key, keeping the legacy index key for old-file verification.
///
/// # Errors
/// A malformed explicit key or corrupt pinned public key.
pub fn resolve_list_trust_key(flag: Option<&str>) -> Result<(VerifyingKey, TrustSource), String> {
    if flag.is_some() || std::env::var(TRUST_KEY_ENV).is_ok_and(|v| !v.trim().is_empty()) {
        return resolve_trust_key(flag);
    }
    verifying_key_from_base64(super::REGISTRY_LIST_PUBKEY_B64)
        .map(|key| (key, TrustSource::Pinned))
        .map_err(|e| e.to_string())
}

fn parse_trust_key(raw: &str, what: &str) -> Result<VerifyingKey, String> {
    let raw = raw.trim();
    let text = if Path::new(raw).is_file() {
        std::fs::read_to_string(raw).map_err(|e| format!("{what}: failed to read {raw}: {e}"))?
    } else {
        raw.to_string()
    };
    verifying_key_from_base64(text.trim())
        .map_err(|e| format!("{what} is not a valid base64 Ed25519 public key: {e:?}"))
}
