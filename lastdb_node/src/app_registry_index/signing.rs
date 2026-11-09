//! Trust-key resolution for KMS-signed interface lists.
//! Detached signature verification lives in `app_identity_crypto`.

use std::path::Path;

use app_identity_crypto::{verifying_key_from_base64, VerifyingKey};

use super::{REGISTRY_LIST_PUBKEY_B64, TRUST_KEY_ENV};

/// Where the trusted verifying key came from, so the CLI can say so.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrustSource {
    Pinned,
    Flag,
    Env,
}

/// Resolve the verifying key for an interface list: `--trust-key`, then
/// `$LASTDB_REGISTRY_TRUST_KEY`, then the pinned registry list key. Each
/// override may be a base64 key or a path to a file that holds one.
///
/// # Errors
/// Returns a parse error for a malformed override or a corrupt pinned key.
pub fn resolve_list_trust_key(flag: Option<&str>) -> Result<(VerifyingKey, TrustSource), String> {
    if let Some(raw) = flag {
        return Ok((parse_trust_key(raw, "--trust-key")?, TrustSource::Flag));
    }
    if let Ok(raw) = std::env::var(TRUST_KEY_ENV) {
        if !raw.trim().is_empty() {
            return Ok((parse_trust_key(&raw, TRUST_KEY_ENV)?, TrustSource::Env));
        }
    }
    verifying_key_from_base64(REGISTRY_LIST_PUBKEY_B64)
        .map(|key| (key, TrustSource::Pinned))
        .map_err(|e| format!("pinned registry list key is corrupt: {e:?}"))
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
