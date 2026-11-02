//! The registry's signed list for one exact app interface.

use app_identity_crypto::{canonicalize, verify, verifying_key_from_base64};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashSet;

use super::{verify_index_bytes, IndexSignature};

/// One publisher's signed statement. The registry adds the owner and listing state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListEntry {
    pub app_id: String,
    pub channel: String,
    pub app_version: String,
    pub source: String,
    pub sha: String,
    pub api_version: u32,
    pub lastdb_build: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outbox_format_version: Option<u32>,
    pub publisher_account: String,
    pub publisher_key: String,
    pub publisher_sig: String,
    pub status: String,
    pub published_at: String,
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

    fn verify(&self) -> Result<(), String> {
        let key = verifying_key_from_base64(&self.publisher_key)
            .map_err(|e| format!("invalid publisher key for {}: {e}", self.app_id))?;
        let raw = BASE64
            .decode(&self.publisher_sig)
            .map_err(|e| e.to_string())?;
        let signature: [u8; 64] = raw
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
    pub list_version: u32,
    pub channel: String,
    pub api_version: u32,
    pub generated_at: String,
    pub seq: u64,
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
        trust: &app_identity_crypto::VerifyingKey,
        channel: &str,
        api_version: u32,
    ) -> Result<Self, String> {
        if bytes.len() > 2_097_152 {
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
        if self.apps.len() > 500 {
            return Err("registry list exceeds 500 apps".into());
        }
        let mut seen = HashSet::new();
        for entry in &self.apps {
            if entry.channel != channel
                || entry.api_version != api_version
                || entry.status != "listed"
                || !valid_app_id(&entry.app_id)
                || !seen.insert(&entry.app_id)
                || entry.sha.len() != 40
                || !entry
                    .sha
                    .bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
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
    let Some(repo) = value
        .strip_prefix("https://github.com/")
        .and_then(|s| s.strip_suffix(".git"))
    else {
        return false;
    };
    let parts: Vec<_> = repo.split('/').collect();
    parts.len() == 2
        && parts.iter().all(|p| {
            !p.is_empty()
                && p.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
        })
}
