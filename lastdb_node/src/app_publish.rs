//! `lastdb app …` — developer-facing app registration + publish flow.
//!
//! Load app manifests, manage developer keys, and check schema coverage on
//! the local Mini daemon. Signed registry publication uses these helpers.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use app_identity_crypto::SigningKey;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

mod devkey;
mod http;
mod manifest;
mod schema_check;

pub use devkey::*;
use http::*;
pub use manifest::*;
pub use schema_check::*;

/// Human-facing metadata carried in `lastdb-app.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppMetadata {
    /// Display name shown in consent prompts and app lists. <= 80 chars.
    pub display_name: String,
    /// One-paragraph description. <= 500 chars.
    pub description: String,
    /// Homepage URL. <= 200 chars.
    pub homepage_url: String,
    /// Optional icon URL. <= 200 chars.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon_url: Option<String>,
}

/// Validate that an app version is a non-empty SemVer string without padding.
pub fn validate_app_version(version: &str) -> Result<(), String> {
    let trimmed = version.trim();
    if trimmed != version || trimmed.is_empty() {
        return Err(
            "version must be a non-empty SemVer string without surrounding whitespace".into(),
        );
    }
    semver::Version::parse(trimmed).map_err(|e| format!("version must be valid SemVer: {e}"))?;
    Ok(())
}
