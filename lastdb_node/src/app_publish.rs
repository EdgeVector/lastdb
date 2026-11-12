//! Developer signing key loading for app interface publication.

use std::path::{Path, PathBuf};

use app_identity_crypto::SigningKey;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;

pub fn default_dev_key_path(home: &Path) -> PathBuf {
    home.join("dev-signing.key")
}

pub fn validate_version(version: &str) -> Result<(), String> {
    if version.trim() != version || version.is_empty() {
        return Err(
            "version must be a non-empty SemVer string without surrounding whitespace".into(),
        );
    }
    semver::Version::parse(version)
        .map(|_| ())
        .map_err(|e| format!("version must be valid SemVer: {e}"))
}

pub fn load_dev_key(path: &Path) -> Result<SigningKey, String> {
    if path
        .to_str()
        .is_some_and(|v| v.starts_with("lastsecrets://"))
    {
        return Err("dev key path does not accept a lastsecrets locator".into());
    }
    let raw = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read dev key {}: {e}", path.display()))?;
    let bytes = BASE64
        .decode(raw.trim())
        .map_err(|e| format!("dev key {} is not valid base64: {e}", path.display()))?;
    let secret: [u8; 32] = bytes
        .try_into()
        .map_err(|_| format!("dev key {} is not a 32-byte Ed25519 secret", path.display()))?;
    Ok(SigningKey::from_bytes(&secret))
}
