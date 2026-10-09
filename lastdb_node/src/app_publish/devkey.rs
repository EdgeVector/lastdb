//! Developer signing key files.

use super::*;

/// Generate a new developer Ed25519 signing key at `path` (0600, base64 of
/// the 32-byte secret). Refuses to overwrite an existing key.
pub fn generate_dev_key(path: &Path) -> Result<String, String> {
    if path.exists() {
        return Err(format!(
            "dev key already exists at {} — delete it first if you really mean to rotate",
            path.display()
        ));
    }
    let mut secret = [0u8; 32];
    let mut urandom = std::fs::File::open("/dev/urandom")
        .map_err(|e| format!("failed to open /dev/urandom: {e}"))?;
    urandom
        .read_exact(&mut secret)
        .map_err(|e| format!("failed to read entropy: {e}"))?;
    let key = SigningKey::from_bytes(&secret);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
    }
    std::fs::write(path, BASE64.encode(secret))
        .map_err(|e| format!("failed to write {}: {e}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(|e| format!("failed to chmod {}: {e}", path.display()))?;
    }
    Ok(BASE64.encode(key.verifying_key().to_bytes()))
}

pub fn load_dev_key(path: &Path) -> Result<SigningKey, String> {
    let raw = std::fs::read_to_string(path).map_err(|e| {
        format!(
            "failed to read dev key {} (run `lastdb app dev-init` first): {e}",
            path.display()
        )
    })?;
    let bytes = BASE64
        .decode(raw.trim())
        .map_err(|e| format!("dev key {} is not valid base64: {e}", path.display()))?;
    let secret: [u8; 32] = bytes
        .try_into()
        .map_err(|_| format!("dev key {} is not a 32-byte Ed25519 secret", path.display()))?;
    Ok(SigningKey::from_bytes(&secret))
}

/// Default developer key location under the resolved node home.
pub fn default_dev_key_path(home: &Path) -> PathBuf {
    home.join("dev-signing.key")
}

// ─── Owner-socket HTTP (declare + auto-identity) ──────────────────────────
