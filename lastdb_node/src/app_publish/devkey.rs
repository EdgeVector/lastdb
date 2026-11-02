//! Developer signing key files.

use super::*;

/// Generate a new developer Ed25519 signing key at `path` (0600, base64 of
/// the 32-byte secret). Refuses to overwrite an existing key.
pub fn generate_dev_key(path: &Path) -> Result<String, String> {
    let locator = path.to_str().and_then(|v| v.strip_prefix("lastsecrets://"));
    if let Some(slug) = locator {
        let exists = std::process::Command::new("lastsecrets")
            .args(["get", slug])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(|_| "LastSecrets is unavailable")?;
        if exists.success() {
            return Err("publisher key already exists in LastSecrets".into());
        }
    }
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
    if let Some(slug) = locator {
        store_person_key(slug, &BASE64.encode(secret))?;
        return Ok(BASE64.encode(key.verifying_key().to_bytes()));
    }
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

fn store_person_key(slug: &str, secret: &str) -> Result<(), String> {
    use std::io::Write as _;
    use std::process::{Command, Stdio};
    let mut child = Command::new("lastsecrets")
        .args([
            "put",
            slug,
            "--label",
            "LastDB registry publisher",
            "--provider",
            "lastdb",
            "--purpose",
            "Person app publishing key",
            "--env",
            "person",
            "--value-stdin",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|_| "LastSecrets is unavailable")?;
    let mut stdin = child.stdin.take().ok_or("LastSecrets input unavailable")?;
    stdin
        .write_all(secret.as_bytes())
        .map_err(|_| "LastSecrets key input failed")?;
    drop(stdin);
    if !child
        .wait()
        .map_err(|_| "LastSecrets write failed")?
        .success()
    {
        return Err("LastSecrets write failed".into());
    }
    Ok(())
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
