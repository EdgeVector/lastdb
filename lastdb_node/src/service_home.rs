//! Admin service-home configuration, explicit `LASTDB_HOME` override detection, and node identity keyfile.

use std::io::Write as _;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};

use base64::Engine as _;
use sha2::{Digest, Sha256};

use crate::profile::paths;

pub const IDENTITY_KEY_FILE: &str = "identity.key";

pub fn load_or_generate_seed_with_meta(home: &Path) -> Result<([u8; 32], bool), String> {
    let key_path = home.join(IDENTITY_KEY_FILE);
    let kp = key_path.display();
    if key_path.exists() {
        let bytes = std::fs::read(&key_path).map_err(|e| format!("failed to read {kp}: {e}"))?;
        let len = bytes.len();
        let seed = bytes
            .as_slice()
            .try_into()
            .map_err(|_| format!("{kp} has invalid length {len} (expected 32)"))?;
        return Ok((seed, false));
    }
    let pair = fold_db::security::Ed25519KeyPair::generate()
        .map_err(|e| format!("identity generation failed: {e}"))?;
    let seed = pair.secret_key_bytes();
    std::fs::create_dir_all(home)
        .map_err(|e| format!("failed to create node home {}: {e}", home.display()))?;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    opts.mode(0o600);
    opts.open(&key_path)
        .and_then(|mut f| f.write_all(&seed).and_then(|()| f.sync_all()))
        .map_err(|e| format!("failed to write {kp}: {e}"))?;
    Ok((seed, true))
}

pub fn user_hash_from_pubkey(pubkey_b64: &str) -> Result<String, String> {
    let raw = base64::engine::general_purpose::STANDARD
        .decode(pubkey_b64)
        .map_err(|e| format!("node public key is not valid base64: {e}"))?;
    Ok(fold_db::hex::hex_lower(&Sha256::digest(&raw)[..16]))
}

pub fn explicit_home_override_present() -> bool {
    std::env::var_os(paths::LASTDB_HOME_ENV).is_some()
}

pub fn configured_home() -> Result<Option<PathBuf>, String> {
    let path = if let Some(p) = std::env::var_os("LASTDBD_SERVICE_HOME_CONFIG") {
        PathBuf::from(p)
    } else {
        let sub = if cfg!(target_os = "macos") {
            "Library/Application Support/LastDB/lastdbd-service-home"
        } else {
            ".config/lastdb/lastdbd-service-home"
        };
        let h = std::env::var_os("HOME")
            .ok_or("Cannot locate lastdb admin service-home config: HOME is not set")?;
        PathBuf::from(h).join(sub)
    };
    let raw = match std::fs::read_to_string(&path) {
        Ok(r) if r.trim().is_empty() => return Ok(None),
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(format!(
                "Failed to read lastdb admin service-home config {}: {e}",
                path.display()
            ));
        }
    };
    let home = paths::expand_tilde_path(raw.trim())?;
    if !home.is_absolute() {
        return Err(format!(
            "lastdbd service home must be absolute after ~ expansion (got {})",
            home.display()
        ));
    }
    let primary = std::env::var_os("HOME").map(|h| PathBuf::from(h).join(paths::LASTDB_DIR));
    if let Some(p) = primary.filter(|p| p.is_dir()) {
        let canon = std::fs::canonicalize(&p).unwrap_or(p);
        if std::fs::canonicalize(&home).unwrap_or_else(|_| home.clone()) == canon {
            return Err(format!(
                "Refusing to configure lastdbd service home as the primary live database home {}. Choose a separate directory for a second node.",
                canon.display()
            ));
        }
    }
    Ok(Some(home))
}
