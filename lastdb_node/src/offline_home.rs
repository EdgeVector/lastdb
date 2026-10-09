//! Guards and key loading shared by the offline maintenance binaries
//! (`lastdb_local_maintain`, `lastdb_restore_probe`, the atom-content tools and
//! the smoke and proof binaries).
//!
//! Each binary used to carry its own copy of the primary-home guard. Two copies
//! checked only `~/.lastdb` and let the legacy `~/.folddb` home through. One
//! definition keeps every tool on the same list of protected homes.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use fold_db::crypto::E2eKeys;
use fold_db::security::Ed25519KeyPair;

/// Directory names under `$HOME` that hold a live primary or legacy primary home.
pub const PRIMARY_HOME_DIR_NAMES: [&str; 2] = [".lastdb", ".folddb"];

fn user_home() -> Result<PathBuf, String> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| "HOME unset".to_string())
}

/// The primary and legacy primary homes of the current user, canonical when the
/// directory exists. A home that does not exist is skipped.
pub fn existing_primary_homes() -> Result<Vec<PathBuf>, String> {
    Ok(existing_primary_homes_under(&user_home()?))
}

fn existing_primary_homes_under(user_home: &Path) -> Vec<PathBuf> {
    PRIMARY_HOME_DIR_NAMES
        .iter()
        .map(|name| user_home.join(name))
        .filter(|path| path.exists())
        .map(|path| std::fs::canonicalize(&path).unwrap_or(path))
        .collect()
}

/// Refuse `path` when it is, or lies under, a primary or legacy primary home.
///
/// A path that does not exist yet is resolved through its nearest existing
/// ancestor, so a missing path under a primary home is still refused.
pub fn refuse_primary(path: &Path) -> Result<(), String> {
    refuse_primary_under(&user_home()?, path)
}

fn refuse_primary_under(user_home: &Path, path: &Path) -> Result<(), String> {
    let abs = canonicalize_lenient(path);
    for name in PRIMARY_HOME_DIR_NAMES {
        let primary_abs = canonicalize_lenient(&user_home.join(name));
        if abs == primary_abs || abs.starts_with(&primary_abs) {
            return Err(format!(
                "refusing primary/legacy path {} — use a CoW copy first",
                abs.display()
            ));
        }
    }
    Ok(())
}

/// Canonicalize `path`, resolving a missing tail through the nearest existing
/// ancestor so symlinked prefixes (`/var` vs `/private/var`) still compare equal.
fn canonicalize_lenient(path: &Path) -> PathBuf {
    let mut tail = Vec::new();
    let mut cursor = path;
    loop {
        if let Ok(base) = std::fs::canonicalize(cursor) {
            return tail.iter().rev().fold(base, |acc, part| acc.join(part));
        }
        match (cursor.parent(), cursor.file_name()) {
            (Some(parent), Some(name)) => {
                tail.push(name.to_os_string());
                cursor = parent;
            }
            _ => return path.to_path_buf(),
        }
    }
}

/// Load the account E2E content keys and the device signing key from the
/// `identity.key` seed in `data_dir`.
pub fn load_e2e_keys(data_dir: &Path) -> Result<(E2eKeys, Arc<Ed25519KeyPair>), String> {
    let identity_path = data_dir.join("identity.key");
    let bytes = std::fs::read(&identity_path)
        .map_err(|e| format!("read {}: {e}", identity_path.display()))?;
    if bytes.len() < 32 {
        return Err(format!("identity.key too short ({} bytes)", bytes.len()));
    }
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&bytes[..32]);
    let e2e = E2eKeys::from_ed25519_seed(&seed).map_err(|e| format!("e2e keys: {e}"))?;
    let keypair =
        Arc::new(Ed25519KeyPair::from_secret_key(&seed).map_err(|e| format!("ed25519: {e}"))?);
    Ok((e2e, keypair))
}
