//! The ONE node-identity root shared by every LastDB node binary.
//!
//! The canonical identity artifact is a plain 32-byte Ed25519 seed at
//! `<node-home>/identity.key`, owner-only (`0o600`) — the keyfile-root
//! decision (no OS keychain, no sealed-tree ceremony for the identity;
//! decided 2026-07-10, card `fold-unify-node-identity`, superseding the
//! sealed `node_identity` Sled tree as the identity's source of truth).
//! The same seed is both the node identity (mutation signer, public key)
//! and the E2E encryption root (`E2eKeys::from_ed25519_seed`) — the
//! documented single-key-root path in fold_db core.
//!
//! Both `lastdb_node` (the minimal `lastdbd` daemon) and `fold_db_node`
//! (the full desktop node) resolve identity through THIS crate, so the
//! same node home yields the same node public key and the same
//! `user_hash` in either binary. Socket clients (fbrain, fkanban, …)
//! would otherwise resolve a different owner identity per binary.

use std::io;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// File name of the identity seed under the node home (32 raw bytes, `0o600`).
pub const IDENTITY_KEY_FILE: &str = "identity.key";

/// Path of the identity keyfile under a node home.
pub fn seed_path(home: &Path) -> PathBuf {
    home.join(IDENTITY_KEY_FILE)
}

/// Load the 32-byte identity seed from `<home>/identity.key`, if present.
///
/// Distinguishes "no keyfile" (`Ok(None)`) from "keyfile exists but is
/// unreadable/corrupt" (`Err`): a corrupt keyfile must never be silently
/// treated as a fresh install, because generating a replacement would rotate
/// the node's user_hash and orphan its data.
pub fn load_seed(home: &Path) -> Result<Option<[u8; 32]>, String> {
    let key_path = seed_path(home);
    if !key_path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(&key_path)
        .map_err(|e| format!("failed to read {}: {e}", key_path.display()))?;
    let seed: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
        format!(
            "{} has invalid length {} (expected 32)",
            key_path.display(),
            bytes.len()
        )
    })?;
    Ok(Some(seed))
}

/// Load the identity seed, generating and persisting a fresh one (owner-only
/// `0o600`) on first boot. Reports whether a **new** seed was written
/// (`true` = first boot; caller should surface the 24-word recovery phrase).
pub fn load_or_generate_seed_with_meta(home: &Path) -> Result<([u8; 32], bool), String> {
    if let Some(seed) = load_seed(home)? {
        return Ok((seed, false));
    }
    let keypair = fold_db::security::Ed25519KeyPair::generate()
        .map_err(|e| format!("identity generation failed: {e}"))?;
    let seed = keypair.secret_key_bytes();
    write_seed(home, &seed)?;
    Ok((seed, true))
}

/// Persist an existing 32-byte seed as `<home>/identity.key` (owner-only),
/// refusing to clobber an existing file — two racing writers must not
/// silently overwrite the winner's identity. Used both by fresh-install
/// generation and by the one-way sealed-identity migration in the full node.
pub fn write_seed(home: &Path, seed: &[u8; 32]) -> Result<(), String> {
    std::fs::create_dir_all(home)
        .map_err(|e| format!("failed to create node home {}: {e}", home.display()))?;
    let key_path = seed_path(home);
    write_owner_only(&key_path, seed)
        .map_err(|e| format!("failed to write {}: {e}", key_path.display()))
}

/// Write `bytes` to `path` with owner-only permissions (`0o600`), refusing to
/// clobber an existing file.
fn write_owner_only(path: &Path, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write as _;
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt as _;

    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    opts.mode(0o600);
    let mut f = opts.open(path)?;
    f.write_all(bytes)?;
    f.sync_all()
}

/// Derive the canonical `user_hash` from a base64 Ed25519 public key: the
/// first 16 bytes of SHA-256 over the RAW key bytes, hex-encoded. Must stay
/// identical across every binary, or socket clients would resolve a
/// different owner identity per binary.
pub fn user_hash_from_pubkey(pubkey_b64: &str) -> Result<String, String> {
    use base64::Engine as _;
    let raw = base64::engine::general_purpose::STANDARD
        .decode(pubkey_b64)
        .map_err(|e| format!("node public key is not valid base64: {e}"))?;
    let digest = Sha256::digest(&raw);
    Ok(fold_db::hex::hex_lower(&digest[..16]))
}
