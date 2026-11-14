//! Node Ed25519 identity keyfile and public-key user-hash derivation.

use std::io::Write as _;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::Path;

use base64::Engine as _;
use sha2::{Digest, Sha256};

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
