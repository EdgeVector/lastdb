//! AES-256-GCM envelope v1 — matches fold_db `encrypt_envelope` / `decrypt_envelope`.

use aes_gcm::{
    aead::{Aead, KeyInit, OsRng},
    Aes256Gcm, Nonce,
};
use rand::RngCore;

const ENVELOPE_VERSION: u8 = 0x01;
const NONCE_SIZE: usize = 12;
const MIN_ENVELOPE: usize = 1 + NONCE_SIZE + 16;

pub fn encrypt_envelope(key: &[u8; 32], plaintext: &[u8]) -> Result<Vec<u8>, String> {
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| e.to_string())?;
    let mut nonce_bytes = [0u8; NONCE_SIZE];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|e| format!("encrypt: {e}"))?;
    let mut out = Vec::with_capacity(1 + NONCE_SIZE + ciphertext.len());
    out.push(ENVELOPE_VERSION);
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

pub fn decrypt_envelope(key: &[u8; 32], envelope: &[u8]) -> Result<Vec<u8>, String> {
    if envelope.len() < MIN_ENVELOPE {
        return Err(format!("envelope too short: {}", envelope.len()));
    }
    if envelope[0] != ENVELOPE_VERSION {
        return Err(format!("unsupported envelope version {}", envelope[0]));
    }
    let nonce = Nonce::from_slice(&envelope[1..1 + NONCE_SIZE]);
    let ciphertext = &envelope[1 + NONCE_SIZE..];
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| e.to_string())?;
    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| format!("decrypt: {e}"))
}
