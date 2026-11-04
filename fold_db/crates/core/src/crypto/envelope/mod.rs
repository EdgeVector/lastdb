use super::error::{CryptoError, CryptoResult};
use aes_gcm::{
    aead::{Aead, KeyInit, OsRng},
    Aes256Gcm, Nonce,
};
use rand::RngCore;

/// The envelope: AES-256-GCM, 12-byte nonce, 16-byte tag, no associated data.
/// `[version:1][nonce:12][ciphertext+tag]`.
pub const ENVELOPE_VERSION: u8 = 0x01;

/// Size constants for the envelope format
pub(crate) const NONCE_SIZE: usize = 12;
const VERSION_SIZE: usize = 1;
pub(crate) const TAG_SIZE: usize = 16;
/// Minimum ciphertext size: version(1) + nonce(12) + tag(16)
const MIN_ENVELOPE_SIZE: usize = VERSION_SIZE + NONCE_SIZE + TAG_SIZE;

/// Encrypt plaintext using AES-256-GCM with a random nonce.
///
/// Returns a self-describing binary envelope:
/// ```text
/// [version: 1B] [nonce: 12B] [ciphertext+tag: variable]
/// ```
///
/// The GCM authentication tag (16 bytes) is appended to the ciphertext
/// by the `aes-gcm` crate automatically.
pub fn encrypt_envelope(key: &[u8; 32], plaintext: &[u8]) -> CryptoResult<Vec<u8>> {
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| CryptoError::EncryptionFailed(format!("Invalid key: {e}")))?;

    // Generate a random 12-byte nonce
    let mut nonce_bytes = [0u8; NONCE_SIZE];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|e| CryptoError::EncryptionFailed(format!("AES-GCM encrypt: {e}")))?;

    // Build envelope: version || nonce || ciphertext+tag
    let mut envelope = Vec::with_capacity(VERSION_SIZE + NONCE_SIZE + ciphertext.len());
    envelope.push(ENVELOPE_VERSION);
    envelope.extend_from_slice(&nonce_bytes);
    envelope.extend_from_slice(&ciphertext);

    Ok(envelope)
}

/// Decrypt an envelope produced by `encrypt_envelope`.
///
/// Parses the version byte, extracts the nonce, and decrypts the ciphertext.
pub fn decrypt_envelope(key: &[u8; 32], envelope: &[u8]) -> CryptoResult<Vec<u8>> {
    if envelope.len() < MIN_ENVELOPE_SIZE {
        return Err(CryptoError::InvalidFormat(format!(
            "Envelope too short: {} bytes (minimum {})",
            envelope.len(),
            MIN_ENVELOPE_SIZE
        )));
    }

    let version = envelope[0];
    if version != ENVELOPE_VERSION {
        return Err(CryptoError::UnsupportedVersion(version));
    }

    let nonce_bytes = &envelope[VERSION_SIZE..VERSION_SIZE + NONCE_SIZE];
    let ciphertext = &envelope[VERSION_SIZE + NONCE_SIZE..];

    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| CryptoError::DecryptionFailed(format!("Invalid key: {e}")))?;

    let nonce = Nonce::from_slice(nonce_bytes);

    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| CryptoError::DecryptionFailed(format!("AES-GCM decrypt: {e}")))
}
