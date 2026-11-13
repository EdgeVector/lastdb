//! Cryptographic primitives for E2E key derivation and AES-256-GCM envelopes.

use ed25519_compact::x25519;
use hkdf::Hkdf;
use sha2::Sha256;
use thiserror::Error;
use zeroize::{Zeroize, Zeroizing};

pub use at_rest::{seal_at_rest_raw, AT_REST_ENC_BINARY_PREFIX};
pub use envelope::{
    decrypt_envelope, decrypt_envelope_with_aad, encrypt_envelope, encrypt_envelope_with_aad,
};

#[derive(Debug, Error)]
pub enum CryptoError {
    #[error("Encryption failed: {0}")]
    EncryptionFailed(String),
    #[error("Decryption failed: {0}")]
    DecryptionFailed(String),
    #[error("Invalid ciphertext format: {0}")]
    InvalidFormat(String),
    #[error("Key management error: {0}")]
    KeyError(String),
    #[error("Unsupported envelope version: {0}")]
    UnsupportedVersion(u8),
}

pub type CryptoResult<T> = Result<T, CryptoError>;

#[derive(Clone, Zeroize, zeroize::ZeroizeOnDrop)]
pub struct E2eKeys {
    encryption_key: [u8; 32],
}

impl E2eKeys {
    pub fn from_ed25519_seed(seed: &[u8; 32]) -> CryptoResult<Self> {
        let ed_kp = ed25519_compact::KeyPair::from_seed(ed25519_compact::Seed::new(*seed));
        let x25519_sk = x25519::SecretKey::from_ed25519(&ed_kp.sk).map_err(|e| {
            CryptoError::KeyError(format!("Ed25519→X25519 conversion failed: {e:?}"))
        })?;
        let mut secret = Zeroizing::new([0u8; 32]);
        secret.copy_from_slice(x25519_sk.as_ref());
        let hk = Hkdf::<Sha256>::new(Some(b"fold:e2e:v1"), &*secret);
        let mut encryption_key = [0u8; 32];
        hk.expand(b"fold:content-key", &mut encryption_key)
            .expect("32 bytes is a valid HKDF-SHA256 output length");
        Ok(Self { encryption_key })
    }

    pub fn encryption_key(&self) -> [u8; 32] {
        self.encryption_key
    }

    pub fn from_content_key(encryption_key: [u8; 32]) -> CryptoResult<Self> {
        (encryption_key != [0u8; 32])
            .then_some(Self { encryption_key })
            .ok_or_else(|| CryptoError::KeyError("content key is empty".into()))
    }
}

pub mod envelope {
    use super::{CryptoError, CryptoResult};
    use aes_gcm::{
        aead::{Aead, AeadCore, KeyInit, OsRng, Payload},
        Aes256Gcm, Nonce,
    };

    pub const ENVELOPE_VERSION: u8 = 0x01;
    pub(crate) const NONCE_SIZE: usize = 12;
    pub(crate) const TAG_SIZE: usize = 16;
    const MIN_ENVELOPE_SIZE: usize = 1 + NONCE_SIZE + TAG_SIZE;

    pub fn encrypt_envelope(key: &[u8; 32], plaintext: &[u8]) -> CryptoResult<Vec<u8>> {
        encrypt_envelope_with_aad(key, b"", plaintext)
    }

    pub fn encrypt_envelope_with_aad(
        key: &[u8; 32],
        aad: &[u8],
        plaintext: &[u8],
    ) -> CryptoResult<Vec<u8>> {
        let cipher = Aes256Gcm::new_from_slice(key)
            .map_err(|e| CryptoError::EncryptionFailed(format!("Invalid key: {e}")))?;
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ct = cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .map_err(|e| CryptoError::EncryptionFailed(format!("AES-GCM encrypt: {e}")))?;
        let mut out = Vec::with_capacity(1 + NONCE_SIZE + ct.len());
        out.push(ENVELOPE_VERSION);
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ct);
        Ok(out)
    }

    pub fn decrypt_envelope(key: &[u8; 32], envelope: &[u8]) -> CryptoResult<Vec<u8>> {
        decrypt_envelope_with_aad(key, b"", envelope)
    }

    pub fn decrypt_envelope_with_aad(
        key: &[u8; 32],
        aad: &[u8],
        envelope: &[u8],
    ) -> CryptoResult<Vec<u8>> {
        if envelope.len() < MIN_ENVELOPE_SIZE {
            return Err(CryptoError::InvalidFormat(format!(
                "Envelope too short: {} bytes (minimum {MIN_ENVELOPE_SIZE})",
                envelope.len()
            )));
        }
        if envelope[0] != ENVELOPE_VERSION {
            return Err(CryptoError::UnsupportedVersion(envelope[0]));
        }
        let (nonce, msg) = envelope[1..].split_at(NONCE_SIZE);
        Aes256Gcm::new_from_slice(key)
            .map_err(|e| CryptoError::DecryptionFailed(format!("Invalid key: {e}")))?
            .decrypt(Nonce::from_slice(nonce), Payload { msg, aad })
            .map_err(|e| CryptoError::DecryptionFailed(format!("AES-GCM decrypt: {e}")))
    }
}

pub mod at_rest {
    use super::envelope::{decrypt_envelope, encrypt_envelope};
    use super::{CryptoError, CryptoResult};

    pub const AT_REST_ENC_BINARY_PREFIX: &str = "ENB:";

    pub fn seal_at_rest_raw(key: &[u8; 32], plaintext: &[u8]) -> CryptoResult<Vec<u8>> {
        let ct = encrypt_envelope(key, plaintext)?;
        let mut out = Vec::with_capacity(AT_REST_ENC_BINARY_PREFIX.len() + 1 + ct.len());
        out.extend_from_slice(AT_REST_ENC_BINARY_PREFIX.as_bytes());
        out.push(0);
        out.extend_from_slice(&ct);
        Ok(out)
    }

    pub fn open_at_rest_bounded(key: &[u8; 32], stored: &[u8], max: u64) -> CryptoResult<Vec<u8>> {
        let Some(rest) = stored
            .strip_prefix(AT_REST_ENC_BINARY_PREFIX.as_bytes())
            .filter(|_| {
                max > 0
                    && max.checked_add(1).is_some()
                    && usize::try_from(max).is_ok()
                    && stored.len() as u64 <= max
            })
        else {
            return Err(CryptoError::InvalidFormat(
                "bounded encrypted value required".into(),
            ));
        };
        let Some((&flags, ct)) = rest.split_first() else {
            return Err(CryptoError::InvalidFormat(
                "at-rest ENB envelope is missing its flags byte".into(),
            ));
        };
        if flags != 0 {
            return Err(CryptoError::InvalidFormat(format!(
                "at-rest ENB envelope has unsupported flags 0x{flags:02x}"
            )));
        }
        let plain = decrypt_envelope(key, ct)?;
        (plain.len() as u64 <= max).then_some(plain).ok_or_else(|| {
            CryptoError::InvalidFormat("decoded value exceeds explicit bound".into())
        })
    }
}
