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

const HKDF_SALT: &[u8] = b"fold:e2e:v1";
const CONTENT_KEY_INFO: &[u8] = b"fold:content-key";

/// E2E AES-256-GCM content key derived from an identity secret and zeroized on drop.
#[derive(Clone, Zeroize, zeroize::ZeroizeOnDrop)]
pub struct E2eKeys {
    encryption_key: [u8; 32],
}

impl E2eKeys {
    pub fn from_secret(secret: &[u8; 32]) -> Self {
        let hk = Hkdf::<Sha256>::new(Some(HKDF_SALT), secret);
        let mut encryption_key = [0u8; 32];
        hk.expand(CONTENT_KEY_INFO, &mut encryption_key)
            .expect("32 bytes is a valid HKDF-SHA256 output length");
        Self { encryption_key }
    }

    pub fn from_ed25519_seed(seed: &[u8; 32]) -> CryptoResult<Self> {
        let ed_seed = ed25519_compact::Seed::new(*seed);
        let ed_kp = ed25519_compact::KeyPair::from_seed(ed_seed);
        let x25519_sk = x25519::SecretKey::from_ed25519(&ed_kp.sk).map_err(|e| {
            CryptoError::KeyError(format!("Ed25519→X25519 conversion failed: {e:?}"))
        })?;
        let mut secret = Zeroizing::new([0u8; 32]);
        secret.copy_from_slice(x25519_sk.as_ref());
        Ok(Self::from_secret(&secret))
    }

    pub fn encryption_key(&self) -> [u8; 32] {
        self.encryption_key
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
    const VERSION_SIZE: usize = 1;
    pub(crate) const TAG_SIZE: usize = 16;
    const MIN_ENVELOPE_SIZE: usize = VERSION_SIZE + NONCE_SIZE + TAG_SIZE;

    pub fn encrypt_envelope(key: &[u8; 32], plaintext: &[u8]) -> CryptoResult<Vec<u8>> {
        let cipher = Aes256Gcm::new_from_slice(key)
            .map_err(|e| CryptoError::EncryptionFailed(format!("Invalid key: {e}")))?;
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = cipher
            .encrypt(&nonce, plaintext)
            .map_err(|e| CryptoError::EncryptionFailed(format!("AES-GCM encrypt: {e}")))?;
        let mut envelope = Vec::with_capacity(VERSION_SIZE + NONCE_SIZE + ciphertext.len());
        envelope.push(ENVELOPE_VERSION);
        envelope.extend_from_slice(&nonce);
        envelope.extend_from_slice(&ciphertext);
        Ok(envelope)
    }

    /// Same envelope as `encrypt_envelope`, with associated data authenticated
    /// and left outside the ciphertext. The caller must present the same bytes
    /// to `decrypt_envelope_with_aad`.
    pub fn encrypt_envelope_with_aad(
        key: &[u8; 32],
        aad: &[u8],
        plaintext: &[u8],
    ) -> CryptoResult<Vec<u8>> {
        let cipher = Aes256Gcm::new_from_slice(key)
            .map_err(|e| CryptoError::EncryptionFailed(format!("Invalid key: {e}")))?;
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .map_err(|e| CryptoError::EncryptionFailed(format!("AES-GCM encrypt: {e}")))?;
        let mut envelope = Vec::with_capacity(VERSION_SIZE + NONCE_SIZE + ciphertext.len());
        envelope.push(ENVELOPE_VERSION);
        envelope.extend_from_slice(&nonce);
        envelope.extend_from_slice(&ciphertext);
        Ok(envelope)
    }

    pub fn decrypt_envelope(key: &[u8; 32], envelope: &[u8]) -> CryptoResult<Vec<u8>> {
        if envelope.len() < MIN_ENVELOPE_SIZE {
            return Err(CryptoError::InvalidFormat(format!(
                "Envelope too short: {} bytes (minimum {MIN_ENVELOPE_SIZE})",
                envelope.len()
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
        cipher
            .decrypt(Nonce::from_slice(nonce_bytes), ciphertext)
            .map_err(|e| CryptoError::DecryptionFailed(format!("AES-GCM decrypt: {e}")))
    }

    /// Open an envelope produced by `encrypt_envelope_with_aad`.
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
        let version = envelope[0];
        if version != ENVELOPE_VERSION {
            return Err(CryptoError::UnsupportedVersion(version));
        }
        let nonce_bytes = &envelope[VERSION_SIZE..VERSION_SIZE + NONCE_SIZE];
        let ciphertext = &envelope[VERSION_SIZE + NONCE_SIZE..];
        let cipher = Aes256Gcm::new_from_slice(key)
            .map_err(|e| CryptoError::DecryptionFailed(format!("Invalid key: {e}")))?;
        cipher
            .decrypt(
                Nonce::from_slice(nonce_bytes),
                Payload {
                    msg: ciphertext,
                    aad,
                },
            )
            .map_err(|e| CryptoError::DecryptionFailed(format!("AES-GCM decrypt: {e}")))
    }
}

pub mod at_rest {
    use super::envelope::{decrypt_envelope, encrypt_envelope};
    use super::{CryptoError, CryptoResult};

    pub const AT_REST_ENC_BINARY_PREFIX: &str = "ENB:";

    pub fn seal_at_rest_raw(key: &[u8; 32], plaintext: &[u8]) -> CryptoResult<Vec<u8>> {
        let ciphertext = encrypt_envelope(key, plaintext)?;
        let mut out = Vec::with_capacity(AT_REST_ENC_BINARY_PREFIX.len() + 1 + ciphertext.len());
        out.extend_from_slice(AT_REST_ENC_BINARY_PREFIX.as_bytes());
        out.push(0);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    fn decode_binary_ciphertext(stored: &[u8]) -> CryptoResult<Option<&[u8]>> {
        if !stored.starts_with(AT_REST_ENC_BINARY_PREFIX.as_bytes()) {
            return Ok(None);
        }
        let Some((&flags, ciphertext)) = stored[AT_REST_ENC_BINARY_PREFIX.len()..].split_first()
        else {
            return Err(CryptoError::InvalidFormat(
                "at-rest ENB envelope is missing its flags byte".to_string(),
            ));
        };
        if flags != 0 {
            return Err(CryptoError::InvalidFormat(format!(
                "at-rest ENB envelope has unsupported flags 0x{flags:02x}"
            )));
        }
        Ok(Some(ciphertext))
    }

    pub fn open_at_rest_bounded(key: &[u8; 32], stored: &[u8], max: u64) -> CryptoResult<Vec<u8>> {
        if max == 0
            || max.checked_add(1).is_none()
            || usize::try_from(max).is_err()
            || stored.len() as u64 > max
            || !stored.starts_with(AT_REST_ENC_BINARY_PREFIX.as_bytes())
        {
            return Err(CryptoError::InvalidFormat(
                "bounded encrypted value required".into(),
            ));
        }
        let Some(ciphertext) = decode_binary_ciphertext(stored)? else {
            return Err(CryptoError::InvalidFormat(
                "at-rest value has no ENB: marker".into(),
            ));
        };
        let plain = decrypt_envelope(key, ciphertext)?;
        if plain.len() as u64 > max {
            return Err(CryptoError::InvalidFormat(
                "decoded value exceeds explicit bound".into(),
            ));
        }
        Ok(plain)
    }
}
