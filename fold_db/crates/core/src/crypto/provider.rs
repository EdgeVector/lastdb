use super::envelope::{decrypt_envelope_with_context, encrypt_envelope};
use super::error::CryptoResult;
use async_trait::async_trait;

/// Trait for providing encryption/decryption of byte payloads.
///
/// Implementations handle key management internally. Callers
/// only see `encrypt(plaintext) -> ciphertext` and `decrypt(ciphertext) -> plaintext`.
#[async_trait]
pub trait CryptoProvider: Send + Sync {
    /// Encrypt plaintext bytes.
    ///
    /// Returns a self-describing ciphertext envelope that includes
    /// all metadata needed for decryption (nonce, version, etc.).
    async fn encrypt(&self, plaintext: &[u8]) -> CryptoResult<Vec<u8>>;

    /// Decrypt a ciphertext envelope back to plaintext.
    async fn decrypt(&self, ciphertext: &[u8]) -> CryptoResult<Vec<u8>>;
}

// ---------------------------------------------------------------------------
// NoOpCryptoProvider — passthrough, no encryption (for tests)
// ---------------------------------------------------------------------------

/// A no-op provider that passes data through without encryption.
/// Used for unit tests and migration scenarios.
pub struct NoOpCryptoProvider;

#[async_trait]
impl CryptoProvider for NoOpCryptoProvider {
    async fn encrypt(&self, plaintext: &[u8]) -> CryptoResult<Vec<u8>> {
        Ok(plaintext.to_vec())
    }

    async fn decrypt(&self, ciphertext: &[u8]) -> CryptoResult<Vec<u8>> {
        Ok(ciphertext.to_vec())
    }
}

// ---------------------------------------------------------------------------
// LocalCryptoProvider — file-based key, AES-256-GCM (for standalone dev)
// ---------------------------------------------------------------------------

/// A local crypto provider that derives its key from a file on disk.
///
/// On first use, if no key file exists, a random 256-bit key is generated
/// and written to `~/.fold_db/encryption.key`. Subsequent uses load
/// the same key.
///
/// This gives local dev parity with the cloud envelope encryption
/// pattern without requiring AWS KMS.
///
/// The key is zeroized when the provider is dropped (at-rest threat model
/// Gap G3, `docs/security/at-rest-threat-model.md`).
#[derive(zeroize::ZeroizeOnDrop)]
pub struct LocalCryptoProvider {
    key: [u8; 32],
}

impl LocalCryptoProvider {
    /// Create a provider from an explicit key (useful for tests).
    pub fn from_key(key: [u8; 32]) -> Self {
        Self { key }
    }
}

#[async_trait]
impl CryptoProvider for LocalCryptoProvider {
    async fn encrypt(&self, plaintext: &[u8]) -> CryptoResult<Vec<u8>> {
        encrypt_envelope(&self.key, plaintext)
    }

    async fn decrypt(&self, ciphertext: &[u8]) -> CryptoResult<Vec<u8>> {
        decrypt_envelope_with_context(&self.key, ciphertext, &[])
    }
}
