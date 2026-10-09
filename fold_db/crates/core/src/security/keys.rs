//! Ed25519 key generation and management

use crate::security::{SecurityError, SecurityResult};
use base64::{engine::general_purpose, Engine as _};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};

/// Ed25519 key pair for client-side use
#[derive(Debug)]
pub struct Ed25519KeyPair {
    signing_key: SigningKey,
    verifying_key: VerifyingKey,
}

impl Ed25519KeyPair {
    /// Generate a new Ed25519 key pair
    pub fn generate() -> SecurityResult<Self> {
        let signing_key = SigningKey::generate(&mut OsRng);
        let verifying_key = signing_key.verifying_key();

        Ok(Self {
            signing_key,
            verifying_key,
        })
    }

    /// Create a key pair from a secret key
    pub fn from_secret_key(secret_key: &[u8]) -> SecurityResult<Self> {
        // Ed25519 private keys are either 32 bytes (raw seed) or 64 bytes
        // (seed || derived public). Accept both so callers that stored the
        // 64-byte form (e.g. older identity writers) still round-trip.
        let seed = match secret_key.len() {
            32 => secret_key,
            64 => &secret_key[..32],
            n => {
                return Err(SecurityError::KeyGenerationFailed(format!(
                    "Ed25519 secret key must be 32 or 64 bytes, got {n}"
                )));
            }
        };

        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(seed);

        let signing_key = SigningKey::from_bytes(&key_bytes);
        let verifying_key = signing_key.verifying_key();

        Ok(Self {
            signing_key,
            verifying_key,
        })
    }

    /// Create a key pair from a base64-encoded secret key. This is the
    /// canonical loader for the node's persistent identity — the private
    /// key is stored as base64 in the identity tree. Fails loudly with a
    /// clear error when the input is missing, malformed, or the wrong
    /// length.
    pub fn from_secret_key_base64(secret_key_base64: &str) -> SecurityResult<Self> {
        if secret_key_base64.is_empty() {
            return Err(SecurityError::KeyGenerationFailed(
                "Ed25519 secret key is empty".to_string(),
            ));
        }
        let bytes = general_purpose::STANDARD
            .decode(secret_key_base64)
            .map_err(|e| {
                SecurityError::KeyGenerationFailed(format!(
                    "Ed25519 secret key is not valid base64: {e}"
                ))
            })?;
        Self::from_secret_key(&bytes)
    }

    /// Get the public key as bytes
    pub fn public_key_bytes(&self) -> [u8; 32] {
        self.verifying_key.to_bytes()
    }

    /// Get the secret key as bytes
    pub fn secret_key_bytes(&self) -> [u8; 32] {
        self.signing_key.to_bytes()
    }

    /// Get the public key as base64-encoded string
    pub fn public_key_base64(&self) -> String {
        general_purpose::STANDARD.encode(self.public_key_bytes())
    }

    /// Get the secret key as base64-encoded string
    pub fn secret_key_base64(&self) -> String {
        general_purpose::STANDARD.encode(self.secret_key_bytes())
    }

    /// Sign a message with this key pair
    pub fn sign(&self, message: &[u8]) -> Signature {
        self.signing_key.sign(message)
    }

    /// Verify a signature using the public key
    pub fn verify(&self, message: &[u8], signature: &Signature) -> bool {
        self.verifying_key.verify(message, signature).is_ok()
    }
}

/// Ed25519 public key for server-side verification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ed25519PublicKey {
    verifying_key: VerifyingKey,
}

impl Ed25519PublicKey {
    /// Create a public key from bytes
    pub fn from_bytes(bytes: &[u8]) -> SecurityResult<Self> {
        if bytes.len() != 32 {
            return Err(SecurityError::InvalidPublicKey(
                "Public key must be 32 bytes".to_string(),
            ));
        }

        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(bytes);

        let verifying_key = VerifyingKey::from_bytes(&key_bytes)
            .map_err(|e| SecurityError::InvalidPublicKey(e.to_string()))?;

        Ok(Self { verifying_key })
    }

    /// Create a public key from base64-encoded string
    pub fn from_base64(base64_key: &str) -> SecurityResult<Self> {
        let bytes = general_purpose::STANDARD
            .decode(base64_key)
            .map_err(|e| SecurityError::InvalidPublicKey(e.to_string()))?;

        Self::from_bytes(&bytes)
    }

    /// Get the public key as bytes
    pub fn to_bytes(&self) -> [u8; 32] {
        self.verifying_key.to_bytes()
    }

    /// Get the public key as base64-encoded string
    pub fn to_base64(&self) -> String {
        general_purpose::STANDARD.encode(self.to_bytes())
    }

    /// Verify a signature using this public key
    pub fn verify(&self, message: &[u8], signature: &Signature) -> bool {
        self.verifying_key.verify(message, signature).is_ok()
    }
}

/// Utility functions for key management
pub struct KeyUtils;

impl KeyUtils {
    /// Generate a unique key ID from a public key
    pub fn generate_key_id(public_key: &Ed25519PublicKey) -> String {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        hasher.update(public_key.to_bytes());
        let hash = hasher.finalize();

        // Use first 16 bytes of SHA256 hash as key ID
        general_purpose::STANDARD.encode(&hash[..16])
    }

    /// Parse a signature from base64-encoded string
    pub fn signature_from_base64(base64_sig: &str) -> SecurityResult<Signature> {
        let bytes = general_purpose::STANDARD
            .decode(base64_sig)
            .map_err(|e| SecurityError::InvalidSignature(e.to_string()))?;

        if bytes.len() != 64 {
            return Err(SecurityError::InvalidSignature(
                "Signature must be 64 bytes".to_string(),
            ));
        }

        let mut sig_bytes = [0u8; 64];
        sig_bytes.copy_from_slice(&bytes);

        Ok(Signature::from_bytes(&sig_bytes))
    }

    /// Convert a signature to base64-encoded string
    pub fn signature_to_base64(signature: &Signature) -> String {
        general_purpose::STANDARD.encode(signature.to_bytes())
    }

    /// Generate a random nonce
    pub fn generate_nonce() -> String {
        use rand::RngCore;

        let mut nonce = [0u8; 16];
        OsRng.fill_bytes(&mut nonce);
        general_purpose::STANDARD.encode(nonce)
    }
}
