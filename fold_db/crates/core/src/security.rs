//! Ed25519 key pair generation, message signing, and verification.

use base64::{engine::general_purpose, Engine as _};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SecurityError {
    #[error("Key generation failed: {0}")]
    KeyGenerationFailed(String),
}

pub type SecurityResult<T> = Result<T, SecurityError>;

#[derive(Debug)]
pub struct Ed25519KeyPair {
    signing_key: SigningKey,
    verifying_key: VerifyingKey,
}

impl Ed25519KeyPair {
    pub fn generate() -> SecurityResult<Self> {
        let signing_key = SigningKey::generate(&mut OsRng);
        let verifying_key = signing_key.verifying_key();
        Ok(Self {
            signing_key,
            verifying_key,
        })
    }

    pub fn from_secret_key(secret_key: &[u8]) -> SecurityResult<Self> {
        let key_bytes: [u8; 32] = secret_key.try_into().map_err(|_| {
            SecurityError::KeyGenerationFailed(format!(
                "Ed25519 secret key must be 32 bytes, got {}",
                secret_key.len()
            ))
        })?;
        let signing_key = SigningKey::from_bytes(&key_bytes);
        let verifying_key = signing_key.verifying_key();
        Ok(Self {
            signing_key,
            verifying_key,
        })
    }

    pub fn public_key_bytes(&self) -> [u8; 32] {
        self.verifying_key.to_bytes()
    }

    pub fn secret_key_bytes(&self) -> [u8; 32] {
        self.signing_key.to_bytes()
    }

    pub fn public_key_base64(&self) -> String {
        general_purpose::STANDARD.encode(self.public_key_bytes())
    }

    pub fn sign(&self, message: &[u8]) -> Signature {
        self.signing_key.sign(message)
    }

    pub fn verify(&self, message: &[u8], signature: &Signature) -> bool {
        self.verifying_key.verify(message, signature).is_ok()
    }
}

pub struct KeyUtils;

impl KeyUtils {
    pub fn signature_to_base64(signature: &Signature) -> String {
        general_purpose::STANDARD.encode(signature.to_bytes())
    }
}
