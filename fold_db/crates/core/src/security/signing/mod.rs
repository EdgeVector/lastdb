//! Message signing and verification functionality

use crate::security::{
    Ed25519PublicKey, KeyUtils, PublicKeyInfo, SecurityError, SecurityResult, SignedMessage,
    VerificationResult,
};
use base64::{engine::general_purpose, Engine as _};
use std::sync::{Arc, RwLock, RwLockReadGuard};

/// Acquire a read guard on `lock`, mapping poison to `SecurityError::KeyNotFound`.
fn read_lock<T>(lock: &RwLock<T>) -> SecurityResult<RwLockReadGuard<'_, T>> {
    lock.read()
        .map_err(|_| SecurityError::KeyNotFound("Failed to acquire read lock".to_string()))
}

/// Verifies a molecule signature against canonical bytes.
/// Returns true if valid, false otherwise. Never panics.
pub fn verify_molecule_signature(
    canonical_bytes: &[u8],
    signature_base64: &str,
    writer_pubkey_base64: &str,
) -> bool {
    let Ok(pubkey) = Ed25519PublicKey::from_base64(writer_pubkey_base64) else {
        return false;
    };
    let Ok(signature) = KeyUtils::signature_from_base64(signature_base64) else {
        return false;
    };
    pubkey.verify(canonical_bytes, &signature)
}

/// Message verifier for server-side use
pub struct MessageVerifier {
    /// The registered public key (in-memory cache)
    public_key: Arc<RwLock<Option<PublicKeyInfo>>>,
    /// Maximum allowed timestamp drift in seconds
    max_timestamp_drift: i64,
}

impl MessageVerifier {
    /// Create a new message verifier
    pub fn new(max_timestamp_drift: i64) -> Self {
        Self {
            public_key: Arc::new(RwLock::new(None)),
            max_timestamp_drift,
        }
    }

    /// Get the system public key info
    pub fn get_system_public_key(&self) -> SecurityResult<Option<PublicKeyInfo>> {
        Ok(read_lock(&self.public_key)?.clone())
    }

    /// Verify a signed message
    pub fn verify_message(
        &self,
        signed_message: &SignedMessage,
    ) -> SecurityResult<VerificationResult> {
        // Get the public key info
        let Some(key_info) = self.get_system_public_key()? else {
            return Ok(VerificationResult::failure(
                "System public key not found".to_string(),
            ));
        };

        // Check if key is valid (not expired, active, etc.)
        if !key_info.is_valid() {
            return Ok(VerificationResult::failure(
                "Public key is not valid (expired or inactive)".to_string(),
            ));
        }

        // Check timestamp validity
        let timestamp_valid = self.is_timestamp_valid(signed_message.timestamp);

        // Parse the public key
        let public_key = match Ed25519PublicKey::from_base64(&key_info.public_key) {
            Ok(key) => key,
            Err(e) => {
                return Ok(VerificationResult::failure(format!(
                    "Invalid public key format: {e}"
                )))
            }
        };

        // Parse the signature
        let signature = match KeyUtils::signature_from_base64(&signed_message.signature) {
            Ok(sig) => sig,
            Err(e) => {
                return Ok(VerificationResult::failure(format!(
                    "Invalid signature format: {e}"
                )))
            }
        };

        // Recreate the message that was signed
        let message_to_verify = match self.create_message_to_verify(signed_message) {
            Ok(msg) => msg,
            Err(e) => {
                return Ok(VerificationResult::failure(format!(
                    "Failed to recreate message: {e}"
                )))
            }
        };

        // Verify the signature
        let is_valid = public_key.verify(&message_to_verify, &signature);

        if is_valid && timestamp_valid {
            Ok(VerificationResult::success(key_info, timestamp_valid))
        } else {
            Ok(VerificationResult::failure(
                "Signature verification failed".to_string(),
            ))
        }
    }

    /// Check if timestamp is within acceptable range
    fn is_timestamp_valid(&self, timestamp: i64) -> bool {
        let current_time = chrono::Utc::now().timestamp();
        // Widen to i128 so an attacker-controlled wire timestamp (incl.
        // i64::MIN) can't overflow the subtraction or trip i64::MIN.abs()'s
        // panic. The difference of two i64 values always fits in i128.
        let drift_secs = (i128::from(current_time) - i128::from(timestamp)).abs();
        drift_secs <= i128::from(self.max_timestamp_drift)
    }

    /// Recreate the original signed message for verification
    fn create_message_to_verify(&self, signed_message: &SignedMessage) -> SecurityResult<Vec<u8>> {
        let mut message = general_purpose::STANDARD
            .decode(&signed_message.payload)
            .map_err(|e| SecurityError::DeserializationError(e.to_string()))?;
        message.extend_from_slice(&signed_message.timestamp.to_be_bytes());
        message.extend_from_slice(signed_message.public_key_id.as_bytes());
        Ok(message)
    }
}
