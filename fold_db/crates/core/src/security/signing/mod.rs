//! Message signing and verification functionality

use crate::{
    constants::SINGLE_PUBLIC_KEY_ID,
    db_operations::DbOperations,
    security::{
        Ed25519PublicKey, KeyUtils, PublicKeyInfo, SecurityError, SecurityResult, SignedMessage,
        VerificationResult,
    },
};
use base64::{engine::general_purpose, Engine as _};
use serde_json::Value;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Acquire a read guard on `lock`, mapping poison to `SecurityError::KeyNotFound`.
fn read_lock<T>(lock: &RwLock<T>) -> SecurityResult<RwLockReadGuard<'_, T>> {
    lock.read()
        .map_err(|_| SecurityError::KeyNotFound("Failed to acquire read lock".to_string()))
}

/// Acquire a write guard on `lock`, mapping poison to `SecurityError::KeyNotFound`.
fn write_lock<T>(lock: &RwLock<T>) -> SecurityResult<RwLockWriteGuard<'_, T>> {
    lock.write()
        .map_err(|_| SecurityError::KeyNotFound("Failed to acquire write lock".to_string()))
}

/// Signs canonical molecule bytes with the given keypair.
/// Returns (signature_base64, writer_pubkey_base64).
///
/// # Panics
/// Panics if signing somehow fails (indicates broken invariant).
pub fn sign_molecule_update(
    canonical_bytes: &[u8],
    keypair: &crate::security::Ed25519KeyPair,
) -> (String, String) {
    let signature = keypair.sign(canonical_bytes);
    let signature_base64 = KeyUtils::signature_to_base64(&signature);
    let writer_pubkey_base64 = keypair.public_key_base64();
    (signature_base64, writer_pubkey_base64)
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

/// Message signer for client-side use
pub struct MessageSigner {
    keypair: crate::security::Ed25519KeyPair,
}

impl MessageSigner {
    /// Create a new message signer with a key pair
    pub fn new(keypair: crate::security::Ed25519KeyPair) -> Self {
        Self { keypair }
    }

    /// Sign a message payload
    pub fn sign_message(&self, payload: &Value) -> SecurityResult<SignedMessage> {
        // Serialize the payload to canonical JSON
        let payload_bytes = self.serialize_payload(payload)?;

        // Create timestamp
        let timestamp = chrono::Utc::now().timestamp();

        // Create message to sign (payload + timestamp + key_id)
        let mut message_to_sign = payload_bytes.clone();
        message_to_sign.extend_from_slice(&timestamp.to_be_bytes());
        message_to_sign.extend_from_slice(SINGLE_PUBLIC_KEY_ID.as_bytes());

        // Sign the message
        let signature = self.keypair.sign(&message_to_sign);
        let signature_base64 = KeyUtils::signature_to_base64(&signature);

        // Base64 encode the original payload for storage
        let payload_base64 = general_purpose::STANDARD.encode(&payload_bytes);

        Ok(SignedMessage::new(
            payload_base64,
            SINGLE_PUBLIC_KEY_ID.to_string(),
            signature_base64,
            timestamp,
        ))
    }

    /// Serialize payload to canonical JSON bytes
    fn serialize_payload(&self, payload: &Value) -> SecurityResult<Vec<u8>> {
        serde_json::to_vec(payload).map_err(|e| SecurityError::SerializationError(e.to_string()))
    }
}

/// Message verifier for server-side use with optional persistence
pub struct MessageVerifier {
    /// The registered public key (in-memory cache)
    public_key: Arc<RwLock<Option<PublicKeyInfo>>>,
    /// Database operations for persistence
    db_ops: Option<Arc<DbOperations>>,
    /// Maximum allowed timestamp drift in seconds
    max_timestamp_drift: i64,
}

impl MessageVerifier {
    /// Create a new message verifier without persistence
    pub fn new(max_timestamp_drift: i64) -> Self {
        Self {
            public_key: Arc::new(RwLock::new(None)),
            db_ops: None,
            max_timestamp_drift,
        }
    }

    /// Create a new message verifier with database persistence
    pub async fn new_with_persistence(
        max_timestamp_drift: i64,
        db_ops: Arc<DbOperations>,
    ) -> SecurityResult<Self> {
        let verifier = Self {
            public_key: Arc::new(RwLock::new(None)),
            db_ops: Some(db_ops),
            max_timestamp_drift,
        };

        // Load persisted key from database
        verifier.load_persisted_key_async().await?;
        Ok(verifier)
    }

    /// Load the persisted public key from database into memory
    async fn load_persisted_key_async(&self) -> SecurityResult<()> {
        if let Some(db_ops) = &self.db_ops {
            match db_ops.get_system_public_key().await {
                Ok(Some(persisted_key)) => {
                    let mut key_lock = write_lock(&self.public_key)?;
                    *key_lock = Some(persisted_key);
                    tracing::info!(
                        target: "fold_node::permissions",
                        "Loaded system public key from database"
                    );
                }
                Ok(None) => {
                    tracing::info!(
                        target: "fold_node::permissions",
                        "No system public key found in database."
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        target: "fold_node::permissions",
                        "Failed to load persisted public key: {}",
                        e
                    );
                    // Don't fail initialization - continue without persisted key
                }
            }
        }
        Ok(())
    }

    /// Persist a public key to database
    async fn persist_public_key(&self, key_info: &PublicKeyInfo) -> SecurityResult<()> {
        if let Some(db_ops) = &self.db_ops {
            match db_ops.store_system_public_key(key_info).await {
                Ok(()) => {
                    tracing::debug!(
                        target: "fold_node::permissions",
                        "Persisted system public key"
                    );
                    Ok(())
                }
                Err(e) => {
                    tracing::error!(
                        target: "fold_node::permissions",
                        "Failed to persist system public key: {}",
                        e
                    );
                    // Don't fail the operation - key is still in memory
                    Ok(())
                }
            }
        } else {
            Ok(())
        }
    }

    /// Register the system-wide public key with automatic persistence
    pub async fn register_system_public_key(&self, key_info: PublicKeyInfo) -> SecurityResult<()> {
        let mut key_to_store = key_info;
        key_to_store.id = SINGLE_PUBLIC_KEY_ID.to_string();

        // Store in memory first
        {
            let mut key = write_lock(&self.public_key)?;
            *key = Some(key_to_store.clone());
        }

        // Then persist to database
        self.persist_public_key(&key_to_store).await?;

        tracing::info!(
            target: "fold_node::permissions",
            "Registered system public key"
        );
        Ok(())
    }

    /// Remove the system public key from both memory and database
    pub async fn remove_system_public_key(&self) -> SecurityResult<()> {
        // Remove from memory
        {
            let mut key = write_lock(&self.public_key)?;
            *key = None;
        }

        // Remove from database
        if let Some(db_ops) = &self.db_ops {
            match db_ops.delete_system_public_key().await {
                Ok(_) => tracing::debug!(
                    target: "fold_node::permissions",
                    "Removed system public key from database"
                ),
                Err(e) => tracing::error!(
                    target: "fold_node::permissions",
                    "Failed to remove system public key from database: {}",
                    e
                ),
            }
        }

        Ok(())
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

    /// Check permissions and verify message
    pub fn verify_message_with_permissions(
        &self,
        signed_message: &SignedMessage,
        required_permissions: &[String],
    ) -> SecurityResult<VerificationResult> {
        let verification_result = self.verify_message(signed_message)?;
        if !verification_result.is_valid {
            return Ok(verification_result);
        }

        if let Some(key_info) = &verification_result.public_key_info {
            for perm in required_permissions {
                if !key_info.permissions.contains(perm) {
                    return Ok(VerificationResult::failure(format!(
                        "Missing required permission: {perm}"
                    )));
                }
            }
        }

        Ok(verification_result)
    }
}
