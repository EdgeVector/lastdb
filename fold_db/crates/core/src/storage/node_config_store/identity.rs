use serde::{Deserialize, Serialize};

use super::NodeConfigStore;
use crate::storage::error::StorageError;

pub(super) fn is_missing_identity_key_error(error: &StorageError) -> bool {
    error.to_string().contains("no decryption key configured")
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeIdentity {
    pub private_key: String,
    pub public_key: String,
}

impl NodeConfigStore {
    // --- Node identity ---
    //
    // `identity:private_key` is encrypted at rest with AES-256-GCM when a
    // crypto key is configured on this store handle. The public key is
    // stored in plaintext: there is no confidentiality requirement and
    // callers (e.g. discovery_config) sometimes need to read it without
    // holding the encryption key.

    pub fn get_identity(&self) -> Result<Option<NodeIdentity>, StorageError> {
        let Some(public_key) = self.get("identity:public_key")? else {
            return Ok(None);
        };
        let Some(stored_private) = self.get("identity:private_key")? else {
            return Ok(None);
        };
        let private_key = self.decrypt_sensitive(stored_private).map_err(|e| {
            if is_missing_identity_key_error(&e) {
                tracing::info!(
                    "encrypted node identity is present but this config handle has no decryption key;                      persisted identity will not be applied until the node is opened with its master key"
                );
            } else {
                tracing::error!("failed to load encrypted node identity: {}", e);
            }
            e
        })?;
        Ok(Some(NodeIdentity {
            private_key,
            public_key,
        }))
    }

    fn set_identity_with_hook<F>(
        &self,
        id: &NodeIdentity,
        before_public_key: F,
    ) -> Result<(), StorageError>
    where
        F: FnOnce() -> Result<(), StorageError>,
    {
        let encrypted_private = self.encrypt_sensitive(&id.private_key)?;
        before_public_key()?;
        self.set_many(vec![
            (
                b"identity:private_key".to_vec(),
                encrypted_private.into_bytes(),
            ),
            (
                b"identity:public_key".to_vec(),
                id.public_key.as_bytes().to_vec(),
            ),
        ])
    }

    pub fn set_identity(&self, id: &NodeIdentity) -> Result<(), StorageError> {
        self.set_identity_with_hook(id, || Ok(()))
    }

    /// Read the raw stored value for `identity:private_key` without
    /// decryption. Exposed for tests that need to verify the on-disk
    /// representation is ciphertext.
    #[doc(hidden)]
    pub fn raw_identity_private_key(&self) -> Result<Option<String>, StorageError> {
        self.get("identity:private_key")
    }
}
