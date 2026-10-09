//! Thin delegator methods on `DbOperations` for public-key persistence.
//! Real implementations live on [`super::public_key_store::PublicKeyStore`].

use super::core::DbOperations;
use crate::schema::SchemaError;
use crate::security::PublicKeyInfo;

impl DbOperations {
    /// Gets the system-wide public key
    pub async fn get_system_public_key(&self) -> Result<Option<PublicKeyInfo>, SchemaError> {
        self.public_keys().get_system_public_key().await
    }

    /// Stores the system-wide public key
    pub async fn store_system_public_key(
        &self,
        key_info: &PublicKeyInfo,
    ) -> Result<(), SchemaError> {
        self.public_keys().store_system_public_key(key_info).await
    }

    /// Deletes the system-wide public key
    pub async fn delete_system_public_key(&self) -> Result<bool, SchemaError> {
        self.public_keys().delete_system_public_key().await
    }
}
