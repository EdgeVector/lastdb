//! Explicit account-key proof, independent of storage wrapper policy.

use super::at_rest::{open_at_rest_bounded, seal_at_rest_raw};
use crate::storage::{NamespacedStore, StorageError};

pub(crate) const NAMESPACE: &str = "metadata";
pub(crate) const KEY: &[u8] = b"__content_key_proof_v1";
const VALUE: &[u8] = b"lastdb-content-key-proof-v1";

pub(crate) fn create(key: &[u8; 32]) -> Result<Vec<u8>, StorageError> {
    seal_at_rest_raw(key, VALUE)
        .map_err(|error| StorageError::EncryptionError(format!("content key proof: {error}")))
}

pub(crate) fn verify(key: &[u8; 32], stored: &[u8]) -> Result<(), StorageError> {
    let plain = open_at_rest_bounded(key, stored, 4096)
        .map_err(|error| StorageError::EncryptionError(format!("content key proof: {error}")))?;
    if plain != VALUE {
        return Err(StorageError::EncryptionError(
            "content key proof differs; refusing to replace it".into(),
        ));
    }
    Ok(())
}

/// A wrong key never replaces an existing proof or any source record.
/// Every keyed constructor checks this one record after its format fence.
pub(crate) async fn verify_or_establish(
    store: &dyn NamespacedStore,
    key: &[u8; 32],
    fresh: bool,
) -> Result<(), StorageError> {
    let kv = store.open_namespace(NAMESPACE).await?;
    if let Some(stored) = kv.get(KEY).await? {
        return verify(key, &stored);
    }
    if !fresh {
        return Err(StorageError::EncryptionError(
            "content key proof is missing; refusing to replace it".into(),
        ));
    }
    kv.put(KEY, create(key)?).await
}
