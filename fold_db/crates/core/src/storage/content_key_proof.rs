//! Explicit account-key proof, independent of storage wrapper policy.

use crate::crypto::at_rest::{open_at_rest_bounded, seal_at_rest_raw};
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

/// Home creation writes this record. Every keyed constructor checks it after
/// its format fence, and a missing or different proof is refused.
pub(crate) async fn verify_stored(
    store: &dyn NamespacedStore,
    key: &[u8; 32],
) -> Result<(), StorageError> {
    let kv = store.open_namespace(NAMESPACE).await?;
    match kv.get(KEY).await? {
        Some(stored) => verify(key, &stored),
        None => Err(StorageError::EncryptionError(
            "content key proof is missing; this program opens only a home it created".into(),
        )),
    }
}
