//! Explicit account-key proof, independent of storage wrapper policy.

use crate::crypto::at_rest::{open_at_rest_bounded, seal_at_rest_raw, AT_REST_ENC_BINARY_PREFIX};
use crate::storage::{NamespacedStore, StorageError};

pub(crate) use crate::storage::format::CONTENT_KEY_PROOF_KEY as KEY;
pub(crate) use crate::storage::format::METADATA_NAMESPACE as NAMESPACE;
use crate::storage::format::{CONTENT_KEY_PROOF_MAX_BYTES, CONTENT_KEY_PROOF_VALUE as VALUE};

pub(crate) fn create(key: &[u8; 32]) -> Result<Vec<u8>, StorageError> {
    seal_at_rest_raw(key, VALUE)
        .map_err(|error| StorageError::EncryptionError(format!("content key proof: {error}")))
}

pub(crate) fn verify(key: &[u8; 32], stored: &[u8]) -> Result<(), StorageError> {
    // `create` writes only the `ENB:` form of `seal_at_rest_raw`.
    if !stored.starts_with(AT_REST_ENC_BINARY_PREFIX.as_bytes()) {
        return Err(StorageError::EncryptionError(
            "content key proof is not a binary encrypted value".into(),
        ));
    }
    let plain = open_at_rest_bounded(key, stored, CONTENT_KEY_PROOF_MAX_BYTES)
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
