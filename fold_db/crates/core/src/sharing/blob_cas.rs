//! Local content-addressed blob store for slice file bytes and personal
//! file-blob cache.
//!
//! Keys are `blob_ref` strings (`sha256:<hex>`).
//!
//! The live write is [`put_blob_sealed_with_access_dedup_in_ops`]. It stores a
//! sealed row and skips a second copy when an intact row is already there.

use super::delivery_wire::{decode_blob_bytes, decode_blob_bytes_with_dek, ContentAddressedBlob};
use crate::db_operations::DbOperations;
use crate::error::FoldDbError;
use crate::storage::KvStore;
use std::sync::Arc;

mod dedup;
pub use dedup::{put_blob_sealed_with_access_dedup_in_ops, SealedPutOutcome};

const CAS_BLOB_TREE: &str = "cas_blobs";

async fn namespace(ops: &DbOperations) -> Result<Arc<dyn KvStore>, FoldDbError> {
    ops.open_namespace(CAS_BLOB_TREE)
        .await
        .map_err(FoldDbError::from)
}

/// Load a blob record by `blob_ref` from the primary backend, if present.
pub async fn get_blob_in_ops(
    ops: &DbOperations,
    blob_ref: &str,
) -> Result<Option<ContentAddressedBlob>, FoldDbError> {
    let store = namespace(ops).await?;
    match store.get(blob_ref.as_bytes()).await? {
        Some(bytes) => {
            let blob: ContentAddressedBlob = serde_json::from_slice(&bytes)?;
            if blob.blob_ref != blob_ref {
                return Err(FoldDbError::SecurityError(
                    "local CAS record key differs".into(),
                ));
            }
            Ok(Some(blob))
        }
        None => Ok(None),
    }
}

/// Decode raw file bytes for a stored blob_ref.
///
/// Sealed rows fail closed here. Open them with `decode_blob_bytes_with_dek`.
pub async fn get_blob_bytes_in_ops(
    ops: &DbOperations,
    blob_ref: &str,
) -> Result<Option<Vec<u8>>, FoldDbError> {
    match get_blob_in_ops(ops, blob_ref).await? {
        Some(blob) if blob.local_cipher_suite.is_some() => Err(FoldDbError::SecurityError(
            format!("cas_blobs entry {blob_ref} is sealed under file KDK; open with DEK"),
        )),
        Some(blob) => Ok(Some(decode_blob_bytes(&blob)?)),
        None => Ok(None),
    }
}

/// True when a stored record is Trinity-sealed under a file KDK.
#[must_use]
pub fn is_local_cas_sealed(blob: &ContentAddressedBlob) -> bool {
    blob.local_cipher_suite
        .as_deref()
        .is_some_and(crate::sharing::delivery_wire::file_blob_cipher_suite_supported)
}
