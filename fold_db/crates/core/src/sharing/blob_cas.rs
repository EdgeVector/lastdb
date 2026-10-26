//! Local content-addressed blob store for slice file bytes and personal
//! file-blob cache.
//!
//! Keys are `blob_ref` strings (`sha256:<hex>`).
//!
//! ## Operation Trinity (Holy Ghost)
//! Durable local cache must not store plaintext file bytes when a file KDK is
//! available. Prefer [`put_blob_sealed_under_dek_in_ops`]. Plain puts require
//! `LASTDB_ALLOW_PLAIN_CAS=1` for an explicit legacy migration.

use super::delivery_wire::{
    content_addressed_blob_sealed_for_access, content_addressed_blob_sealed_under_dek,
    decode_blob_bytes, decode_blob_bytes_with_dek, validate_local_ciphertext_blob,
    ContentAddressedBlob, FileBlobAccess,
};
use crate::db_operations::DbOperations;
use crate::error::FoldDbError;
use crate::storage::KvStore;
use std::sync::Arc;

mod dedup;
pub use dedup::{
    put_blob_sealed_under_dek_dedup_in_ops, put_blob_sealed_with_access_dedup_in_ops,
    SealedPutOutcome,
};

const CAS_BLOB_TREE: &str = "cas_blobs";

/// When true, plain (unsealed) `cas_blobs` writes are allowed.
/// Product default is **false** (Operation Trinity).
#[must_use]
pub fn plain_cas_writes_allowed() -> bool {
    env_flag::var_truthy("LASTDB_ALLOW_PLAIN_CAS")
}

async fn namespace(ops: &DbOperations) -> Result<Arc<dyn KvStore>, FoldDbError> {
    ops.open_namespace(CAS_BLOB_TREE)
        .await
        .map_err(FoldDbError::from)
}

/// Persist a blob under its `blob_ref`.
///
/// - If `access.dek` is present → sealed under file KDK (Trinity path).
/// - Else if `LASTDB_ALLOW_PLAIN_CAS=1` → legacy plain store.
/// - Else → refuse (no durable plaintext without DEK).
pub async fn put_blob_in_ops(
    ops: &DbOperations,
    blob: &ContentAddressedBlob,
) -> Result<(), FoldDbError> {
    if let Some(access) = blob.access.as_ref() {
        if !access.dek.is_empty() {
            let plain = if blob.local_cipher_suite.is_some() {
                decode_blob_bytes_with_dek(blob, &access.dek)?
            } else {
                decode_blob_bytes(blob)?
            };
            return put_blob_sealed_for_access_in_ops(
                ops,
                &plain,
                blob.media_type.clone(),
                blob.name.clone(),
                access,
            )
            .await
            .map(|_| ());
        }
    }
    if blob.local_cipher_suite.is_some() {
        return put_raw_record(ops, blob).await;
    }
    if plain_cas_writes_allowed() {
        let _ = decode_blob_bytes(blob)?;
        return put_raw_record(ops, blob).await;
    }
    Err(FoldDbError::SecurityError(
        "refusing plain cas_blobs write (Operation Trinity); provide file KDK or set LASTDB_ALLOW_PLAIN_CAS=1"
            .into(),
    ))
}

/// Store a pointer's exact versioned identity without placing its key on the row.
pub async fn put_blob_sealed_for_access_in_ops(
    ops: &DbOperations,
    plaintext: &[u8],
    media_type: Option<String>,
    name: Option<String>,
    access: &FileBlobAccess,
) -> Result<ContentAddressedBlob, FoldDbError> {
    let sealed = content_addressed_blob_sealed_for_access(plaintext, media_type, name, access)?;
    put_raw_record(ops, &sealed).await?;
    Ok(sealed)
}

/// Seal plaintext under the file KDK and store in `cas_blobs` (no DEK on disk).
pub async fn put_blob_sealed_under_dek_in_ops(
    ops: &DbOperations,
    plaintext: &[u8],
    media_type: Option<String>,
    name: Option<String>,
    dek_hex: &str,
) -> Result<ContentAddressedBlob, FoldDbError> {
    let sealed = content_addressed_blob_sealed_under_dek(plaintext, media_type, name, dek_hex)?;
    put_raw_record(ops, &sealed).await?;
    Ok(sealed)
}

async fn put_raw_record(
    ops: &DbOperations,
    blob: &ContentAddressedBlob,
) -> Result<(), FoldDbError> {
    let runtime = ops.namespaced_store().disk_pack_runtime();
    // Native gate first, then the blob gate: the dwell pass uses the same order.
    // The native write view includes earlier accepted changes even while they
    // still await disk flush.
    crate::storage::disk_pack::transaction::background_scope(runtime, async {
        let _target_gate = ops
            .atoms()
            .lock_liveness_blobs(std::slice::from_ref(&blob.blob_ref))
            .await;
        let store = namespace(ops).await?;
        write_row_under_gate(&store, blob).await?;
        // The same native request holds the row and its counter.
        ops.atoms()
            .register_blob_upload(&blob.blob_ref)
            .await
            .map_err(FoldDbError::Schema)
    })
    .await
}

/// Write one row. The caller holds the blob's liveness target gate.
async fn write_row_under_gate(
    store: &Arc<dyn KvStore>,
    blob: &ContentAddressedBlob,
) -> Result<(), FoldDbError> {
    validate_local_ciphertext_blob(blob)?;
    // Stamp `stored_at` at the durable-write chokepoint so every writer path
    // (personal cache, delivery import, fork) dates its row. The stamp is
    // information only. The blob counter decides when a row may go: the put
    // registers the upload, and the dwell keeps the row until the atom that
    // names it arrives.
    let value = if blob.stored_at.is_some() {
        serde_json::to_vec(blob)?
    } else {
        let mut stamped = blob.clone();
        stamped.stored_at = Some(chrono::Utc::now().to_rfc3339());
        serde_json::to_vec(&stamped)?
    };
    store.put(blob.blob_ref.as_bytes(), value).await?;
    Ok(())
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
/// Sealed Trinity rows fail closed without a DEK — use
/// [`get_blob_bytes_with_dek_in_ops`].
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

/// Open a sealed (or legacy plain) local CAS blob under the file KDK.
pub async fn get_blob_bytes_with_dek_in_ops(
    ops: &DbOperations,
    blob_ref: &str,
    dek_hex: &str,
) -> Result<Option<Vec<u8>>, FoldDbError> {
    match get_blob_in_ops(ops, blob_ref).await? {
        Some(blob) => Ok(Some(decode_blob_bytes_with_dek(&blob, dek_hex)?)),
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
