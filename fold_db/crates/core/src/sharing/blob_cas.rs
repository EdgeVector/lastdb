//! Local content-addressed blob store for slice file bytes and personal
//! file-blob cache.
//!
//! Used on import so a recipient can resolve `$lastdb_file.blob_ref` after a
//! delivery slice lands. Keys are `blob_ref` strings (`sha256:<hex>`).
//!
//! ## Operation Trinity (Holy Ghost)
//! Durable local cache must not store plaintext file bytes when a file KDK is
//! available. Prefer [`put_blob_sealed_under_dek_in_ops`]. Plain puts require
//! `LASTDB_ALLOW_PLAIN_CAS=1` (tests / migrate only).

use super::delivery_wire::{
    content_addressed_blob_sealed_under_dek, decode_blob_bytes, decode_blob_bytes_with_dek,
    ContentAddressedBlob,
};
use crate::db_operations::DbOperations;
use crate::error::FoldDbError;
use crate::storage::KvStore;
use std::sync::Arc;

mod dedup;
pub use dedup::{
    put_blob_sealed_under_dek_dedup_in_ops, SealedPutOutcome, DUPLICATE_PUT_REFRESH_SECS,
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
                return put_raw_record(ops, blob).await;
            } else {
                decode_blob_bytes(blob)?
            };
            return put_blob_sealed_under_dek_in_ops(
                ops,
                &plain,
                blob.media_type.clone(),
                blob.name.clone(),
                &access.dek,
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
    let _target_gate = ops
        .atoms()
        .lock_liveness_blobs(std::slice::from_ref(&blob.blob_ref))
        .await;
    let store = namespace(ops).await?;
    write_row_under_gate(&store, blob).await
}

/// Write one row. The caller holds the blob's liveness target gate.
async fn write_row_under_gate(
    store: &Arc<dyn KvStore>,
    blob: &ContentAddressedBlob,
) -> Result<(), FoldDbError> {
    // Stamp `stored_at` at the durable-write chokepoint so every writer path
    // (personal cache, delivery import, fork) dates its row. The GC age gate
    // (`gc_orphan_file_blobs`) refuses to reclaim an undated or fresh row, so
    // a re-put of existing content also refreshes the row into the protected
    // window — that is the property that makes "blob row put, then pointer
    // atom write" safe against a sweep running in between.
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

/// Hard-delete one blob row by `blob_ref`. Returns whether a row was present.
///
/// Destructive: callers own the reachability argument. The only production
/// caller is `db_operations::file_blob_gc::gc_orphan_file_blobs`, which proves
/// no live atom references the blob before calling this.
pub async fn delete_blob_in_ops(ops: &DbOperations, blob_ref: &str) -> Result<bool, FoldDbError> {
    let store = namespace(ops).await?;
    Ok(store.delete(blob_ref.as_bytes()).await?)
}

/// One `cas_blobs` row header for the GC sweep: `(blob_ref, value bytes,
/// stored_at)`. Values are not decoded beyond the envelope fields.
pub(crate) struct BlobRowHeader {
    pub blob_ref: String,
    pub approx_bytes: u64,
    pub stored_at: Option<String>,
}

/// Result of [`scan_blob_row_headers`]: the rows it could read, and the
/// `blob_ref`s of rows whose at-rest value it could not open.
pub(crate) struct BlobRowScan {
    pub headers: Vec<BlobRowHeader>,
    /// Rows whose value failed to decrypt / inflate (e.g. a row sealed over
    /// `LASTDB_AT_REST_MAX_INFLATED_BYTES`). The caller must RETAIN them: an
    /// unreadable row cannot be aged, so it is never a delete candidate.
    pub unreadable: Vec<String>,
}

/// Scan every `cas_blobs` row header. GC-sweep only: this is a full namespace
/// scan, the admin-verb exception to the no-scan contract (same standing as
/// `gc_orphan_atoms_with`'s `atom:` scan).
///
/// Keys are enumerated without opening values (keys are plaintext at the
/// encryption seam), then each row is read on its own. One row that fails to
/// open is reported in [`BlobRowScan::unreadable`] instead of aborting the
/// whole scan: a single over-ceiling row used to fail every `gc-file-blobs`
/// pass with HTTP 500 before it could report anything. Any other storage
/// error still fails the scan.
pub(crate) async fn scan_blob_row_headers(ops: &DbOperations) -> Result<BlobRowScan, FoldDbError> {
    let store = namespace(ops).await?;
    let keys = store.scan_prefix_keys(b"").await?;
    let mut out = BlobRowScan {
        headers: Vec::with_capacity(keys.len()),
        unreadable: Vec::new(),
    };
    for key in keys {
        let blob_ref = String::from_utf8_lossy(&key).into_owned();
        let value = match store.get(&key).await {
            Ok(Some(value)) => value,
            // Deleted since the key scan, or an un-enveloped row that reads
            // as absent: not a row, same as the old value scan.
            Ok(None) => continue,
            Err(crate::storage::StorageError::EncryptionError(error)) => {
                tracing::warn!(
                    blob_ref = %blob_ref,
                    error = %error,
                    "gc-file-blobs: cas_blobs row cannot be opened; retaining it"
                );
                out.unreadable.push(blob_ref);
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        let stored_at = serde_json::from_slice::<serde_json::Value>(&value)
            .ok()
            .and_then(|v| {
                v.get("stored_at")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            });
        out.headers.push(BlobRowHeader {
            blob_ref,
            approx_bytes: (key.len() + value.len()) as u64,
            stored_at,
        });
    }
    Ok(out)
}

/// Stamp `stored_at` on an existing row that lacks one, leaving every other
/// byte of the row unchanged. Used by the GC sweep to date the pre-`stored_at`
/// population on first observation instead of deleting rows it cannot age.
pub(crate) async fn stamp_blob_stored_at(
    ops: &DbOperations,
    blob_ref: &str,
    stored_at: &str,
) -> Result<(), FoldDbError> {
    let store = namespace(ops).await?;
    let Some(bytes) = store.get(blob_ref.as_bytes()).await? else {
        return Ok(());
    };
    let mut value: serde_json::Value = serde_json::from_slice(&bytes)?;
    let Some(obj) = value.as_object_mut() else {
        return Ok(());
    };
    if obj.get("stored_at").is_some_and(|v| !v.is_null()) {
        return Ok(());
    }
    obj.insert(
        "stored_at".to_string(),
        serde_json::Value::String(stored_at.to_string()),
    );
    store
        .put(blob_ref.as_bytes(), serde_json::to_vec(&value)?)
        .await?;
    Ok(())
}

/// Load a blob record by `blob_ref` from the primary backend, if present.
pub async fn get_blob_in_ops(
    ops: &DbOperations,
    blob_ref: &str,
) -> Result<Option<ContentAddressedBlob>, FoldDbError> {
    let store = namespace(ops).await?;
    match store.get(blob_ref.as_bytes()).await? {
        Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
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

/// Store every blob from a delivery payload.
///
/// Blobs that carry `access.dek` are sealed under that KDK. Others require
/// `LASTDB_ALLOW_PLAIN_CAS=1` or already-sealed `local_cipher_suite`.
pub async fn put_blobs_in_ops(
    ops: &DbOperations,
    blobs: &[ContentAddressedBlob],
) -> Result<usize, FoldDbError> {
    for blob in blobs {
        put_blob_in_ops(ops, blob).await?;
    }
    Ok(blobs.len())
}

/// True when a stored record is Trinity-sealed under a file KDK.
#[must_use]
pub fn is_local_cas_sealed(blob: &ContentAddressedBlob) -> bool {
    blob.local_cipher_suite
        .as_deref()
        .is_some_and(crate::sharing::delivery_wire::file_blob_cipher_suite_supported)
}
