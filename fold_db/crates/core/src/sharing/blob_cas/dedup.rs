//! Duplicate-put handling for local CAS rows.
//!
//! A `cas_blobs` write is an append to a log-structured store, and nothing
//! compacts the plane on its own. A second [`super::put_blob_sealed_under_dek_in_ops`]
//! of the same bytes therefore leaves a second full copy on disk. The local blob
//! PUT is retried by design (a push that crashed before its pointer batch runs
//! again with the same bytes), so it uses
//! [`put_blob_sealed_under_dek_dedup_in_ops`] instead.
//!
//! # The design
//!
//! Hash the bytes, take the blob's liveness target gate, and read the row that
//! is already there BEFORE sealing anything:
//!
//! - **No row, or a row that does not open and hash to its key:** seal and
//!   write. A damaged row is repaired by the next put of its bytes.
//! - **An intact row:** write no second copy.
//!
//! "Intact" covers both row formats. A sealed row must open under the DEK. A
//! legacy plain row (written by the photo migration, which has DEK-less
//! readers) must hash to its key. A plain row stays plain: sealing it here would
//! make `get_blob_bytes_in_ops` refuse it.
//!
//! In every case the put also registers the upload with the blob counter, in the
//! same native write request as the row. A blob that no atom names yet starts
//! its dwell, and a blob that is already in its dwell starts it again. The caller
//! puts the blob first and writes the pointer record second, so the dwell is what
//! keeps the blob in that gap. The age of the row decides nothing.
//!
//! # Memory
//!
//! The bytes are copied as little as the formats allow. The sealed row is built
//! only when a write is needed, it is stamped in place (no clone), it is
//! serialized into one buffer sized up front, and it is dropped before the store
//! write. Sealing and serialization still use file-sized buffers. Native
//! pending-memory and disk-space admission account for the stored row.

use super::{decode_blob_bytes_with_dek, is_local_cas_sealed, namespace, ContentAddressedBlob};
use crate::db_operations::DbOperations;
use crate::error::FoldDbError;
use crate::hex::sha256_hex;
use crate::schema::SchemaError;
use crate::sharing::delivery_wire::content_addressed_blob_sealed_under_dek;
use crate::storage::{KvStore, StorageError};
use chrono::Utc;
use std::sync::Arc;

/// What a local put did to the `cas_blobs` plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealedPutOutcome {
    /// No usable row existed. This call wrote one.
    Stored,
    /// An intact row existed. This call wrote no second copy.
    AlreadyPresent,
}

impl SealedPutOutcome {
    /// True when an intact row for these bytes existed before the call.
    #[must_use]
    pub fn existed(self) -> bool {
        !matches!(self, Self::Stored)
    }
}

/// What the plane held for one `blob_ref` before a put.
enum Existing {
    Absent,
    /// A row is there but it does not open, or it does not match its key.
    Unusable,
    /// A row that opens (sealed) or hashes (plain) to its key.
    Intact,
}

/// Seal `plaintext` under `dek_hex` and store it unless an intact row for the
/// same bytes is already there. Register the upload with the blob counter. See
/// the module docs.
pub async fn put_blob_sealed_under_dek_dedup_in_ops(
    ops: &DbOperations,
    plaintext: &[u8],
    media_type: Option<String>,
    name: Option<String>,
    dek_hex: &str,
) -> Result<SealedPutOutcome, FoldDbError> {
    let runtime = ops.namespaced_store().disk_pack_runtime();
    // Take the native gate before the blob lock. Its private read view sees
    // earlier accepted rows, so a pending put cannot look absent or stale.
    crate::storage::disk_pack::transaction::background_scope(runtime, async {
        let content_sha256 = sha256_hex(plaintext);
        let blob_ref = format!("sha256:{content_sha256}");
        let _target_gate = ops
            .atoms()
            .lock_liveness_blobs(std::slice::from_ref(&blob_ref))
            .await;
        let store = namespace(ops).await?;
        let wanted = Wanted {
            blob_ref: &blob_ref,
            content_sha256: &content_sha256,
            plain_len: plaintext.len(),
            dek_hex,
        };
        let outcome = match inspect_existing(&store, &wanted).await {
            Existing::Intact => SealedPutOutcome::AlreadyPresent,
            Existing::Absent | Existing::Unusable => {
                let mut sealed =
                    content_addressed_blob_sealed_under_dek(plaintext, media_type, name, dek_hex)?;
                if sealed.blob_ref != blob_ref {
                    return Err(FoldDbError::SecurityError(format!(
                        "sealed blob_ref {} does not match the gate target {blob_ref}",
                        sealed.blob_ref
                    )));
                }
                // Stamp here, on the value we own. The shared row writer would
                // clone the whole sealed string to stamp it.
                sealed.stored_at = Some(Utc::now().to_rfc3339());
                let value = encode_row(&sealed)?;
                drop(sealed);
                put_row(&store, &blob_ref, value).await?;
                SealedPutOutcome::Stored
            }
        };
        // The same native request holds the row and its counter.
        ops.atoms()
            .register_blob_upload(&blob_ref)
            .await
            .map_err(FoldDbError::Schema)?;
        Ok(outcome)
    })
    .await
}

/// Serialize a row into one buffer sized up front. The sealed text is the whole
/// row for a large blob, and `to_vec` would grow its buffer by doubling, which
/// copies that text several times at once.
fn encode_row(row: &ContentAddressedBlob) -> Result<Vec<u8>, FoldDbError> {
    let mut out = Vec::with_capacity(row.bytes_b64.len() + 1024);
    serde_json::to_writer(&mut out, row)?;
    Ok(out)
}

/// Write one serialized row. The caller holds the blob's liveness target gate.
///
/// A full disk and capture backpressure keep their identity across this
/// boundary, so the node answers 507 and 503 and not an opaque 500.
async fn put_row(
    store: &Arc<dyn KvStore>,
    blob_ref: &str,
    value: Vec<u8>,
) -> Result<(), FoldDbError> {
    store
        .put(blob_ref.as_bytes(), value)
        .await
        .map_err(storage_error)
}

fn storage_error(error: StorageError) -> FoldDbError {
    let keeps_identity = error.is_storage_full()
        || error.is_capture_queue_full()
        || matches!(&error, StorageError::IoError(io) if StorageError::is_storage_full_io(io));
    if keeps_identity {
        FoldDbError::Schema(SchemaError::from(error))
    } else {
        FoldDbError::from(error)
    }
}

/// The bytes a put wants the row to hold.
struct Wanted<'a> {
    blob_ref: &'a str,
    content_sha256: &'a str,
    plain_len: usize,
    dek_hex: &'a str,
}

async fn inspect_existing(store: &Arc<dyn KvStore>, wanted: &Wanted<'_>) -> Existing {
    // A read error is not proof the row is gone, but a put is idempotent and
    // the write that follows repairs the row, so treat it as unusable.
    let Ok(found) = store.get(wanted.blob_ref.as_bytes()).await else {
        return Existing::Unusable;
    };
    let Some(bytes) = found else {
        return Existing::Absent;
    };
    let Ok(row) = serde_json::from_slice::<ContentAddressedBlob>(&bytes) else {
        return Existing::Unusable;
    };
    drop(bytes);
    // A plain row has no cipher suite. A row with a suite this binary does not
    // know cannot be opened, so it does not count.
    let format_known = row.local_cipher_suite.is_none() || is_local_cas_sealed(&row);
    let same_blob = format_known
        && row.blob_ref == wanted.blob_ref
        && row.content_sha256 == wanted.content_sha256
        && row.size == wanted.plain_len as u64;
    // Opening checks the AEAD tag (sealed rows) and the plaintext hash against
    // the row, so a row that passes holds exactly these bytes.
    let opens = same_blob
        && decode_blob_bytes_with_dek(&row, wanted.dek_hex)
            .is_ok_and(|raw| raw.len() == wanted.plain_len);
    if !opens {
        return Existing::Unusable;
    }
    Existing::Intact
}
