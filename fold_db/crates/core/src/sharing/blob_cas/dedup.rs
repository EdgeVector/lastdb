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
//! - **An intact row younger than [`DUPLICATE_PUT_REFRESH_SECS`]:** write
//!   nothing. No second copy.
//! - **An intact row older than that, or one with no usable `stored_at`:**
//!   write the same row back with a new `stored_at`. No second seal.
//!
//! "Intact" covers both row formats. A sealed row must open under the DEK. A
//! legacy plain row (written by the photo migration, which has DEK-less
//! readers) must hash to its key. A plain row stays plain: sealing it here would
//! make `get_blob_bytes_in_ops` refuse it.
//!
//! # Why the freshness check stays
//!
//! The GC age gate (`gc-file-blobs`) reclaims an unreferenced row only once
//! `stored_at` is older than `FRESH_WRITE_GRACE_SECS` (600 s). The caller puts
//! the blob first and writes the pointer record second, so `stored_at` is what
//! keeps a sweep from reaping a row in that gap. A row that already exists and
//! is, say, 590 s old (an orphan of an earlier crashed push) must NOT be skipped:
//! the sweep could reap it before this caller's pointer lands. Skipping only
//! rows younger than half the gate bounds the age after a put to 300 s. The
//! caller then has at least 300 s, and a renewed row has the full 600 s.
//!
//! The cost of the rule: a re-put of the same bytes more than 300 s after the
//! last write rewrites the row, so the log holds one more copy of it. That is
//! bounded by the re-put rate, not by the number of puts. No compaction runs
//! automatically on `cas_blobs`; a dead copy stays on disk until an owner
//! compacts that collection.
//!
//! # Memory
//!
//! The bytes are copied as little as the formats allow. The sealed row is built
//! only when a write is needed, it is stamped in place (no clone), it is
//! serialized into one buffer sized up front, and it is dropped before the store
//! write. The store layers below still copy the value (seal, base64, group
//! buffer); that cost is why one local blob is bounded (see
//! `LOCAL_FILE_BLOB_MAX_BYTES_DEFAULT`).

use super::{decode_blob_bytes_with_dek, is_local_cas_sealed, namespace, ContentAddressedBlob};
use crate::db_operations::file_blob_gc::FRESH_WRITE_GRACE_SECS;
use crate::db_operations::DbOperations;
use crate::error::FoldDbError;
use crate::hex::sha256_hex;
use crate::schema::SchemaError;
use crate::sharing::delivery_wire::content_addressed_blob_sealed_under_dek;
use crate::storage::{KvStore, StorageError};
use chrono::{DateTime, Utc};
use std::sync::Arc;

/// A duplicate put skips the write when the row is younger than this. Half of
/// the GC grace window, so the age after any put is at most this value.
pub const DUPLICATE_PUT_REFRESH_SECS: i64 = FRESH_WRITE_GRACE_SECS / 2;

const _: () = assert!(
    DUPLICATE_PUT_REFRESH_SECS > 0 && DUPLICATE_PUT_REFRESH_SECS < FRESH_WRITE_GRACE_SECS,
    "a skipped duplicate put must leave the row inside the GC grace window"
);

/// What a local put did to the `cas_blobs` plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealedPutOutcome {
    /// No usable row existed. This call wrote one.
    Stored,
    /// An intact row existed and was young. This call wrote nothing.
    AlreadyPresent,
    /// An intact row existed but was older than [`DUPLICATE_PUT_REFRESH_SECS`].
    /// This call wrote it back with a new `stored_at`.
    Refreshed,
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
    Intact {
        row: Box<ContentAddressedBlob>,
        age: chrono::Duration,
    },
}

/// Seal `plaintext` under `dek_hex` and store it unless an intact row for the
/// same bytes is already there and young. See the module docs.
pub async fn put_blob_sealed_under_dek_dedup_in_ops(
    ops: &DbOperations,
    plaintext: &[u8],
    media_type: Option<String>,
    name: Option<String>,
    dek_hex: &str,
) -> Result<SealedPutOutcome, FoldDbError> {
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
    match inspect_existing(&store, &wanted).await {
        Existing::Intact { row, age } => {
            if age < chrono::Duration::seconds(DUPLICATE_PUT_REFRESH_SECS) {
                return Ok(SealedPutOutcome::AlreadyPresent);
            }
            // Same row, same format, new stamp. No second seal.
            let mut renewed = row;
            renewed.stored_at = Some(Utc::now().to_rfc3339());
            let value = encode_row(&renewed)?;
            drop(renewed);
            put_row(&store, &blob_ref, value).await?;
            Ok(SealedPutOutcome::Refreshed)
        }
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
            Ok(SealedPutOutcome::Stored)
        }
    }
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
    let age = row
        .stored_at
        .as_deref()
        .and_then(|at| DateTime::parse_from_rfc3339(at).ok())
        .map(|at| Utc::now().signed_duration_since(at.with_timezone(&Utc)))
        .filter(|age| *age >= chrono::Duration::zero());
    // No usable stamp (none, unparseable, or in the future) is not evidence of
    // a young write: treat the row as due for renewal.
    Existing::Intact {
        age: age.unwrap_or_else(|| chrono::Duration::seconds(DUPLICATE_PUT_REFRESH_SECS)),
        row: Box::new(row),
    }
}
