//! The `$lastdb_file` pointer shape — the core codec for "this record's value
//! is a reference to a content-addressed file blob".
//!
//! Lives in `record` (not `sharing`) because the pointer is part of the core
//! data model: the resident ladder tracks `file_blob_ref` per record, and the
//! purge/GC paths must recognize pointers on every build, sharing feature or
//! not. `sharing::delivery_wire` re-exports these names for its existing
//! callers and layers access/thumbnail metadata handling on top.

use std::collections::HashMap;

use serde_json::Value;

/// JSON object key for a portable file pointer stored as a record value.
pub const LASTDB_FILE_KEY: &str = "$lastdb_file";

/// Account-derived file encryption addressed by SHA-256 of its binary ciphertext.
/// The plaintext digest and derived key stay inside the encrypted record pointer.
/// This is the only file-blob cipher suite. `sharing::delivery_wire` re-exports it.
pub const FILE_BLOB_CIPHER_SUITE: &str =
    "lastdb-file-blob-account-v2:aes-256-gcm-ciphertext-sha256";

/// If `value` is a `$lastdb_file` pointer, return its blob_ref when present.
pub fn blob_ref_from_value(value: &Value) -> Option<&str> {
    value
        .get(LASTDB_FILE_KEY)
        .and_then(|v| v.as_object())
        .and_then(|obj| obj.get("blob_ref"))
        .and_then(|v| v.as_str())
}

/// Every blob ref a record names — content pointer plus the `file_blob_ref`
/// metadata entry.
///
/// This union is the reachability definition shared by purge accounting and
/// the blob counter: the resident persist path records refs in metadata, the
/// personal/sharing write paths record them in content, and a count that reads
/// only one of the two lets the dwell pass delete blobs the other still
/// needs.
pub fn blob_refs_of_value(
    content: &Value,
    metadata: Option<&HashMap<String, String>>,
) -> Vec<String> {
    let mut refs = Vec::new();
    if let Some(r) = blob_ref_from_value(content) {
        refs.push(r.to_string());
    }
    if let Some(r) = content
        .get(LASTDB_FILE_KEY)
        .and_then(|v| v.get("thumbnail"))
        .and_then(|v| v.get("blob_ref"))
        .and_then(Value::as_str)
    {
        if !refs.iter().any(|existing| existing == r) {
            refs.push(r.to_string());
        }
    }
    if let Some(meta) = metadata {
        if let Some(r) = meta.get("file_blob_ref") {
            if !refs.iter().any(|existing| existing == r) {
                refs.push(r.clone());
            }
        }
    }
    refs
}
