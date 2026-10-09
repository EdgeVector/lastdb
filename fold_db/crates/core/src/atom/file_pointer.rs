//! The `$lastdb_file` pointer shape — the core codec for "this atom's value
//! is a reference to a content-addressed file blob".
//!
//! Lives in `atom` (not `sharing`) because the pointer is part of the core
//! data model: the resident ladder tracks `file_blob_ref` per atom, and the
//! purge/GC paths must recognize pointers on every build, sharing feature or
//! not. `sharing::delivery_wire` re-exports these names for its existing
//! callers and layers access/thumbnail metadata handling on top.

use std::collections::HashMap;

use serde_json::Value;

/// JSON object key for a portable file pointer stored as an atom value.
pub const LASTDB_FILE_KEY: &str = "$lastdb_file";

/// If `value` is a file pointer or legacy embedded file, return its blob_ref
/// when present (legacy `$file` has no blob_ref until re-packed).
pub fn blob_ref_from_atom_value(value: &Value) -> Option<&str> {
    if let Some(obj) = value.get(LASTDB_FILE_KEY).and_then(|v| v.as_object()) {
        return obj.get("blob_ref").and_then(|v| v.as_str());
    }
    if let Some(s) = value.get("$blob_ref").and_then(|v| v.as_str()) {
        return Some(s);
    }
    None
}

/// Every blob ref an atom names — content pointer plus metadata
/// (`file_blob_ref` verbatim; `file_hash` as `sha256:{hash}`).
///
/// This union is the reachability definition shared by purge accounting and
/// `gc_orphan_file_blobs`: the resident persist path records refs in
/// metadata, the personal/sharing write paths record them in content, and a
/// keep-set that reads only one of the two deletes blobs the other still
/// needs.
pub fn blob_refs_of_atom(
    content: &Value,
    metadata: Option<&HashMap<String, String>>,
) -> Vec<String> {
    let mut refs = Vec::new();
    if let Some(r) = blob_ref_from_atom_value(content) {
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
        if let Some(h) = meta.get("file_hash") {
            let r = format!("sha256:{h}");
            if !refs.iter().any(|existing| existing == &r) {
                refs.push(r);
            }
        }
    }
    refs
}

/// Logical bytes of every unique blob reference named by an atom.
///
/// `None` means the atom names a blob but does not carry its logical size.
/// Storage accounting must then fail closed instead of reporting a false
/// exact zero. No blob reference means an exact `Some(0)`.
pub fn blob_logical_bytes_of_atom(
    content: &Value,
    metadata: Option<&HashMap<String, String>>,
) -> Option<u64> {
    let refs = blob_refs_of_atom(content, metadata);
    if refs.is_empty() {
        return Some(0);
    }
    let pointer = content.get(LASTDB_FILE_KEY).and_then(Value::as_object);
    let main_ref = pointer
        .and_then(|obj| obj.get("blob_ref"))
        .and_then(Value::as_str);
    let main_size = pointer
        .and_then(|obj| obj.get("encrypted_size_bytes"))
        .and_then(Value::as_u64);
    let thumbnail = pointer.and_then(|obj| obj.get("thumbnail"));
    let thumbnail_ref = thumbnail
        .and_then(|value| value.get("blob_ref"))
        .and_then(Value::as_str);
    let thumbnail_size = thumbnail
        .and_then(|value| value.pointer("/access/encrypted_size_bytes"))
        .and_then(Value::as_u64);
    let metadata_ref = metadata.and_then(|meta| meta.get("file_blob_ref").map(String::as_str));
    let metadata_hash_ref = metadata
        .and_then(|meta| meta.get("file_hash"))
        .map(|hash| format!("sha256:{hash}"));
    let metadata_size = metadata.and_then(|meta| {
        meta.get("encrypted_size_bytes")
            .or_else(|| meta.get("file_size_bytes"))
            .and_then(|value| value.parse::<u64>().ok())
    });

    let mut total = 0u64;
    for blob_ref in refs {
        let size = if main_ref == Some(blob_ref.as_str()) {
            main_size
        } else if thumbnail_ref == Some(blob_ref.as_str()) {
            thumbnail_size
        } else if metadata_ref == Some(blob_ref.as_str())
            || metadata_hash_ref.as_deref() == Some(blob_ref.as_str())
        {
            metadata_size
        } else {
            None
        }?;
        total = total.saturating_add(size);
    }
    Some(total)
}
