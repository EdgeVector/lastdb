//! Exact record families whose existing payload envelope is sufficient.

use crate::atom::{atom_binary_row_parts, atom_key_codec};
use crate::crypto::is_sealed_at_rest;
use crate::sharing::delivery_wire::FILE_BLOB_CIPHER_SUITE;
use serde::Deserialize;

/// Counters and metadata in the same physical collection retain their outer seal.
pub(crate) fn is_inner_protected_family(namespace: &str, key: &[u8]) -> bool {
    match namespace {
        "main" | "atoms" => std::str::from_utf8(key)
            .ok()
            .and_then(atom_key_codec::body_uuid_of)
            .is_some(),
        "cas_blobs" => key
            .strip_prefix(b"sha256:")
            .is_some_and(|hash| hash.len() == 64 && hash.iter().all(u8::is_ascii_hexdigit)),
        _ => false,
    }
}

/// Recognize the unchanged element format without copying its encrypted payload.
/// The ordinary atom/file reader still authenticates the inner envelope.
pub(crate) fn has_inner_protection(namespace: &str, key: &[u8], value: &[u8]) -> bool {
    if !is_inner_protected_family(namespace, key) {
        return false;
    }
    if namespace == "cas_blobs" {
        return cas_has_inner_seal(key, value);
    }
    atom_has_inner_seal(value)
}

#[derive(Deserialize)]
struct AtomContent<'a> {
    #[serde(borrow)]
    content: &'a str,
}

pub(crate) fn atom_has_inner_seal(value: &[u8]) -> bool {
    if value.starts_with(crate::atom::ATOM_BINARY_ROW_PREFIX) {
        return atom_binary_row_parts(value)
            .ok()
            .flatten()
            .is_some_and(|(_, content)| is_sealed_at_rest(content));
    }
    serde_json::from_slice::<AtomContent<'_>>(value)
        .is_ok_and(|atom| is_sealed_at_rest(atom.content.as_bytes()))
}

#[derive(Deserialize)]
struct FileContent<'a> {
    #[serde(borrow)]
    blob_ref: &'a str,
    #[serde(borrow)]
    content_sha256: &'a str,
    #[serde(borrow)]
    local_cipher_suite: &'a str,
    #[serde(borrow)]
    bytes_b64: &'a str,
    #[serde(default)]
    access: Option<serde::de::IgnoredAny>,
}

pub(crate) fn cas_has_inner_seal(key: &[u8], value: &[u8]) -> bool {
    serde_json::from_slice::<FileContent<'_>>(value).is_ok_and(|file| {
        file.access.is_none()
            && file.local_cipher_suite == FILE_BLOB_CIPHER_SUITE
            && file.blob_ref.as_bytes() == key
            && key.strip_prefix(b"sha256:") == Some(file.content_sha256.as_bytes())
            && file
                .bytes_b64
                .starts_with(crate::crypto::AT_REST_ENC_PREFIX)
    })
}
