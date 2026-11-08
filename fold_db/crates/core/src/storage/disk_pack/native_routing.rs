//! Native key encoding for the disk backend.
//!
//! The main namespace is the `records` collection. Every other namespace is
//! its own collection.
//!
//! ## Key encoding
//!
//! Trait keys are bytes; native ids are strings. UTF-8 keys that do not
//! start with `b64:` are stored as themselves (production field keys are
//! UTF-8). Other keys use `b64:` + standard base64.

use crate::storage::error::{StorageError, StorageResult};
use crate::storage::format::{MAIN_NAMESPACE, RECORDS_COLLECTION};
use base64::{engine::general_purpose::STANDARD, Engine as _};

/// Map a logical namespace to its one native collection.
pub(in crate::storage) fn collection_for_namespace(namespace: &str) -> &str {
    if namespace == MAIN_NAMESPACE {
        RECORDS_COLLECTION
    } else {
        namespace
    }
}

pub(in crate::storage) fn encode(key: &[u8]) -> String {
    if let Ok(key) = std::str::from_utf8(key) {
        if !key.starts_with("b64:") {
            return key.to_string();
        }
    }

    format!("b64:{}", STANDARD.encode(key))
}

pub(in crate::storage) fn decode(key: &str) -> StorageResult<Vec<u8>> {
    let Some(encoded) = key.strip_prefix("b64:") else {
        return Ok(key.as_bytes().to_vec());
    };
    STANDARD
        .decode(encoded)
        .map_err(|e| StorageError::BackendError(format!("laststore key decode failed: {e}")))
}
