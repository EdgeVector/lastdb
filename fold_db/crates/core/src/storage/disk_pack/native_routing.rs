//! Canonical current-key routing for the native disk backend.
//!
//! ## Key encoding
//!
//! Trait keys are bytes; native ids are strings. UTF-8 keys that do not
//! start with `b64:` are stored as themselves (production field keys are
//! UTF-8). Other keys use `b64:` + standard base64.

mod key_routing;

use crate::mini_cutover::classify_main_key;
use crate::storage::error::{StorageError, StorageResult};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use key_routing::*;

const TIPS_COLLECTION: &str = "tips";

pub(in crate::storage) fn collection_for_key(key: &[u8]) -> &'static str {
    main_collection_for_key(key)
}

pub(in crate::storage) fn prefix_collections(prefix: &[u8]) -> Vec<&'static str> {
    main_collections_for_prefix(prefix)
}

pub(in crate::storage) fn range_collections(start: &[u8], end: &[u8]) -> Vec<&'static str> {
    main_collections_for_range(start, end)
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
