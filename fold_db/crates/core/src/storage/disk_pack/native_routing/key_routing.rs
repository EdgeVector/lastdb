//! Canonical current-key placement. Native reads never consult old collections.
//!
//! Every `main` row uses `records`. A record's file lives in its own
//! namespace (`cas_blobs`), never in `main`.

use super::*;

pub(super) fn main_collection_for_key(_key: &[u8]) -> &'static str {
    RECORDS_COLLECTION
}

pub(super) fn main_collections_for_prefix(_prefix: &[u8]) -> Vec<&'static str> {
    vec![RECORDS_COLLECTION]
}

pub(super) fn main_collections_for_range(_start: &[u8], _end: &[u8]) -> Vec<&'static str> {
    vec![RECORDS_COLLECTION]
}
