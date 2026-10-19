//! Canonical current-key placement. Native reads never consult old collections.

use super::*;

/// Non-default current key families. Unlisted keys belong to `tips`.
const ROUTED_PREFIXES: &[&str] = &[
    "atom:",
    "aref:v2:",
    "aref:",
    "mref:v1:",
    "bref:v1:",
    "protein:",
    "molprot:",
    "fldprot:",
    "pfq:",
    "mhr:",
    "mhk:",
    "mhi:",
    "schema_atoms:",
    "idx:",
    "schemaidx:",
];

pub(super) fn main_collection_for_key(key: &[u8]) -> &'static str {
    classify_main_key(key).unwrap_or(TIPS_COLLECTION)
}

/// `key` without a leading 64-hex org storage prefix (`{sha256}:rest`).
pub(super) fn strip_org_storage_prefix(key: &str) -> &str {
    crate::kind_partition::split_org_storage_prefix(key).map_or(key, |(_, rest)| rest)
}

pub(super) fn main_collections_for_key(key: &[u8]) -> Vec<&'static str> {
    vec![main_collection_for_key(key)]
}

/// Broad metadata prefixes can intersect several current key families.
pub(super) fn main_collections_for_prefix(prefix: &[u8]) -> Vec<&'static str> {
    let mut collections = vec![main_collection_for_key(prefix)];
    let Ok(prefix) = std::str::from_utf8(prefix) else {
        return collections;
    };
    let bare = strip_org_storage_prefix(prefix);
    for family in ROUTED_PREFIXES {
        if family.starts_with(bare) {
            let collection = main_collection_for_key(family.as_bytes());
            if !collections.contains(&collection) {
                collections.push(collection);
            }
        }
    }
    collections
}

pub(super) fn main_collections_for_range(start: &[u8], end: &[u8]) -> Vec<&'static str> {
    if end.is_empty() {
        return main_collections_for_prefix(start);
    }
    let common_len = start
        .iter()
        .zip(end)
        .take_while(|(left, right)| left == right)
        .count();
    main_collections_for_prefix(&start[..common_len])
}
