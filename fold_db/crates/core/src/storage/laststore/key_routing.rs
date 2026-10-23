//! Canonical current-key placement. Native reads never consult old collections.

use super::*;

/// Current physical prefixes and their exact destination collections.
const ROUTED_PREFIXES: &[(&str, &str)] = &[
    ("atom:", "atoms"),
    ("fdep:v1:", "atoms"),
    ("mref:v1:", "molecule_ref_edges"),
    ("bref:v3:", "atoms"),
    ("protein:", "proteins"),
    ("molprot:", "proteins"),
    ("fldprot:", "proteins"),
    ("pfq:", "proteins"),
    ("schemaidx\0", "indexes"),
];

pub(super) fn main_collection_for_key(key: &[u8]) -> &'static str {
    let bare = std::str::from_utf8(key)
        .ok()
        .and_then(|key| crate::storage::current_keys::source_key(key).ok());
    bare.and_then(|key| classify_main_key(key.as_bytes()))
        .unwrap_or(TIPS_COLLECTION)
}

/// A structural prefix selects only current families. A prefix that stops
/// inside a scope can intersect every current collection.
pub(super) fn main_collections_for_prefix(prefix: &[u8]) -> Vec<&'static str> {
    let Some(bare) = std::str::from_utf8(prefix)
        .ok()
        .and_then(|prefix| crate::storage::current_keys::source_key(prefix).ok())
    else {
        return all_current_collections();
    };
    let family = bare.split([':', '\0']).next().unwrap_or_default();
    if !crate::storage::current_keys::main_family(family) {
        return all_current_collections();
    }
    let mut collections = Vec::new();
    for (family_prefix, collection) in ROUTED_PREFIXES {
        if (family_prefix.starts_with(bare) || bare.starts_with(family_prefix))
            && !collections.contains(collection)
        {
            collections.push(*collection);
        }
    }
    if collections.is_empty() {
        collections.push(TIPS_COLLECTION);
    }
    collections
}

fn all_current_collections() -> Vec<&'static str> {
    let mut collections = vec![TIPS_COLLECTION];
    for (_, collection) in ROUTED_PREFIXES {
        if !collections.contains(collection) {
            collections.push(*collection);
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
