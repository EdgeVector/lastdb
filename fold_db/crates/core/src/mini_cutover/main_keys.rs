//! Current main-key physical placement. No older collection fallback exists.

use crate::kind_partition::{colon_prefix_matches, split_org_storage_prefix};

/// Classify one `main` key (raw bytes) into its destination collection.
/// Returns `None` for an unknown prefix — callers must fail loudly.
pub fn classify_main_key(key: &[u8]) -> Option<&'static str> {
    let s = std::str::from_utf8(key).ok()?;
    let bare = split_org_storage_prefix(s).map_or(s, |(_, rest)| rest);
    if colon_prefix_matches(bare, "atom:") {
        Some("atoms")
    } else if bare.starts_with("mref:v1:") {
        Some("molecule_ref_edges")
    } else if bare.starts_with("bref:v3:") {
        Some("atoms")
    } else if bare.starts_with("protein:")
        || bare.starts_with("molprot:")
        || bare.starts_with("fldprot:")
        || bare.starts_with("pfq:")
    {
        // Ideal plane: first-class proteins collection (docs/lastdb-ideal-storage-shape.md).
        Some("proteins")
    } else if colon_prefix_matches(bare, "schemaidx:") {
        Some("indexes")
    } else if bare.starts_with("mk:")
        || bare.starts_with("rdel:v2:")
        || bare.starts_with("mgp:v1:")
        || bare.starts_with("mgr:v1:")
        || bare.starts_with("mgd:v1:")
        || bare.starts_with("mh:")
        || colon_prefix_matches(bare, "history:")
        || colon_prefix_matches(bare, "conflict:")
        || colon_prefix_matches(bare, "dellog:")
        || bare.starts_with("crq:")
        || bare.starts_with("crb:")
        || bare.starts_with("crs:")
    {
        Some("tips")
    } else {
        None
    }
}
