//! Current main-key physical placement. No older collection fallback exists.

use lastdb_model::kind_partition::{colon_prefix_matches, split_org_storage_prefix};

/// Classify one `main` key (raw bytes) into its destination collection.
///
/// Blob counters and the blob dwell queue (`bref:v3:`) use the routed
/// collection (`blob_counts`). Every other `main` row uses `records`. Returns
/// `None` for a key that this list does not name; the caller places it in
/// `records`.
pub fn classify_main_key(key: &[u8]) -> Option<&'static str> {
    let s = std::str::from_utf8(key).ok()?;
    let bare = split_org_storage_prefix(s).map_or(s, |(_, rest)| rest);
    if bare.starts_with("bref:v3:") {
        Some("blob_counts")
    } else if colon_prefix_matches(bare, "dellog:")
        || bare.starts_with("crq:")
        || bare.starts_with("crb:")
        || bare.starts_with("ahk:")
        || bare.starts_with("ahp:")
        || bare.starts_with("ahq:")
        || bare.starts_with("ahb:")
    {
        Some("records")
    } else {
        None
    }
}
