//! Current main-key physical placement. No older collection fallback exists.

use lastdb_model::kind_partition::split_org_storage_prefix;

/// Classify one `main` key (raw bytes) into its destination collection.
///
/// Blob counters and the blob dwell queue (`bref:`) use the routed
/// collection (`blob_counts`). Every other `main` row uses `records`. Returns
/// `None` for a key that is not UTF-8; the caller places it in `records`.
pub fn classify_main_key(key: &[u8]) -> Option<&'static str> {
    let s = std::str::from_utf8(key).ok()?;
    let bare = split_org_storage_prefix(s).map_or(s, |(_, rest)| rest);
    Some(if bare.starts_with("bref:") {
        "blob_counts"
    } else {
        "records"
    })
}
