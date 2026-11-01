use super::*;

/// `mk:{M}:{esc(hash)}\0{range}` — unified per-key record key for every field kind.
#[must_use]
pub fn hash_range_record_key(molecule_uuid: &str, hash: &str, range: &str) -> String {
    format!(
        "{MK_PREFIX}{molecule_uuid}:{}{SEP}{range}",
        escape_segment(hash)
    )
}

/// `mk:{M}:{esc(hash)}\0` — prefix selecting exactly one hash's ranges in a
/// HashRange field (`HashKey` scan). Unambiguous: `hash="a"` won't match `"ab"`.
#[must_use]
pub fn hash_range_scan_prefix_for_hash(molecule_uuid: &str, hash: &str) -> String {
    format!("{MK_PREFIX}{molecule_uuid}:{}{SEP}", escape_segment(hash))
}

/// Parse the part of a unified record key that follows `mk:{M}:` (the
/// `esc(hash)\0range` suffix) back into `(hash, range)`. Use this when the
/// caller has already stripped the full — possibly `{storage_prefix}:`-prefixed —
/// record prefix from a scan. `None` if the escape is malformed or the suffix
/// is not unified (no `\0` delimiter).
#[must_use]
pub fn decode_hash_range_suffix(suffix: &str) -> Option<(String, String)> {
    // esc(hash) contains no SEP, so the first SEP is the delimiter; the range
    // (which may itself contain SEP) is everything after it.
    let (esc, range) = suffix.split_once(SEP)?;
    let hash = unescape_segment(esc)?;
    Some((hash, range.to_string()))
}
