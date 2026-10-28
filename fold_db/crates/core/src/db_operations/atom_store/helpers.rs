//! Active schema-key helpers.

/// Strip the (possibly `{storage_prefix}:`-prefixed) `mk:{M}:` record prefix from a
/// scanned key, yielding the encoded key segment. The scan guarantees the
/// prefix is present; the fallback only guards an otherwise-impossible mismatch.
pub(crate) fn strip_record_prefix(stored_key: &str, scan_prefix: &str) -> String {
    stored_key
        .strip_prefix(scan_prefix)
        .unwrap_or(stored_key)
        .to_string()
}
