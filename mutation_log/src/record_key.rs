//! Record key convention for history records.
//!
//! A history record key is one string. A hash-only schema uses the hash key
//! as is. A hash-range schema joins the hash and the range with
//! [`RANGE_SEPARATOR`]. The schema decides which reading applies, so a hash
//! key that holds the separator is only refused for a hash-range schema.

/// Unit separator between the hash and the range of a ranged record key.
pub const RANGE_SEPARATOR: char = '\u{1f}';

/// Join a hash and an optional range into a record key.
///
/// `None` when the hash is empty, the range is empty, or the hash holds the
/// separator (so the first separator always ends the hash).
#[must_use]
pub fn join_record_key(hash: &str, range: Option<&str>) -> Option<String> {
    if hash.is_empty() {
        return None;
    }
    let Some(range) = range else {
        return Some(hash.to_owned());
    };
    if range.is_empty() || hash.contains(RANGE_SEPARATOR) {
        return None;
    }
    Some(format!("{hash}{RANGE_SEPARATOR}{range}"))
}

/// Split a ranged record key at its first separator into `(hash, range)`.
#[must_use]
pub fn split_record_key(key: &str) -> Option<(&str, &str)> {
    let (hash, range) = key.split_once(RANGE_SEPARATOR)?;
    (!hash.is_empty() && !range.is_empty()).then_some((hash, range))
}
