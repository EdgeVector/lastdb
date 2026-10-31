use super::*;

const MOLECULE_KEY_PREFIXES: &[&str] = &[MK_PREFIX, MH_PREFIX];

/// Which part of a molecule's storage counter one written row feeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoleculeCounterRow {
    /// A current `mk:` tip record (`tip_index_bytes`).
    Tip,
    /// A current molecule header (`molecule_metadata_bytes`).
    Structure,
}

/// Classify a current tip or molecule-header key for the molecule counter.
#[must_use]
pub fn molecule_counter_row(key: &str) -> Option<(MoleculeCounterRow, &str)> {
    let candidates = [
        (MK_PREFIX, MoleculeCounterRow::Tip),
        (MH_PREFIX, MoleculeCounterRow::Structure),
    ];
    let (start, prefix, class) = candidates
        .iter()
        .filter_map(|(prefix, class)| key.find(prefix).map(|at| (at, *prefix, *class)))
        .min_by_key(|(at, _, _)| *at)?;
    let rest = &key[start + prefix.len()..];
    let end = rest.find([':', SEP]).unwrap_or(rest.len());
    let uuid = &rest[..end];
    if uuid.is_empty() {
        None
    } else {
        Some((class, uuid))
    }
}

/// Read the molecule uuid out of a molecule-scoped storage key.
///
/// Accepts keys with or without a `{storage_prefix}:` head, because the
/// keep-small write path meters the final storage key rather than the base
/// key. Returns `None` for a key that names no molecule (an atom row, a
/// metadata row), which is the caller's signal to leave those bytes
/// unattributed rather than guess an owner.
#[must_use]
pub fn molecule_uuid_from_storage_key(key: &str) -> Option<&str> {
    let (start, prefix) = MOLECULE_KEY_PREFIXES
        .iter()
        .filter_map(|prefix| key.find(prefix).map(|at| (at, *prefix)))
        .min_by_key(|(at, _)| *at)?;
    let rest = &key[start + prefix.len()..];
    let end = rest.find([':', SEP]).unwrap_or(rest.len());
    let uuid = &rest[..end];
    if uuid.is_empty() {
        None
    } else {
        Some(uuid)
    }
}

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

/// Parse a stored HashRange record key back into `(hash, range)`. `None` if the
/// prefix doesn't match or the escape is malformed.
#[must_use]
pub fn decode_hash_range(stored_key: &str, molecule_uuid: &str) -> Option<(String, String)> {
    let suffix = stored_key.strip_prefix(&molecule_record_prefix(molecule_uuid))?;
    decode_hash_range_suffix(suffix)
}

/// Parse a stored HashRange record key when the caller does not know which
/// molecule UUID is on the key.
#[must_use]
pub fn decode_hash_range_any(stored_key: &str) -> Option<(String, String)> {
    let molecule_uuid = molecule_uuid_from_storage_key(stored_key)?;
    decode_hash_range(stored_key, molecule_uuid)
}
