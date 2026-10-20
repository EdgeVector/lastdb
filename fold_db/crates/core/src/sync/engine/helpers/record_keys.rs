/// Extract the molecule uuid `M` from a per-key record key `mk:{M}:{key}`
/// (optionally `{org}:`- or `from:{sender}:`-prefixed). `M` is a 64-hex
/// molecule uuid and contains no `:`, so it is the segment between `mk:` and
/// the next `:`. Returns `None` if the key is not a per-key record key.
pub(crate) fn molecule_uuid_from_record_key(key: &str) -> Option<String> {
    let after = position_after_prefix(key, crate::atom::molecule_key_codec::MK_PREFIX)?;
    let rest = &key[after..];
    // `mk:{M}:{key}` — `M` ends at the first `:` (uuids never contain `:`).
    let (uuid, _) = rest.split_once(':')?;
    (!uuid.is_empty()).then(|| uuid.to_string())
}

/// Extract the molecule uuid `M` from a header key `mh:{M}` (optionally
/// prefixed). `M` is the whole remainder after `mh:`. Returns `None` if the key
/// is not a header key.
pub(crate) fn molecule_uuid_from_header_key(key: &str) -> Option<String> {
    let after = position_after_prefix(key, crate::atom::molecule_key_codec::MH_PREFIX)?;
    let uuid = &key[after..];
    (!uuid.is_empty()).then(|| uuid.to_string())
}

/// Byte index just past the first occurrence of `marker` in `key`, requiring
/// that `marker` is either at the start or immediately follows a `:` (so a
/// random substring `...mk:...` inside an arbitrary value can't be mistaken for
/// the record prefix — the only legitimate prefixes are `{org}:` and
/// `from:{sender}:`, both `:`-terminated).
pub(crate) fn position_after_prefix(key: &str, marker: &str) -> Option<usize> {
    if let Some(rest) = key.strip_prefix(marker) {
        return Some(key.len() - rest.len());
    }
    let needle = format!(":{marker}");
    let idx = key.find(&needle)?;
    Some(idx + needle.len())
}

/// Return the storage scope before a molecule key marker.
///
/// `build_storage_key` expects the prefix without its trailing delimiter:
/// `org:mk:{M}:k` -> `Some("org")`, while bare `mk:{M}:k` stays personal.
pub(crate) fn storage_scope_for_key_marker<'a>(key: &'a str, marker: &str) -> Option<&'a str> {
    if key.starts_with(marker) {
        return None;
    }
    let needle = format!(":{marker}");
    let marker_pos = key.find(&needle)?;
    (marker_pos > 0).then(|| &key[..marker_pos])
}
