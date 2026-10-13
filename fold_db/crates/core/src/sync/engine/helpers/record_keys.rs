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

pub(crate) fn hash_range_page_index_complete_key_for_record_key(
    key: &str,
    mol_uuid: &str,
) -> Option<String> {
    let record_prefix = crate::atom::molecule_key_codec::molecule_record_prefix(mol_uuid);
    let prefix_pos = key.find(&record_prefix)?;
    let suffix = &key[prefix_pos + record_prefix.len()..];
    if !suffix.contains('\u{0}') {
        return None;
    }
    crate::atom::molecule_key_codec::decode_hash_range_suffix(suffix)?;
    let storage_prefix = &key[..prefix_pos];
    Some(format!(
        "{}{}",
        storage_prefix,
        crate::atom::molecule_key_codec::hash_range_page_index_complete_key(mol_uuid)
    ))
}

/// Extract the molecule uuid `M` from a header key `mh:{M}` (optionally
/// prefixed). `M` is the whole remainder after `mh:`. Returns `None` if the key
/// is not a header key.
pub(crate) fn molecule_uuid_from_header_key(key: &str) -> Option<String> {
    let after = position_after_prefix(key, crate::atom::molecule_key_codec::MH_PREFIX)?;
    let uuid = &key[after..];
    (!uuid.is_empty()).then(|| uuid.to_string())
}

/// Whether `key` is a HashRange append-log order entry `mord:{M}:{seq}`
/// or `mord\0{M}:{seq}` (optionally `{org}:`-/`from:`-prefixed).
pub(crate) fn is_order_log_entry_key(key: &str) -> bool {
    crate::kind_partition::rest_of(key, "mord").is_some_and(|rest| !rest.is_empty())
}

/// Whether `key` is a HashRange append-log count record `moc:{M}` or
/// `moc\0{M}` (optionally `{org}:`-/`from:`-prefixed).
pub(crate) fn is_order_count_key(key: &str) -> bool {
    crate::kind_partition::rest_of(key, "moc").is_some_and(|rest| !rest.is_empty())
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
