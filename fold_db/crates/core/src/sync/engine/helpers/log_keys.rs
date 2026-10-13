pub(crate) fn parse_flat_log_key(key: &str) -> Option<u64> {
    parse_seq_key(relative_mutation_log_key(key).unwrap_or(key), "log/")
}

/// Strip an optional `{scope}/` prefix so list keys parse as `log/…`.
pub(crate) fn relative_mutation_log_key(listed: &str) -> Option<&str> {
    if listed.starts_with("log/") {
        return Some(listed);
    }
    listed.find("/log/").map(|i| &listed[i + 1..])
}

/// Parse a continuous-plane log object key into (optional writer, through_id).
///
/// Accepts schema-folder, writer-scoped, and legacy flat keys.
pub(crate) fn parse_mutation_log_object_key(key: &str) -> Option<(Option<String>, u64)> {
    if let Some(seq) = parse_flat_log_key(key) {
        return Some((None, seq));
    }
    let key = relative_mutation_log_key(key).unwrap_or(key);
    let rest = key.strip_prefix("log/")?;
    let (writer, rest) = rest.split_once('/')?;
    if writer.is_empty() || writer.contains("..") {
        return None;
    }
    let suffix = rest.strip_suffix(".enc")?;
    let path_parts = suffix.split('/').collect::<Vec<_>>();
    let seq = if path_parts.len() > 1 {
        let (filename, schema_parts) = path_parts.split_last()?;
        if schema_parts
            .iter()
            .any(|component| component.is_empty() || component.contains(".."))
        {
            return None;
        }
        let (utc_nanos, sequence) = filename.rsplit_once('_')?;
        let _: u64 = utc_nanos.parse().ok()?;
        sequence.parse::<u64>().ok()?
    } else {
        path_parts.first()?.parse::<u64>().ok()?
    };
    Some((Some(writer.to_string()), seq))
}

/// Parse the sequence number from a point-in-time snapshot key
/// `snapshots/{seq}.enc`.
///
/// The storage Lambda strips the scope prefix on list responses, so keys come
/// back as `snapshots/{name}` (mirrors `parse_flat_log_key` for `log/`). The
/// `snapshots/latest.enc` pointer is intentionally **not** a `{seq}.enc` key
/// and returns `None` (`"latest"` does not parse as `u64`), so retention never
/// considers it for deletion.
pub(crate) fn parse_snapshot_key(key: &str) -> Option<u64> {
    parse_seq_key(key, "snapshots/")
}

pub(crate) fn parse_seq_key(key: &str, prefix: &str) -> Option<u64> {
    let key = key.strip_prefix(prefix)?;
    let seq_str = key.strip_suffix(".enc")?;
    seq_str.parse::<u64>().ok()
}

/// Parse the sequence number from a thumbnail pack key `thumbs/packs/{seq}.enc`.
///
/// Pack IDs share the `{last_seq}.enc` naming convention with point-in-time
/// snapshots (see `compact_target` / `backup_snapshot`). Non-seq names
/// (malformed, non-numeric) return `None` and are never selected for prune.
pub(crate) fn parse_thumb_pack_key(key: &str) -> Option<u64> {
    parse_seq_key(key, "thumbs/packs/")
}
