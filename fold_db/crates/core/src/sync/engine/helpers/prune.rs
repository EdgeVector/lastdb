use super::log_keys::*;

/// Given the bare snapshot names under `snapshots/` (as returned by
/// `list_snapshot_objects`, i.e. `snapshots/{name}`) and a retention count,
/// return the bare `{name}` of each point-in-time `{seq}.enc` snapshot that
/// should be **deleted**.
///
/// Retention policy:
/// - The `latest.enc` pointer is always kept (it never parses as a seq).
/// - The `retain` highest-seq `{seq}.enc` snapshots are kept.
/// - Every older `{seq}.enc` snapshot is returned for deletion.
///
/// `retain == 0` keeps ONLY `latest.enc` (every `{seq}.enc` is pruned). That is
/// safe and intended: `latest.enc` is always kept here, and restore reads the
/// snapshot together with its `last_seq` from `latest.enc` alone — the
/// timestamped copies are never needed to restore. Returns bare names
/// (e.g. `"42.enc"`) ready for `presign_snapshot_delete`.
pub(crate) fn snapshots_to_prune(keys: &[String], retain: usize) -> Vec<String> {
    // Collect (seq, bare_name) for every point-in-time snapshot.
    let mut snapshots: Vec<(u64, String)> = keys
        .iter()
        .filter_map(|key| {
            let seq = parse_snapshot_key(key)?;
            // Strip the `snapshots/` prefix to get the bare name the delete
            // endpoint expects (e.g. `42.enc`).
            let name = key.strip_prefix("snapshots/")?.to_string();
            Some((seq, name))
        })
        .collect();
    // Highest seq first; keep the first `retain`, prune the rest.
    snapshots.sort_unstable_by_key(|(seq, _)| std::cmp::Reverse(*seq));
    snapshots
        .into_iter()
        .skip(retain)
        .map(|(_, name)| name)
        .collect()
}

/// Given bare or prefixed pack keys under `thumbs/packs/` and a retention
/// count, return each bare `{seq}.enc` pack id that should be **deleted**.
///
/// Retention policy (mirrors [`snapshots_to_prune`]):
/// - The `retain` highest-seq packs are kept.
/// - Every older `{seq}.enc` pack is returned for deletion.
///
/// Unlike snapshots, packs have **no** `latest.enc` dual-write — the current
/// pack *is* `{last_seq}.enc`. Callers that pass `snapshot_retention == 0`
/// should therefore pass `retain.max(1)` so the newest pack survives as the
/// restore source for packed thumbnails (loose objects are deleted after pack
/// upload). Returns bare ids (e.g. `"42.enc"`) for `presign_thumb_pack_delete`.
pub(crate) fn thumb_packs_to_prune(keys: &[String], retain: usize) -> Vec<String> {
    let mut packs: Vec<(u64, String)> = keys
        .iter()
        .filter_map(|key| {
            // Accept both full list keys (`thumbs/packs/42.enc`) and bare
            // ids already stripped by a caller.
            let seq = parse_thumb_pack_key(key).or_else(|| {
                let seq_str = key.strip_suffix(".enc")?;
                seq_str.parse::<u64>().ok()
            })?;
            let name = key
                .strip_prefix("thumbs/packs/")
                .unwrap_or(key.as_str())
                .to_string();
            // Only prune true `{seq}.enc` leaves; skip anything that lost
            // its numeric identity above.
            if !name.ends_with(".enc") {
                return None;
            }
            Some((seq, name))
        })
        .collect();
    packs.sort_unstable_by_key(|(seq, _)| std::cmp::Reverse(*seq));
    packs
        .into_iter()
        .skip(retain)
        .map(|(_, name)| name)
        .collect()
}
