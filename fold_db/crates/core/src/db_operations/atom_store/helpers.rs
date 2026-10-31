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

/// Strip the `{storage_prefix}:` part from each scanned per-key row key.
pub(crate) fn strip_per_key_storage_prefix(
    storage_prefix: Option<&str>,
    rows: Vec<(String, super::types::PerKeyRecord)>,
) -> Vec<(String, super::types::PerKeyRecord)> {
    let strip = crate::schema::types::field::build_storage_key(storage_prefix, "");
    rows.into_iter()
        .map(|(k, r)| (k.strip_prefix(&strip).unwrap_or(&k).to_string(), r))
        .collect()
}

/// Select one logical slot from the immutable base and the live change row.
pub(crate) fn merge_generation_slot(
    live: Option<super::types::PerKeyRecord>,
    base: Option<super::types::MoleculeGenerationSlot>,
) -> Option<super::types::PerKeyRecord> {
    use super::types::MoleculeGenerationSlot;
    use crate::atom::incoming_wins_lww;
    match base {
        None
        | Some(MoleculeGenerationSlot {
            record: None,
            shadow: None,
        }) => live,
        Some(MoleculeGenerationSlot {
            record: Some(base), ..
        }) => match live {
            Some(live) if incoming_wins_lww(live.entry.lww_key(), base.entry.lww_key()) => {
                Some(live)
            }
            _ => Some(base),
        },
        Some(MoleculeGenerationSlot {
            record: None,
            shadow: Some(shadow),
        }) => {
            live.filter(|candidate| incoming_wins_lww(candidate.entry.lww_key(), shadow.lww_key()))
        }
    }
}

/// Hide a slot that a newer generation delete marker shadows.
pub(crate) fn apply_generation_delete(
    record: Option<super::types::PerKeyRecord>,
    deletion: Option<super::types::MoleculeGenerationDelete>,
) -> Option<super::types::PerKeyRecord> {
    match (record, deletion) {
        (Some(record), Some(deletion))
            if !crate::atom::incoming_wins_lww(
                record.entry.lww_key(),
                deletion.shadow.lww_key(),
            ) =>
        {
            None
        }
        (record, _) => record,
    }
}

/// Check the scope that a verb names. The only scopes a verb may name are the
/// personal scope (no value) and a database scope: exactly 64 lowercase hex
/// characters. A mistyped scope reads an empty range and looks like a clean
/// scope, so the verbs that take a scope check it before they read. The check is
/// narrower than the key validation of the storage layer on purpose: a verb
/// never needs a `cloud:` or `from:` scope or a reserved name.
pub(crate) fn validate_scope(scope: Option<&str>) -> Result<(), crate::schema::SchemaError> {
    let Some(scope) = scope else {
        return Ok(());
    };
    let lower_hex = scope.len() == 64
        && scope
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'));
    if lower_hex {
        return Ok(());
    }
    let shown: String = scope.chars().take(80).collect();
    Err(crate::schema::SchemaError::InvalidData(format!(
        "invalid scope {shown:?}: a scope is the personal scope (leave the value out) \
         or a database scope of exactly 64 lowercase hex characters"
    )))
}
