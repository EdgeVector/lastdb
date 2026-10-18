//! Retired-key cleanup and active schema-key helpers.

/// Historical derived-index prefixes that the reclaim path may delete.
pub fn retired_index_reclaim_prefixes() -> Vec<&'static str> {
    vec!["mhr:", "mhi:", "mhk:"]
}

/// Strip the (possibly `{storage_prefix}:`-prefixed) `mk:{M}:` record prefix from a
/// scanned key, yielding the encoded key segment. The scan guarantees the
/// prefix is present; the fallback only guards an otherwise-impossible mismatch.
pub(crate) fn strip_record_prefix(stored_key: &str, scan_prefix: &str) -> String {
    stored_key
        .strip_prefix(scan_prefix)
        .unwrap_or(stored_key)
        .to_string()
}

/// Key codec for the schema-keyed secondary index over atoms.
///
/// `list_atoms_by_schema` serves listings from `schemaidx:{len}:{schema}:`
/// marker keys instead of scanning every `atom:` row. The atom UUID lives in
/// the key suffix, and the canonical atom rows are fetched with one `get_many`.
/// The canonical `atom:` row and index marker are written in the same batch.
pub(crate) mod schema_index_codec {
    /// Prefix for one schema's index records: `schemaidx:{len}:{schema}:`.
    pub(crate) fn schema_prefix(schema_name: &str) -> String {
        crate::kind_partition::anchored(
            "schemaidx",
            &format!("{}:{schema_name}:", schema_name.len()),
        )
    }

    /// The index record key for one atom under a schema.
    pub(crate) fn record_key(schema_name: &str, atom_uuid: &str) -> String {
        format!("{}{atom_uuid}", schema_prefix(schema_name))
    }
}
