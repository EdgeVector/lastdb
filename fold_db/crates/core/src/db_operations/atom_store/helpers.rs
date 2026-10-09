//! Pure helpers: per-key explosion, page-index keys, schema index codec.

use crate::atom::molecule_key_codec;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use serde_json::Value;

use super::types::{HashKeyLookupRecord, PerKeyRecord};

/// When `false`, LastDB does **not** co-write, rebuild, or prefer the derived
/// range-major page index (`mhr:` / `mhi:`).
///
/// Product access is Dynamo-style and key-restricted (`HashKey` /
/// `HashRangePrefix` / …) over authoritative `mk:` tips. Whole-field `Page`
/// and residual full-molecule loads fall back to `mk:` scans only.
///
/// Locked direction (Tom 2026-08-01): no engine-derived secondary list for
/// HashRange; extra access patterns are other schemas (+ proteins), not `mhr`.
pub(crate) const HASH_RANGE_PAGE_INDEX_ENABLED: bool = false;

/// When `false`, LastDB does **not** co-write derived HashKey lookup markers
/// (`mhk:`). `HashKey` reads skip the marker and scan the authoritative
/// `mk:{M}:{hash}\0*` prefix (already the fallback when the marker is missing
/// or ambiguous).
///
/// Same standing rule as the page index: no engine-derived HashRange
/// secondaries — see `preference-lastdb-no-engine-derived-hashrange-secondaries`.
pub(crate) const HASH_RANGE_HASH_KEY_LOOKUP_ENABLED: bool = false;

/// The `indexes`-plane key prefixes whose derived rows are reclaimable, given
/// the two co-write flags above.
///
/// A prefix is listed **only** when its flag is off, because off means the
/// engine neither co-writes, rebuilds, nor prefers those rows — the reads fall
/// back to authoritative `mk:` tips. Flip a flag back on and its prefixes drop
/// out of this list, so the reclaim path cannot outlive the retirement that
/// justifies it. That coupling is the whole point: the delete is safe because
/// of the flag, so it must be derived from the flag rather than restated next
/// to it, where the two could silently disagree.
///
/// `schemaidx:` is deliberately absent — it has its own verb
/// (`lastdb db purge-schemaidx`). `schema_atoms:` / `idx:` are absent because
/// no retirement flag governs them.
///
/// Standing rule: `preference-lastdb-no-engine-derived-hashrange-secondaries`.
pub fn retired_index_reclaim_prefixes_for(
    page_index_enabled: bool,
    hash_key_lookup_enabled: bool,
) -> Vec<&'static str> {
    let mut prefixes = Vec::new();
    if !page_index_enabled {
        // `mhi:` is the completion marker for the `mhr:` index; a home holding
        // `mhi:` without `mhr:` would claim a complete index that is not there,
        // so the pair is retired together or not at all.
        prefixes.push("mhr:");
        prefixes.push("mhi:");
    }
    if !hash_key_lookup_enabled {
        prefixes.push("mhk:");
    }
    prefixes
}

/// [`retired_index_reclaim_prefixes_for`] applied to this build's flags.
pub fn retired_index_reclaim_prefixes() -> Vec<&'static str> {
    retired_index_reclaim_prefixes_for(
        HASH_RANGE_PAGE_INDEX_ENABLED,
        HASH_RANGE_HASH_KEY_LOOKUP_ENABLED,
    )
}

pub(crate) fn hash_range_page_index_item(
    molecule_uuid: &str,
    hash: &str,
    range: &str,
    storage_prefix: Option<&str>,
) -> (String, Value) {
    (
        build_storage_key(
            storage_prefix,
            &molecule_key_codec::hash_range_page_index_key(molecule_uuid, hash, range),
        ),
        Value::Bool(true),
    )
}

pub(crate) fn hash_range_page_index_item_for_record_key(
    molecule_uuid: &str,
    base_key: &str,
    storage_prefix: Option<&str>,
) -> Option<(String, Value)> {
    let (hash, range) = molecule_key_codec::decode_hash_range(base_key, molecule_uuid)?;
    Some(hash_range_page_index_item(
        molecule_uuid,
        &hash,
        &range,
        storage_prefix,
    ))
}

/// What the page index was last built from, stamped into the completion
/// marker's value.
///
/// The marker used to be a bare `true` — presence meant "built", and nothing
/// recorded *what it was built from*. That is what let one unfetchable row turn
/// every paged read into a full `mk:` sweep forever: a stale window answers with
/// a rebuild, the rebuild re-derives the same phantom entry, the next read is
/// stale again.
///
/// `repaired` is what keeps the cure from being worse than the disease. A
/// rebuild is NOT always a no-op at an unchanged header: a record deleted out
/// of band leaves an orphan index row, and only a rebuild reaps it. So the
/// budget is **one repair per header** — the first stale window at a given
/// `(version, updated_at)` rebuilds and sets this flag; a second stale window
/// at the same header means the rebuild did not resolve it (a listing/fetch
/// disagreement, not an orphan) and is skipped. Orphans still get collected;
/// the sweep loop still terminates.
///
/// A legacy `true` marker deserializes to `None`, which reads as "unknown
/// provenance" and permits a rebuild — the conservative direction.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct PageIndexBuiltAt {
    pub version: u64,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    #[serde(default)]
    pub repaired: bool,
}

/// Parse a completion-marker value back into the header it was built from.
/// `None` for the legacy `true` marker or any shape that does not round-trip.
pub(crate) fn page_index_built_at(value: &Value) -> Option<PageIndexBuiltAt> {
    serde_json::from_value(value.clone()).ok()
}

pub(crate) fn hash_range_page_index_complete_item(
    molecule_uuid: &str,
    version: u64,
    updated_at: chrono::DateTime<chrono::Utc>,
    repaired: bool,
    storage_prefix: Option<&str>,
) -> (String, Value) {
    let built_at = serde_json::json!({
        "version": version,
        "updated_at": updated_at,
        "repaired": repaired,
    });
    (
        build_storage_key(
            storage_prefix,
            &molecule_key_codec::hash_range_page_index_complete_key(molecule_uuid),
        ),
        built_at,
    )
}

pub(crate) fn hash_key_lookup_item(
    molecule_uuid: &str,
    hash: &str,
    range_and_record: Option<(&str, &PerKeyRecord)>,
    storage_prefix: Option<&str>,
) -> Result<(String, Value), SchemaError> {
    let marker = match range_and_record {
        Some((range, record)) => HashKeyLookupRecord {
            range: Some(range.to_string()),
            record: Some(record.clone()),
        },
        None => HashKeyLookupRecord {
            range: None,
            record: None,
        },
    };
    let value = serde_json::to_value(marker)
        .map_err(|e| SchemaError::InvalidData(format!("serialize hash-key marker: {e}")))?;
    Ok((
        build_storage_key(
            storage_prefix,
            &molecule_key_codec::hash_key_lookup_key(molecule_uuid, hash),
        ),
        value,
    ))
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

    /// Backfill sentinel for a storage scope.
    pub(crate) const BACKFILL_SENTINEL: &str = "schemaidx_v1_done";
}
