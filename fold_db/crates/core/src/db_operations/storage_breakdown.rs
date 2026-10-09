//! Per-schema logical storage breakdown.
//!
//! Answers "how many bytes is each schema responsible for?" by walking the
//! local atom store and summing each atom's serialized size, grouped by the
//! atom's `source_schema_name`.
//!
//! This is a **logical, plaintext** size — the serialized JSON bytes of the
//! locally-stored atoms — NOT the exact encrypted-cloud footprint. Cloud
//! objects are E2E-encrypted opaque blobs the server cannot attribute to a
//! schema (see `exemem_common::storage::is_r2_path` and the storage_service
//! billing path), so per-schema attribution can only be computed client-side
//! here, where the decrypted store and the schema definitions both live. It is
//! an approximation of the cloud footprint, deliberately high-level: encryption
//! overhead, Sled page padding, and the sync log/snapshot framing are not
//! modeled. Tom confirmed approximate/high-level is fine for the storage
//! breakdown view.

use crate::{
    db_operations::{
        keep_small::{atom_histogram, AtomHistogram},
        AtomStore,
    },
    schema::SchemaError,
};
use serde::{Deserialize, Serialize};

/// One schema's contribution to local logical storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaStorage {
    /// Canonical runtime schema name (identity hash, possibly
    /// `{storage_prefix}:{hash}`) — the same name `list_atoms_by_schema` keys on.
    pub schema_name: String,
    /// Catalog `descriptive_name` (or a more human catalog key) when one
    /// exists and differs from `schema_name`. Display join only — never an
    /// accounting key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// Sum of the serialized byte sizes of every atom whose
    /// `source_schema_name` is this schema.
    pub bytes: u64,
    /// How many atoms contributed to `bytes`. Surfaced so the UI can show
    /// "N items" alongside the size and so a schema with many tiny atoms reads
    /// differently from one with a few large ones.
    pub atom_count: u64,
}

/// The local logical storage breakdown: per-schema sizes plus the total.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StorageBreakdown {
    /// One entry per schema that has at least one atom, sorted by `bytes`
    /// descending (largest first) so the UI can render a sorted bar/list and
    /// take a top-N without re-sorting.
    pub per_schema: Vec<SchemaStorage>,
    /// `sum(per_schema[].bytes)` — the total logical bytes across all schemas.
    pub total_logical_bytes: u64,
    /// Atom-size histogram over the same walk (p50/p95/p99/max + fence counts).
    #[serde(default)]
    pub histogram: AtomHistogram,
}

impl AtomStore {
    /// Compute the per-schema logical storage breakdown over `schema_names`.
    ///
    /// This makes a single pass over the canonical `atom:` rows — not the
    /// schema-keyed index — so a schema's atoms are counted once rather than
    /// once per secondary-index copy. Cost is therefore O(all atoms), not
    /// O(result): filtering by `schema_names` narrows what is *reported*, not
    /// what is read. The pass is paged, so it is bounded in memory but not in
    /// time; treat this as an admin/reporting verb, not a hot path.
    ///
    /// Schemas with no atoms are omitted. The returned `per_schema` is sorted by
    /// `bytes` descending.
    ///
    /// `storage_prefix` bounds the scanned key range, so passing `Some(prefix)`
    /// reads only keys under that storage prefix (exact; no bare dual-read) and
    /// `None` the personal-only view — matching the rest of the read path.
    ///
    /// Size metric: `serde_json::to_vec(atom).len()`, i.e. the serialized bytes
    /// of the full `Atom` record (content + provenance) as it round-trips
    /// through the store. This is a logical plaintext approximation, not the
    /// encrypted on-disk or cloud size (see the module docs).
    pub async fn storage_breakdown(
        &self,
        schema_names: &[String],
        storage_prefix: Option<&str>,
    ) -> Result<StorageBreakdown, SchemaError> {
        use super::admin_db::{inventory_next_page_rows, INVENTORY_PAGE_ROWS_MIN};
        use crate::schema::types::field::build_storage_key;
        use std::collections::{HashMap, HashSet};

        // Single pass over canonical atom rows so logical byte totals are
        // not double-counted by the schema secondary index copies.
        // Writes land at `atom\0{uuid}`; leftover `atom:{uuid}` still exists.
        let wanted: HashSet<&str> = schema_names.iter().map(String::as_str).collect();
        let colon = build_storage_key(storage_prefix, "atom:");
        let (scan_prefix, scan_end) = crate::kind_partition::colon_plane_bounds(&colon);

        let mut agg: HashMap<String, (u64, u64)> = HashMap::new();
        let mut sizes: Vec<u64> = Vec::new();
        // `atom:` is every field value in the database, and each row is decoded
        // into an `Atom` that is larger resident than the bytes it came from.
        // Reading the whole prefix therefore cost roughly the atom keyspace
        // twice over — on a live node, for a per-schema total. Page instead, so
        // only one bounded page and one decoded atom are held at a time. See
        // `INVENTORY_PAGE_BYTE_BUDGET` for the budget these pages aim at.
        let mut cursor: Option<Vec<u8>> = None;
        let mut page_rows = INVENTORY_PAGE_ROWS_MIN;
        loop {
            let start = cursor.as_deref().unwrap_or(scan_prefix.as_bytes());
            let rows = self
                .raw()
                .inner()
                .scan_range_paged(start, scan_end.as_bytes(), page_rows)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("storage_breakdown scan: {e}")))?;
            if rows.is_empty() {
                break;
            }
            let exhausted = rows.len() < page_rows;
            let mut page_bytes = 0u64;
            for (k, v) in &rows {
                page_bytes += (k.len() + v.len()) as u64;
                // The start bound is inclusive, so the previous page's last row
                // leads this one; skip it by key, not by position.
                if cursor.as_deref() == Some(k.as_slice()) {
                    continue;
                }
                let atom = self.decode_atom_bytes(v).await.map_err(|e| {
                    SchemaError::InvalidData(format!(
                        "storage_breakdown scan: failed to decode {}: {e}",
                        String::from_utf8_lossy(k)
                    ))
                })?;
                let name = atom.source_schema_name();
                if !wanted.is_empty() && !wanted.contains(name) {
                    continue;
                }
                let b = serde_json::to_vec(&atom).map_or(0, |v| v.len() as u64);
                sizes.push(b);
                let entry = agg.entry(name.to_string()).or_default();
                entry.0 += b;
                entry.1 += 1;
            }
            if exhausted {
                break;
            }
            page_rows = inventory_next_page_rows(page_bytes, rows.len());
            cursor = rows.last().map(|(k, _)| k.clone());
        }

        let mut per_schema: Vec<SchemaStorage> = agg
            .into_iter()
            .map(|(schema_name, (bytes, atom_count))| SchemaStorage {
                schema_name,
                display_name: None,
                bytes,
                atom_count,
            })
            .collect();
        let total_logical_bytes: u64 = per_schema.iter().map(|s| s.bytes).sum();

        // Largest schema first; tie-break on name for deterministic ordering.
        per_schema.sort_by(|a, b| {
            b.bytes
                .cmp(&a.bytes)
                .then_with(|| a.schema_name.cmp(&b.schema_name))
        });

        Ok(StorageBreakdown {
            per_schema,
            total_logical_bytes,
            histogram: atom_histogram(&sizes),
        })
    }
}

impl StorageBreakdown {
    /// Join human catalog names onto accounting rows.
    ///
    /// `names` maps runtime identity (`schema_name`) → display label. A label
    /// equal to the identity is ignored so hashes are not duplicated.
    pub fn apply_display_names(&mut self, names: &std::collections::HashMap<String, String>) {
        for row in &mut self.per_schema {
            if row.display_name.is_some() {
                continue;
            }
            let Some(label) = names.get(&row.schema_name) else {
                continue;
            };
            let label = label.trim();
            if label.is_empty() || label == row.schema_name {
                continue;
            }
            row.display_name = Some(label.to_string());
        }
    }
}
