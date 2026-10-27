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
//! overhead, disk file framing, and the sync log/snapshot framing are not
//! modeled. Tom confirmed approximate/high-level is fine for the storage
//! breakdown view.

use crate::{
    db_operations::{
        atom_store::{decode_plain_atom, RangeWalk},
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

/// What one inventory walk found about the atom counters.
///
/// The walk reads every `atom:` record in key order. A body record
/// (`atom:{uuid}\0b`) is followed by its counter record (`atom:{uuid}\0c`),
/// so the walk checks the pairing as it goes: a body with no counter and a
/// counter with no body are both faults. A scan of the whole home is a
/// maintenance verb. The planner never reads a scan result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AtomCounterTotals {
    /// Counter records read.
    pub counters: u64,
    /// Sum of `refs` over the counters.
    pub refs_total: u64,
    /// Largest `refs` of one counter.
    pub max_refs: u64,
    /// Counters with the `suspect` flag.
    pub suspect: u64,
    /// Bodies that have no counter record.
    pub bodies_without_counter: u64,
    /// Counters that have no body record.
    pub counters_without_body: u64,
    /// Counters that hold zero and are not `suspect`.
    pub zero_counters: u64,
    /// How many counters hold each count.
    #[serde(default)]
    pub refs_histogram: RefsHistogram,
    /// The atoms with the most references, most first.
    #[serde(default)]
    pub hottest: Vec<HotAtom>,
}

/// How many atom counters hold each count. A counter at zero is not here: it
/// is in `zero_counters`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RefsHistogram {
    pub one: u64,
    pub two: u64,
    pub three_to_nine: u64,
    pub ten_to_ninety_nine: u64,
    pub hundred_or_more: u64,
}

/// One atom with a high count.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HotAtom {
    pub atom: String,
    pub refs: u64,
}

/// How many hot atoms the inventory lists.
const HOTTEST_ATOMS: usize = 5;

impl AtomCounterTotals {
    /// Fold the count of one counter into the histogram and the hot list.
    fn note_refs(&mut self, atom: Option<&str>, refs: u64) {
        let bucket = match refs {
            0 => return,
            1 => &mut self.refs_histogram.one,
            2 => &mut self.refs_histogram.two,
            3..=9 => &mut self.refs_histogram.three_to_nine,
            10..=99 => &mut self.refs_histogram.ten_to_ninety_nine,
            _ => &mut self.refs_histogram.hundred_or_more,
        };
        *bucket += 1;
        let Some(atom) = atom else {
            return;
        };
        let full = self.hottest.len() >= HOTTEST_ATOMS;
        if full && self.hottest.last().is_some_and(|last| last.refs >= refs) {
            return;
        }
        let at = self.hottest.partition_point(|hot| hot.refs >= refs);
        self.hottest.insert(
            at,
            HotAtom {
                atom: atom.to_string(),
                refs,
            },
        );
        self.hottest.truncate(HOTTEST_ATOMS);
    }
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
    /// Counter facts of the same walk.
    #[serde(default)]
    pub atom_counters: AtomCounterTotals,
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

        // Single pass over canonical atom rows so logical byte totals are
        // not double-counted by the schema secondary index copies.
        // Every body is one record at `atom:{uuid}\0b`.
        let mut walk = BreakdownWalk::new(schema_names);
        let colon = build_storage_key(storage_prefix, "atom:");
        let (scan_prefix, scan_end) = crate::kind_partition::colon_plane_bounds(&colon);

        // `atom:` is every field value in the database, and each row is decoded
        // into an `Atom` that is larger resident than the bytes it came from.
        // Reading the whole prefix therefore cost roughly the atom keyspace
        // twice over — on a live node, for a per-schema total. Page instead, so
        // only one bounded page and one decoded atom are held at a time. See
        // `INVENTORY_PAGE_BYTE_BUDGET` for the budget these pages aim at. Each
        // read is also bounded by the read budget of one call, whatever the
        // range holds (see `RangeWalk`).
        let mut range = RangeWalk::new(
            scan_prefix.as_bytes(),
            scan_end.as_bytes(),
            INVENTORY_PAGE_ROWS_MIN,
        );
        while !range.done() {
            let rows = self
                .walk_step(&mut range)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("storage_breakdown scan: {e}")))?
                .rows;
            let mut page_bytes = 0u64;
            for (k, v) in &rows {
                page_bytes += (k.len() + v.len()) as u64;
                fold_breakdown_row(&mut walk, k, v)?;
            }
            if !rows.is_empty() {
                range.set_limit(inventory_next_page_rows(page_bytes, rows.len()));
            }
        }

        let BreakdownWalk {
            agg,
            sizes,
            mut counters,
            waiting_body,
            ..
        } = walk;
        if waiting_body.is_some() {
            counters.bodies_without_counter += 1;
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
            atom_counters: counters,
        })
    }
}

/// State of one inventory walk over the `atom:` records.
struct BreakdownWalk<'a> {
    wanted: std::collections::HashSet<&'a str>,
    agg: std::collections::HashMap<String, (u64, u64)>,
    sizes: Vec<u64>,
    counters: AtomCounterTotals,
    /// The uuid of a body that still waits for its counter record.
    waiting_body: Option<String>,
}

impl<'a> BreakdownWalk<'a> {
    fn new(schema_names: &'a [String]) -> Self {
        Self {
            wanted: schema_names.iter().map(String::as_str).collect(),
            agg: std::collections::HashMap::new(),
            sizes: Vec::new(),
            counters: AtomCounterTotals::default(),
            waiting_body: None,
        }
    }
}

/// Fold one `atom:` record into the walk: a counter into the totals, a
/// body into the per-schema sizes.
fn fold_breakdown_row(walk: &mut BreakdownWalk<'_>, k: &[u8], v: &[u8]) -> Result<(), SchemaError> {
    let key = String::from_utf8_lossy(k);
    if crate::atom::atom_key_codec::is_counter_key(&key) {
        note_counter_row(&mut walk.counters, &mut walk.waiting_body, &key, v);
        return Ok(());
    }
    if walk.waiting_body.take().is_some() {
        walk.counters.bodies_without_counter += 1;
    }
    walk.waiting_body = crate::atom::atom_key_codec::body_uuid_of(&key).map(str::to_string);
    let atom = decode_plain_atom(v).map_err(|e| {
        SchemaError::InvalidData(format!(
            "storage_breakdown scan: failed to decode {}: {e}",
            String::from_utf8_lossy(k)
        ))
    })?;
    let name = atom.source_schema_name();
    if !walk.wanted.is_empty() && !walk.wanted.contains(name) {
        return Ok(());
    }
    let b = serde_json::to_vec(&atom).map_or(0, |v| v.len() as u64);
    walk.sizes.push(b);
    let entry = walk.agg.entry(name.to_string()).or_default();
    entry.0 += b;
    entry.1 += 1;
    Ok(())
}

/// Fold one counter record into the totals and settle the body it follows.
fn note_counter_row(
    totals: &mut AtomCounterTotals,
    waiting_body: &mut Option<String>,
    key: &str,
    value: &[u8],
) {
    totals.counters += 1;
    let paired = crate::atom::atom_key_codec::uuid_of(key)
        .is_some_and(|uuid| waiting_body.as_deref() == Some(uuid));
    if paired {
        *waiting_body = None;
    } else {
        totals.counters_without_body += 1;
        if waiting_body.take().is_some() {
            totals.bodies_without_counter += 1;
        }
    }
    let Ok(counter) =
        serde_json::from_slice::<crate::db_operations::atom_store::counter::AtomCounter>(value)
    else {
        return;
    };
    totals.refs_total = totals.refs_total.saturating_add(counter.refs);
    totals.max_refs = totals.max_refs.max(counter.refs);
    totals.note_refs(crate::atom::atom_key_codec::uuid_of(key), counter.refs);
    if counter.suspect {
        totals.suspect += 1;
    } else if counter.refs == 0 {
        totals.zero_counters += 1;
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
