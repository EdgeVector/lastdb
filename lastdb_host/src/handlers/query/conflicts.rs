//! Themed module split from the parent.

use super::*;

/// Bounded fan-out for the per-molecule conflict lookups in
/// [`annotate_conflict_flags`].
///
/// **Size this against the real number, which is small.** A molecule is
/// per-FIELD, not per-row, so a full-schema read touches exactly as many
/// molecules as the schema has fields — regardless of row count. Measured on
/// the primary 2026-07-30: a 1,219-row board read has 14,826 field-metadata
/// entries but only **18 distinct molecules**. The dedup below (like the memo it
/// replaces) collapses 14,826 lookups to 18, so this fan-out saves at most 17
/// round trips, not thousands.
///
/// It is still worth bounding rather than serializing. Each lookup is a
/// latency-bound point read while the `mcc:{M}` id-list cache is warm, but a
/// cache MISS falls through to a 1,024-group prefix sweep — and 18 of those
/// awaited one at a time is seconds, not milliseconds. Sixteen in flight caps
/// that tail without handing the storage layer a burst proportional to
/// result-set size: the QoS gate already caps concurrent bulk operations at 8,
/// so the read path stays bounded at `bulk_permits × 16` in-flight gets.
///
/// Override with `LASTDB_CONFLICT_LOOKUP_CONCURRENCY` (same escape-hatch idiom
/// as `LASTDB_QOS_TOTAL`). `1` restores the pre-fan-out serial behaviour.
pub(in crate::handlers) const CONFLICT_LOOKUP_CONCURRENCY: usize = 16;

/// Resolved fan-out width: the env override when it parses to a positive value,
/// else [`CONFLICT_LOOKUP_CONCURRENCY`]. `0` would stall `buffer_unordered`
/// forever, so it is rejected along with unparseable values.
pub(in crate::handlers) fn conflict_lookup_concurrency() -> usize {
    resolve_lookup_concurrency(
        std::env::var("LASTDB_CONFLICT_LOOKUP_CONCURRENCY")
            .ok()
            .as_deref(),
    )
}

/// The override parse, split out pure so it is tested without mutating
/// process-global env (which no test can serialize against a live node).
pub(in crate::handlers) fn resolve_lookup_concurrency(raw: Option<&str>) -> usize {
    raw.and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(CONFLICT_LOOKUP_CONCURRENCY)
}

/// Stamp `has_conflicts` and return the page field `conflict_flags`.
///
/// One point get of `hcu:mols`. When that stamp exists, one event range for
/// seq greater than `folded_seq`. An absent stamp is `"unknown"` and returns.
/// It does not call `get_unresolved_conflicts(None)`,
/// `home_has_unresolved_conflicts`, or the per-molecule path.
///
/// `LASTDB_HOME_CONFLICT_INDEX_DISABLE` is the only path that still walks
/// `conflict\0{mol}:` once per field molecule. The reads stay unprefixed.
/// Org-prefixed conflicts stay out of this index.
pub(in crate::handlers) async fn annotate_conflict_flags<H: HostNode>(
    host: &H,
    results: &mut [Value],
) -> &'static str {
    let db = host.fold_db();
    match db.db_ops().read_home_conflict_annotation(None).await {
        HomeConflictAnnotation::Disabled => {
            let molecules = collect_molecule_uuids(results);
            if molecules.is_empty() {
                return "known";
            }
            let flags: HashMap<String, bool> = futures::stream::iter(molecules)
                .map(|mol| async move {
                    let flagged = db
                        .db_ops()
                        .get_unresolved_conflicts(Some(&mol), None)
                        .await
                        .is_ok_and(|cs| !cs.is_empty());
                    (mol, flagged)
                })
                .buffer_unordered(conflict_lookup_concurrency())
                .collect()
                .await;
            apply_conflict_flags(results, &flags);
            "known"
        }
        HomeConflictAnnotation::Unknown => {
            apply_unknown_conflict_flags(results);
            "unknown"
        }
        HomeConflictAnnotation::Known { molecules } => {
            if !molecules.is_empty() {
                let flags: HashMap<String, bool> = collect_molecule_uuids(results)
                    .into_iter()
                    .map(|mol| (mol.clone(), molecules.contains(&mol)))
                    .collect();
                apply_conflict_flags(results, &flags);
            }
            "known"
        }
        HomeConflictAnnotation::Incomplete { molecules } => {
            apply_lower_bound_conflict_flags(results, &molecules);
            "unknown"
        }
    }
}

/// Pass 1 — the distinct molecule uuids referenced by any row's field metadata.
/// Deduping here is what subsumes the old per-molecule memo: a uuid is looked up
/// exactly once per query however many fields reference it.
pub(in crate::handlers) fn collect_molecule_uuids(results: &[Value]) -> HashSet<String> {
    let mut molecules: HashSet<String> = HashSet::new();
    for row in results {
        let Some(metadata) = row.get("metadata").and_then(Value::as_object) else {
            continue;
        };
        for field_meta in metadata.values() {
            let Some(mol) = field_meta.get("molecule_uuid").and_then(Value::as_str) else {
                continue;
            };
            if !molecules.contains(mol) {
                molecules.insert(mol.to_string());
            }
        }
    }
    molecules
}

/// Pass 3 — stamp `has_conflicts: true` on every field whose molecule resolved
/// flagged. A uuid that is unflagged, absent, or whose lookup failed leaves the
/// metadata untouched (the key is never written `false`), preserving the
/// pre-existing wire shape.
pub(in crate::handlers) fn apply_conflict_flags(
    results: &mut [Value],
    flags: &HashMap<String, bool>,
) {
    for row in results.iter_mut() {
        let Some(metadata) = row.get_mut("metadata").and_then(Value::as_object_mut) else {
            continue;
        };
        for field_meta in metadata.values_mut() {
            let flagged = field_meta
                .get("molecule_uuid")
                .and_then(Value::as_str)
                .and_then(|mol| flags.get(mol))
                .copied()
                .unwrap_or(false);
            if flagged {
                if let Some(obj) = field_meta.as_object_mut() {
                    obj.insert("has_conflicts".to_string(), Value::Bool(true));
                }
            }
        }
    }
}

pub(in crate::handlers) fn stamp_has_conflicts(field_meta: &mut Value, value: Value) {
    if let Some(obj) = field_meta.as_object_mut() {
        obj.insert("has_conflicts".to_string(), value);
    }
}

/// No stamp. Every field that names a molecule gets the string `"unknown"`.
/// Absent and `false` would both read as clean.
pub(in crate::handlers) fn apply_unknown_conflict_flags(results: &mut [Value]) {
    for row in results.iter_mut() {
        let Some(metadata) = row.get_mut("metadata").and_then(Value::as_object_mut) else {
            continue;
        };
        for field_meta in metadata.values_mut() {
            if field_meta
                .get("molecule_uuid")
                .and_then(Value::as_str)
                .is_none()
            {
                continue;
            }
            stamp_has_conflicts(field_meta, Value::String("unknown".to_string()));
        }
    }
}

/// `complete: false`. A named molecule is `true`. Every other field is the
/// string `"unknown"`, not absent.
pub(in crate::handlers) fn apply_lower_bound_conflict_flags(
    results: &mut [Value],
    molecules: &HashSet<String>,
) {
    for row in results.iter_mut() {
        let Some(metadata) = row.get_mut("metadata").and_then(Value::as_object_mut) else {
            continue;
        };
        for field_meta in metadata.values_mut() {
            let Some(mol) = field_meta.get("molecule_uuid").and_then(Value::as_str) else {
                continue;
            };
            if molecules.contains(mol) {
                stamp_has_conflicts(field_meta, Value::Bool(true));
            } else {
                stamp_has_conflicts(field_meta, Value::String("unknown".to_string()));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Query (POST /api/query) — the paginated read
// ---------------------------------------------------------------------------
