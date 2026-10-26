//! Conflict response rules. No host, storage, environment, or task access.

use std::collections::HashSet;

use serde_json::Value;

/// Default per-molecule fan-out for the disabled home-index fallback.
/// The width bounds storage work independently of the response's field count.
const CONFLICT_LOOKUP_CONCURRENCY: usize = 16;

/// A missing, invalid, or zero override keeps the default. Zero would stall
/// `buffer_unordered`; one keeps the legacy serial fallback available.
pub(super) fn resolve_lookup_concurrency(raw: Option<&str>) -> usize {
    raw.and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(CONFLICT_LOOKUP_CONCURRENCY)
}

/// Deduplicate field molecules before the fallback reads each one once.
pub(super) fn collect_molecule_uuids(results: &[Value]) -> HashSet<String> {
    let mut molecules = HashSet::new();
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

/// Named conflicts are true. With incomplete coverage, every other named
/// molecule is unknown. Complete coverage leaves unflagged metadata untouched;
/// this preserves the response shape without writing false or removing fields.
pub(super) fn stamp_conflict_flags(
    results: &mut [Value],
    molecules: &HashSet<String>,
    complete: bool,
) -> &'static str {
    if !complete || !molecules.is_empty() {
        for row in results {
            let Some(metadata) = row.get_mut("metadata").and_then(Value::as_object_mut) else {
                continue;
            };
            for field_meta in metadata.values_mut() {
                let Some(mol) = field_meta.get("molecule_uuid").and_then(Value::as_str) else {
                    continue;
                };
                let flag = if molecules.contains(mol) {
                    Value::Bool(true)
                } else if !complete {
                    Value::String("unknown".to_string())
                } else {
                    continue;
                };
                if let Some(obj) = field_meta.as_object_mut() {
                    obj.insert("has_conflicts".to_string(), flag);
                }
            }
        }
    }
    if complete {
        "known"
    } else {
        "unknown"
    }
}
