//! Range-mode keyed filter application.

use crate::schema::types::field::{HashRangeFilter, HashRangeFilterResult, SAMPLE_PEEK_CAP};
use crate::schema::types::key_value::KeyValue;
use std::collections::HashMap;

use super::{FilterUtils, DEFAULT_UNFILTERED_PAGE_LIMIT};

fn insert_range(m: &mut HashMap<KeyValue, String>, key: String, uuid: String) {
    m.insert(KeyValue::new(None, Some(key)), uuid);
}

fn extend_range<I: IntoIterator<Item = (String, String)>>(
    m: &mut HashMap<KeyValue, String>,
    iter: I,
) {
    for (k, v) in iter {
        insert_range(m, k, v);
    }
}

/// Apply a [`HashRangeFilter`] to a [`MoleculeRange`].
#[allow(
    clippy::match_same_arms,
    reason = "arms are grouped by filter category (range / hash-range / hash-only) with explanatory comments; the hash-range variants intentionally mirror their range counterparts for a RangeField, so merging would scramble the documented grouping"
)]
pub fn apply_range_filter(
    molecule: &crate::atom::MoleculeHashRange,
    optional_filter: Option<HashRangeFilter>,
) -> HashRangeFilterResult {
    // No filter ⇒ list the first page of records. This used to default to
    // `SampleN(100)`, but SampleN is now a peek capped at SAMPLE_PEEK_CAP, so the
    // bulk default has to be the real list primitive (`Page`). The node handler
    // overrides this with the caller's offset/limit; this default only governs
    // direct core callers, preserving the historical "up to 100 unfiltered" cap.
    let filter = optional_filter.unwrap_or(HashRangeFilter::Page {
        offset: 0,
        limit: DEFAULT_UNFILTERED_PAGE_LIMIT,
    });
    let mut matches = HashMap::new();

    match filter {
        HashRangeFilter::SampleN(n) => {
            // Peek only — clamp to SAMPLE_PEEK_CAP so SampleN can't be a bulk loader.
            extend_range(
                &mut matches,
                range_all_atoms(molecule)
                    .into_iter()
                    .take(n.min(SAMPLE_PEEK_CAP)),
            );
        }
        HashRangeFilter::Page { offset, limit } => {
            // Bounded paginated fetch: stable-sorted keys [offset, offset+limit).
            extend_range(
                &mut matches,
                range_all_atoms(molecule)
                    .into_iter()
                    .skip(offset)
                    .take(limit),
            );
        }
        HashRangeFilter::PageAfter { after, limit } => {
            let after_range = after.range.as_deref().unwrap_or("");
            extend_range(
                &mut matches,
                range_all_atoms(molecule)
                    .into_iter()
                    .filter(|(key, _)| key.as_str() > after_range)
                    .take(limit),
            );
        }
        HashRangeFilter::HashKey(key) | HashRangeFilter::RangeKey(key) => {
            if let Some(uuid) = molecule.get_atom_uuid("", &key).cloned() {
                insert_range(&mut matches, key, uuid);
            }
        }
        HashRangeFilter::RangePrefix(prefix) => {
            extend_range(&mut matches, range_atoms_with_prefix(molecule, &prefix));
        }
        HashRangeFilter::RangeRange { start, end } => {
            extend_range(&mut matches, range_atoms_in_range(molecule, &start, &end));
        }
        HashRangeFilter::HashRangeKeys(keys) => {
            for (_hash, range) in keys {
                if let Some(uuid) = molecule.get_atom_uuid("", &range).cloned() {
                    insert_range(&mut matches, range, uuid);
                }
            }
        }
        HashRangeFilter::RangePattern(_pattern) => {
            // Pattern matching not supported - return empty results
        }
        // Hash-range specific filters - RangeField only handles range keys, so ignore hash components
        HashRangeFilter::HashRangeKey { range, .. } => {
            if let Some(uuid) = molecule.get_atom_uuid("", &range).cloned() {
                insert_range(&mut matches, range, uuid);
            }
        }
        HashRangeFilter::HashRangePrefix { prefix, .. } => {
            extend_range(&mut matches, range_atoms_with_prefix(molecule, &prefix));
        }
        HashRangeFilter::HashRangeRange { start, end, .. } => {
            extend_range(&mut matches, range_atoms_in_range(molecule, &start, &end));
        }
        HashRangeFilter::HashRangePattern {
            pattern: _pattern, ..
        } => {
            // Pattern matching not supported - return empty results
        }
        // Hash-only filters - RangeField doesn't handle hash keys, return empty
        HashRangeFilter::HashPattern(_) => {
            // RangeField doesn't handle hash patterns
        }
        HashRangeFilter::HashRange { .. } => {
            // RangeField doesn't handle hash ranges
        }
    }

    HashRangeFilterResult::new(matches)
}

/// All `(key, atom_uuid)` pairs in a [`MoleculeRange`].
fn range_all_atoms(m: &crate::atom::MoleculeHashRange) -> Vec<(String, String)> {
    // Range-only unified layout stores entries at ("", range).
    m.iter_all_atoms()
        .map(|(_h, r, uuid)| (r.clone(), uuid.clone()))
        .collect()
}

/// `(key, atom_uuid)` pairs of a [`MoleculeRange`] whose key falls in `start..end`.
///
/// `start > end` is treated as an empty range: `BTreeMap::range(start..end)`
/// panics with "range start is greater than range end in BTreeMap" on
/// inverted bounds, but the filter is reached through user-controllable
/// query input — a malformed `RangeRange` (e.g. an LLM-emitted query with
/// a swapped date range) must yield zero rows, not crash the worker.
fn range_atoms_in_range(
    m: &crate::atom::MoleculeHashRange,
    start: &str,
    end: &str,
) -> Vec<(String, String)> {
    if start > end {
        return Vec::new();
    }
    // Range-only slots live under the empty hash partition.
    let Some(ranges) = m.get_atoms_for_hash("") else {
        return Vec::new();
    };
    ranges
        .range(start.to_string()..end.to_string())
        .map(|(key, uuid)| (key.clone(), uuid.clone()))
        .collect()
}

/// `(key, atom_uuid)` pairs of a [`MoleculeRange`] whose key starts with `prefix`.
///
/// An empty `prefix` matches every key, and is handled directly: the
/// `start..create_prefix_end()` range trick cannot express "match every key"
/// (no string upper bound is greater than all strings), so feeding it an
/// empty prefix would wrongly return only the empty-string key.
fn range_atoms_with_prefix(
    m: &crate::atom::MoleculeHashRange,
    prefix: &str,
) -> Vec<(String, String)> {
    if prefix.is_empty() {
        return range_all_atoms(m);
    }
    range_atoms_in_range(m, prefix, &FilterUtils::create_prefix_end(prefix))
}
