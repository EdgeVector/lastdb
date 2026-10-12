//! Hash-mode keyed filter application.

use crate::schema::types::field::{HashRangeFilter, HashRangeFilterResult, SAMPLE_PEEK_CAP};
use crate::schema::types::key_value::KeyValue;
use std::collections::HashMap;

use super::DEFAULT_UNFILTERED_PAGE_LIMIT;

fn insert_hash(m: &mut HashMap<KeyValue, String>, key: String, uuid: String) {
    m.insert(KeyValue::new(Some(key), None), uuid);
}

fn insert_hash_lookup(
    m: &mut HashMap<KeyValue, String>,
    molecule: &crate::atom::MoleculeHashRange,
    hash: String,
) {
    if let Some(uuid) = molecule.get_atom_uuid(&hash, "").cloned() {
        insert_hash(m, hash, uuid);
    }
}

fn extend_hash<I: IntoIterator<Item = (String, String)>>(
    m: &mut HashMap<KeyValue, String>,
    iter: I,
) {
    for (k, v) in iter {
        insert_hash(m, k, v);
    }
}

/// Apply a [`HashRangeFilter`] to a [`MoleculeHash`] (single hash key, no range).
#[allow(
    clippy::match_same_arms,
    reason = "hash-bearing arms (HashRangeKey/HashRangePrefix) intentionally narrow by the same hash-lookup body but are kept separate so each carries the per-variant doc comment that motivated its fix (mirrors the same allow on apply_range_filter); merging would scramble the documented grouping"
)]
pub fn apply_hash_filter(
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
            extend_hash(
                &mut matches,
                hash_all_atoms(molecule)
                    .into_iter()
                    .take(n.min(SAMPLE_PEEK_CAP)),
            );
        }
        HashRangeFilter::Page { offset, limit } => {
            // Bounded paginated fetch: stable-sorted keys [offset, offset+limit).
            extend_hash(
                &mut matches,
                hash_all_atoms(molecule)
                    .into_iter()
                    .skip(offset)
                    .take(limit),
            );
        }
        HashRangeFilter::PageAfter { after, limit } => {
            let after_hash = after.hash.as_deref().unwrap_or("");
            extend_hash(
                &mut matches,
                hash_all_atoms(molecule)
                    .into_iter()
                    .filter(|(key, _)| key.as_str() > after_hash)
                    .take(limit),
            );
        }
        HashRangeFilter::HashKey(key) => insert_hash_lookup(&mut matches, molecule, key),
        // `HashRange { start, end }` filters on the hash dimension. `start > end`
        // is an empty range (as in `range_atoms_in_range`), so a malformed bound
        // from query input yields zero matches instead of falling through to "all".
        HashRangeFilter::HashRange { start, end } => {
            if start <= end {
                extend_hash(
                    &mut matches,
                    hash_all_atoms(molecule).into_iter().filter(|(key, _)| {
                        key.as_str() >= start.as_str() && key.as_str() < end.as_str()
                    }),
                );
            }
        }
        // Variants that carry a real `hash` component narrow to that hash lookup.
        // The range part (`range`, `prefix`, `start..end`) is meaningless on a
        // Hash field and is dropped, as `apply_range_filter` drops the unused
        // `hash`. A query routed at the wrong schema variant must not widen to
        // a full molecule scan.
        HashRangeFilter::HashRangeKey { hash, .. }
        | HashRangeFilter::HashRangePrefix { hash, .. }
        | HashRangeFilter::HashRangeRange { hash, .. } => {
            insert_hash_lookup(&mut matches, molecule, hash);
        }
        HashRangeFilter::HashRangeKeys(keys) => {
            for (hash, _range) in keys {
                insert_hash_lookup(&mut matches, molecule, hash);
            }
        }
        // Glob patterns and pure range-dimension filters are unsupported on a
        // Hash field: the sibling dispatchers `apply_range_filter` and
        // `apply_hash_range_filter` return empty, and so does this one. Falling
        // through to "return everything" would silently widen a misrouted
        // query (e.g. `*Liu*` or `RangePrefix("2025-")`) into a full scan.
        HashRangeFilter::HashPattern(_)
        | HashRangeFilter::RangePattern(_)
        | HashRangeFilter::HashRangePattern { .. }
        | HashRangeFilter::RangeKey(_)
        | HashRangeFilter::RangePrefix(_)
        | HashRangeFilter::RangeRange { .. } => {}
    }

    HashRangeFilterResult::new(matches)
}

/// All `(key, atom_uuid)` pairs in a [`MoleculeHash`], sorted by key.
///
/// The sort is load-bearing for paginated reads via
/// [`HashRangeFilter::SampleN`]. `MoleculeHash::atom_uuids` is a
/// [`std::collections::HashMap`] whose iteration order is reseeded per
/// instance by `RandomState`, and `refresh_from_db` deserializes a fresh
/// instance on every query — so an unsorted `…iter().take(n)` returned
/// a different subset of records on each call. Two `/api/query` requests
/// with `filter = SampleN(n)` against a Hash schema then saw two
/// different subsets, and `skip(offset).take(limit)` at the handler
/// overlapped (duplicate rows) and dropped (gapped rows) across pages.
/// Sorting once here gives every caller a deterministic, prefix-stable
/// list — matching the natural sort order of [`range_all_atoms`] (its
/// backing `BTreeMap` iterates sorted) and the key order of
/// [`MoleculeHashRange::sample`] (range, then hash).
fn hash_all_atoms(m: &crate::atom::MoleculeHashRange) -> Vec<(String, String)> {
    // Hash-only unified layout stores entries at (hash, "").
    let mut out: Vec<(String, String)> = m
        .iter_all_atoms()
        .map(|(h, _r, uuid)| (h.clone(), uuid.clone()))
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}
