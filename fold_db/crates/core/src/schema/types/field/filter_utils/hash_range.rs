//! Hash+range-mode keyed filter application.

use crate::atom::MoleculeHashRange;
use crate::schema::types::field::{HashRangeFilter, HashRangeFilterResult, SAMPLE_PEEK_CAP};
use crate::schema::types::key_value::KeyValue;
use std::collections::HashMap;

use super::{FilterUtils, DEFAULT_UNFILTERED_PAGE_LIMIT};

fn key_tuple_after(key: &KeyValue, after: &KeyValue) -> bool {
    (
        key.range.as_deref().unwrap_or(""),
        key.hash.as_deref().unwrap_or(""),
    ) > (
        after.range.as_deref().unwrap_or(""),
        after.hash.as_deref().unwrap_or(""),
    )
}

fn insert_hash_range(m: &mut HashMap<KeyValue, String>, hash: String, range: String, uuid: String) {
    m.insert(KeyValue::new(Some(hash), Some(range)), uuid);
}

fn extend_hash_range<I: IntoIterator<Item = (String, String)>>(
    m: &mut HashMap<KeyValue, String>,
    hash: &str,
    iter: I,
) {
    for (rk, uuid) in iter {
        insert_hash_range(m, hash.to_string(), rk, uuid);
    }
}

/// Apply a [`HashRangeFilter`] to a [`MoleculeHashRange`].
pub fn apply_hash_range_filter(
    molecule: &MoleculeHashRange,
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
            for key_value in molecule.sample(n.min(SAMPLE_PEEK_CAP)) {
                if let (Some(hash), Some(range)) = (&key_value.hash, &key_value.range) {
                    if let Some(atom_uuid) = molecule.get_atom_uuid(hash, range).cloned() {
                        matches.insert(key_value, atom_uuid);
                    }
                }
            }
        }
        HashRangeFilter::Page { offset, limit } => {
            // Bounded paginated fetch over the SAME (range, hash) order the
            // handler sorts results by (`sort_results_by_key`) — NOT insertion
            // order — so pages are contiguous (no overlap/drop) across requests.
            let mut all: Vec<KeyValue> = molecule
                .iter_all_atoms()
                .map(|(hash, range, _)| KeyValue::new(Some(hash.clone()), Some(range.clone())))
                .collect();
            all.sort_by(|a, b| {
                a.range
                    .as_deref()
                    .unwrap_or("")
                    .cmp(b.range.as_deref().unwrap_or(""))
                    .then_with(|| {
                        a.hash
                            .as_deref()
                            .unwrap_or("")
                            .cmp(b.hash.as_deref().unwrap_or(""))
                    })
            });
            for key_value in all.into_iter().skip(offset).take(limit) {
                if let (Some(hash), Some(range)) = (&key_value.hash, &key_value.range) {
                    if let Some(atom_uuid) = molecule.get_atom_uuid(hash, range).cloned() {
                        matches.insert(key_value, atom_uuid);
                    }
                }
            }
        }
        HashRangeFilter::PageAfter { after, limit } => {
            let mut all: Vec<KeyValue> = molecule
                .iter_all_atoms()
                .map(|(hash, range, _)| KeyValue::new(Some(hash.clone()), Some(range.clone())))
                .collect();
            all.sort_by(|a, b| {
                a.range
                    .as_deref()
                    .unwrap_or("")
                    .cmp(b.range.as_deref().unwrap_or(""))
                    .then_with(|| {
                        a.hash
                            .as_deref()
                            .unwrap_or("")
                            .cmp(b.hash.as_deref().unwrap_or(""))
                    })
            });
            for key_value in all
                .into_iter()
                .filter(|kv| key_tuple_after(kv, &after))
                .take(limit)
            {
                if let (Some(hash), Some(range)) = (&key_value.hash, &key_value.range) {
                    if let Some(atom_uuid) = molecule.get_atom_uuid(hash, range).cloned() {
                        matches.insert(key_value, atom_uuid);
                    }
                }
            }
        }
        HashRangeFilter::HashRangeKey { hash, range } => {
            if let Some(uuid) = molecule.get_atom_uuid(&hash, &range).cloned() {
                insert_hash_range(&mut matches, hash, range, uuid);
            }
        }
        HashRangeFilter::HashKey(hash) => {
            if let Some(range_atoms) = molecule.get_atoms_for_hash(&hash) {
                extend_hash_range(&mut matches, &hash, range_atoms);
            }
        }
        HashRangeFilter::RangeKey(range) => {
            for hash_value in molecule.hash_values() {
                if let Some(uuid) = molecule.get_atom_uuid(hash_value, &range).cloned() {
                    insert_hash_range(&mut matches, hash_value.clone(), range.clone(), uuid);
                }
            }
        }
        HashRangeFilter::HashRangePrefix { hash, prefix } => {
            extend_hash_range(
                &mut matches,
                &hash,
                hash_range_atoms_with_prefix(molecule, &hash, &prefix),
            );
        }
        HashRangeFilter::RangePrefix(prefix) => {
            for hash_value in molecule.hash_values() {
                extend_hash_range(
                    &mut matches,
                    hash_value,
                    hash_range_atoms_with_prefix(molecule, hash_value, &prefix),
                );
            }
        }
        HashRangeFilter::HashRangeRange { hash, start, end } => {
            extend_hash_range(
                &mut matches,
                &hash,
                hash_range_atoms_in_range(molecule, &hash, &start, &end),
            );
        }
        HashRangeFilter::RangeRange { start, end } => {
            for hash_value in molecule.hash_values() {
                extend_hash_range(
                    &mut matches,
                    hash_value,
                    hash_range_atoms_in_range(molecule, hash_value, &start, &end),
                );
            }
        }
        HashRangeFilter::HashRangeKeys(keys) => {
            for (hash, range) in keys {
                if let Some(uuid) = molecule.get_atom_uuid(&hash, &range).cloned() {
                    insert_hash_range(&mut matches, hash, range, uuid);
                }
            }
        }
        HashRangeFilter::HashRangePattern {
            hash: _,
            pattern: _pattern,
        } => {
            // Pattern matching not supported - return empty results
        }
        HashRangeFilter::RangePattern(_pattern) => {
            // Pattern matching not supported - return empty results
        }
        HashRangeFilter::HashPattern(_pattern) => {
            // Pattern matching not supported - return empty results
        }
        HashRangeFilter::HashRange { start, end } => {
            for (hash_value, range_key, uuid) in
                hash_range_atoms_in_hash_range(molecule, &start, &end)
            {
                insert_hash_range(&mut matches, hash_value, range_key, uuid);
            }
        }
    }

    HashRangeFilterResult::new(matches)
}

/// `(range, atom_uuid)` pairs for `hash` whose range falls in `start..end`.
///
/// `start > end` returns an empty Vec for the same reason as
/// [`range_atoms_in_range`]: the underlying `BTreeMap::range` panics on
/// inverted bounds, and a malformed `HashRangeRange`/`RangeRange` filter
/// reaches here through user-controllable query input.
fn hash_range_atoms_in_range(
    m: &MoleculeHashRange,
    hash: &str,
    start: &str,
    end: &str,
) -> Vec<(String, String)> {
    if start > end {
        return Vec::new();
    }
    m.get_atoms_for_hash(hash)
        .map(|range_map| {
            range_map
                .range(start.to_string()..end.to_string())
                .map(|(range, uuid)| (range.clone(), uuid.clone()))
                .collect()
        })
        .unwrap_or_default()
}

/// `(range, atom_uuid)` pairs for `hash` whose range starts with `prefix`.
///
/// An empty `prefix` matches every range key under `hash` (see
/// [`range_atoms_with_prefix`] for why the empty case is special-cased).
fn hash_range_atoms_with_prefix(
    m: &MoleculeHashRange,
    hash: &str,
    prefix: &str,
) -> Vec<(String, String)> {
    if prefix.is_empty() {
        return m
            .get_atoms_for_hash(hash)
            .map(|range_map| range_map.into_iter().collect())
            .unwrap_or_default();
    }
    hash_range_atoms_in_range(m, hash, prefix, &FilterUtils::create_prefix_end(prefix))
}

/// `(hash, range, atom_uuid)` triples for every hash in `start..end`
/// (inclusive start, exclusive end), spanning every range entry under
/// each matching hash.
///
/// Backs [`HashRangeFilter::HashRange`], which the enum documents as
/// "for hash values" — the outer hash dimension of a
/// [`MoleculeHashRange`], not the inner range dimension. The previous
/// implementation iterated every hash unfiltered and passed `start..end`
/// to `hash_range_atoms_in_range`, which filters the *range* dimension —
/// so a query like `HashRange { start: "a", end: "c" }` either returned
/// nothing (when range keys fell outside `start..end`) or returned
/// matches across unrelated hashes (when range keys happened to fall
/// inside it). Filter the hash dimension here and take each matching
/// hash's full range map verbatim.
fn hash_range_atoms_in_hash_range(
    m: &MoleculeHashRange,
    start: &str,
    end: &str,
) -> Vec<(String, String, String)> {
    let mut out = Vec::new();
    for hash in m.hash_values() {
        let h = hash.as_str();
        if h < start || h >= end {
            continue;
        }
        if let Some(range_map) = m.get_atoms_for_hash(hash) {
            for (range, uuid) in range_map {
                out.push((hash.clone(), range, uuid));
            }
        }
    }
    out
}
