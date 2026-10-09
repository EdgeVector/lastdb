//! Hash-mode keyed filter application.

use crate::schema::types::field::{HashRangeFilter, HashRangeFilterResult, SAMPLE_PEEK_CAP};
use crate::schema::types::key_value::KeyValue;
use std::collections::HashMap;

use super::DEFAULT_UNFILTERED_PAGE_LIMIT;

fn insert_hash(m: &mut HashMap<KeyValue, String>, key: String, uuid: String) {
    m.insert(KeyValue::new(Some(key), None), uuid);
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
        HashRangeFilter::HashKey(key) => {
            if let Some(uuid) = molecule.get_atom_uuid(&key, "").cloned() {
                insert_hash(&mut matches, key, uuid);
            }
        }
        // `HashRange { start, end }` is documented as "for hash values"
        // — the same dimension a Hash field stores. PR #381 closed this
        // for `MoleculeHashRange`; the analogue on `MoleculeHash` used
        // to fall through to the catch-all and return every atom, which
        // silently widened `HashRange { "a", "c" }` into a full scan
        // for any caller (LLM-built query, dev UI, …) routing a hash
        // range over a pure-Hash field. Filter on the hash dimension
        // here. `start > end` is treated as an empty range — same
        // pattern as `range_atoms_in_range` / `hash_range_atoms_in_range`
        // — so a malformed bound from user-controllable query input
        // yields zero matches without falling through to "all".
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
        // `HashRangeKey { hash, range }` and `HashRangeKeys([(hash, range), …])`
        // each carry a real `hash` component — the dimension a Hash field
        // stores. The previous catch-all dropped both into "_ => return
        // every atom", so an LLM-built query routing a hash-range key
        // onto a pure-Hash schema (the LLM doesn't always know which
        // variant the schema declares) silently widened the result to a
        // full scan instead of looking up the requested key. Look up by
        // the hash dimension here; the unused `range` is ignored, mirror
        // of how `apply_range_filter` drops the unused `hash` on its own
        // HashRangeKey/HashRangeKeys arms (filter_utils.rs:303-309).
        // `HashRangeKey { hash, range }` and `HashRangeKeys([(hash, range), …])`
        // each carry a real `hash` component — the dimension a Hash field
        // stores. The previous catch-all dropped both into "_ => return
        // every atom", so an LLM-built query routing a hash-range key
        // onto a pure-Hash schema (the LLM doesn't always know which
        // variant the schema declares) silently widened the result to a
        // full scan instead of looking up the requested key. Look up by
        // the hash dimension here; the unused `range` is ignored, mirror
        // of how `apply_range_filter` drops the unused `hash` on its own
        // HashRangeKey/HashRangeKeys arms (filter_utils.rs:303-309).
        HashRangeFilter::HashRangeKey { hash, .. } => {
            if let Some(uuid) = molecule.get_atom_uuid(&hash, "").cloned() {
                insert_hash(&mut matches, hash, uuid);
            }
        }
        HashRangeFilter::HashRangeKeys(keys) => {
            for (hash, _range) in keys {
                if let Some(uuid) = molecule.get_atom_uuid(&hash, "").cloned() {
                    insert_hash(&mut matches, hash, uuid);
                }
            }
        }
        // `HashRangePrefix { hash, prefix }` carries a real `hash`
        // component — the dimension a Hash field stores — so it must
        // narrow to that single hash rather than falling through to the
        // catch-all. Same shape as PR #478's `HashRangeKey`/`HashRangeKeys`
        // arms: the LLM prompt advertises `HashRangePrefix` for HashRange
        // schemas but LLM-built queries don't always classify the schema
        // correctly and will route it at a Hash schema. The `prefix` is
        // meaningless on the hash dimension (a Hash field has no range to
        // prefix-match), so drop it — mirror of how `apply_range_filter`'s
        // `HashRangePrefix` arm drops the unused `hash`. Before this arm
        // the catch-all silently widened every such query to a full
        // molecule scan.
        HashRangeFilter::HashRangePrefix { hash, .. } => {
            if let Some(uuid) = molecule.get_atom_uuid(&hash, "").cloned() {
                insert_hash(&mut matches, hash, uuid);
            }
        }
        // `HashPattern(_)` is the LLM-advertised glob filter for Hash
        // schemas (historical LLM query-planner filter). Glob matching against
        // hash keys is not implemented — the sibling dispatchers
        // `apply_range_filter` and `apply_hash_range_filter` both treat
        // it as "pattern matching not supported → empty". Without this
        // arm the filter fell into the catch-all "return everything" and
        // an LLM-built query like "authors named Liu" with pattern
        // `*Liu*` silently widened to a full molecule scan, returning
        // every record instead of the unsupported-empty signal.
        HashRangeFilter::HashPattern(_) => {}
        // `RangePattern(_)` is the LLM-advertised glob filter for Range
        // schemas. Both sibling dispatchers (`apply_range_filter` and
        // `apply_hash_range_filter`) already treat it as "pattern matching
        // not supported → empty"; the Hash-field path was the outlier and
        // fell through to the catch-all "return everything" arm. The LLM
        // prompt restricts `RangePattern` to Range schemas but LLM-built
        // queries don't always classify the schema correctly and will
        // route a `RangePattern` at a Hash schema — before this arm such
        // a query (e.g. "tags matching `*foo*`" against a Hash-keyed
        // schema) silently widened to a full molecule scan. Direct
        // sibling of PR #482 (`HashPattern`).
        HashRangeFilter::RangePattern(_) => {}
        // `HashRangePattern { hash, pattern }` is the LLM-advertised glob
        // filter for HashRange schemas. The sibling dispatcher
        // `apply_hash_range_filter` treats it as "pattern matching not
        // supported → empty"; `apply_range_filter` does the same. The
        // Hash-field path was the last outlier — without this arm it fell
        // through to the catch-all "return every atom" arm, so an
        // LLM-built `HashRangePattern { hash: "user1", pattern: "*foo*" }`
        // misrouted at a Hash-keyed schema silently widened to a full
        // molecule scan instead of producing the unsupported-empty signal.
        // Same shape as PR #482 (`HashPattern`) and the sibling
        // `RangePattern` arm above; closes the last pattern-variant gap in
        // `apply_hash_filter`.
        HashRangeFilter::HashRangePattern { .. } => {}
        // `HashRangeRange { hash, start, end }` carries a real `hash`
        // component — the dimension a Hash field stores — so it must
        // narrow to that single hash rather than falling through to the
        // catch-all "return every atom" arm. Same shape as
        // `HashRangeKey { hash, .. }` (PR #478) and
        // `HashRangePrefix { hash, .. }` (PR #481): the LLM prompt
        // advertises `HashRangeRange` for HashRange schemas but LLM-built
        // queries don't always classify the schema correctly and will
        // route a `HashRangeRange` at a Hash schema. The `start..end`
        // bounds are meaningless on the hash dimension (a Hash field has
        // no inner range to filter), so drop them — mirror of how
        // `apply_range_filter`'s `HashRangeRange` arm drops the unused
        // `hash`. Before this arm the catch-all silently widened every
        // such query to a full molecule scan.
        HashRangeFilter::HashRangeRange { hash, .. } => {
            if let Some(uuid) = molecule.get_atom_uuid(&hash, "").cloned() {
                insert_hash(&mut matches, hash, uuid);
            }
        }
        // The pure range-dimension filters (`RangeKey`, `RangePrefix`,
        // `RangeRange`) carry only a range-key payload — no hash component
        // to narrow to — and a Hash field has no range dimension to match
        // against. The sibling pattern arms (`HashPattern`,
        // `RangePattern`, `HashRangePattern`) already treat this same kind
        // of structural misroute as "unsupported → empty" (PR #482 / #519);
        // before these arms, the catch-all `_ => extend_hash(hash_all_atoms)`
        // silently widened every such query to a full molecule scan.
        // LLM-built queries don't always classify the schema variant
        // ahead of time and can route `RangeKey("user@x.com")` /
        // `RangePrefix("2025-")` / `RangeRange { "2025-01-01", "2025-12-31" }`
        // at a Hash schema — closing this gap makes the misroute surface as
        // "no results" instead of "every result", mirroring the pattern arms.
        HashRangeFilter::RangeKey(_)
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
