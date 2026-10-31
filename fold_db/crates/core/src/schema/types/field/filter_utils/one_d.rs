//! One-dimensional key selection over the shared HashRange molecule.

use super::DEFAULT_UNFILTERED_PAGE_LIMIT;
use crate::atom::molecule_slot::OneDSlot;
use crate::atom::MoleculeHashRange;
use crate::schema::types::field::{HashRangeFilter, HashRangeFilterResult, SAMPLE_PEEK_CAP};
use crate::schema::types::key_value::KeyValue;
use std::collections::HashMap;
use std::ops::Bound::{Excluded, Included, Unbounded};

type Matches = HashMap<KeyValue, String>;

/// Apply hash-only semantics; slots are `(hash, "")`.
pub fn apply_hash_filter(
    molecule: &MoleculeHashRange,
    filter: Option<HashRangeFilter>,
) -> HashRangeFilterResult {
    apply_one_d(molecule, filter, OneDSlot::Hash)
}

/// Apply range-only semantics; slots are `("", range)`.
pub fn apply_range_filter(
    molecule: &MoleculeHashRange,
    filter: Option<HashRangeFilter>,
) -> HashRangeFilterResult {
    apply_one_d(molecule, filter, OneDSlot::Range)
}

/// Project selectors without widening unsupported or misrouted filters.
fn apply_one_d(
    molecule: &MoleculeHashRange,
    filter: Option<HashRangeFilter>,
    slot: OneDSlot,
) -> HashRangeFilterResult {
    use HashRangeFilter::*;
    let filter = filter.unwrap_or(Page {
        offset: 0,
        limit: DEFAULT_UNFILTERED_PAGE_LIMIT,
    });
    let mut matches = HashMap::new();
    match (slot, filter) {
        (_, SampleN(n)) => page(
            &mut matches,
            molecule,
            slot,
            0,
            n.min(SAMPLE_PEEK_CAP),
            None,
        ),
        (_, Page { offset, limit }) => page(&mut matches, molecule, slot, offset, limit, None),
        (_, PageAfter { after, limit }) => {
            let after = match slot {
                OneDSlot::Hash => after.hash,
                OneDSlot::Range => after.range,
            };
            page(
                &mut matches,
                molecule,
                slot,
                0,
                limit,
                Some(after.as_deref().unwrap_or("")),
            );
        }
        (_, HashKey(key))
        | (OneDSlot::Range, RangeKey(key) | HashRangeKey { range: key, .. })
        | (
            OneDSlot::Hash,
            HashRangeKey { hash: key, .. }
            | HashRangePrefix { hash: key, .. }
            | HashRangeRange { hash: key, .. },
        ) => {
            lookup(&mut matches, molecule, key, slot);
        }
        (_, HashRangeKeys(keys)) => {
            for (hash, range) in keys {
                let key = match slot {
                    OneDSlot::Hash => hash,
                    OneDSlot::Range => range,
                };
                lookup(&mut matches, molecule, key, slot);
            }
        }
        (OneDSlot::Range, RangePrefix(prefix) | HashRangePrefix { prefix, .. }) => {
            extend(&mut matches, range_prefix(molecule, &prefix), slot);
        }
        (OneDSlot::Hash, HashRange { start, end }) => {
            if start <= end {
                extend(
                    &mut matches,
                    all_atoms(molecule, slot)
                        .into_iter()
                        .filter(|(key, _)| key >= &start && key < &end),
                    slot,
                );
            }
        }
        (OneDSlot::Range, RangeRange { start, end } | HashRangeRange { start, end, .. }) => {
            if start <= end {
                if let Some(entries) = molecule.atom_entries_for_hash("") {
                    extend(
                        &mut matches,
                        entries
                            .range::<str, _>((Included(start.as_str()), Excluded(end.as_str())))
                            .map(|(key, entry)| (key.clone(), entry.atom_uuid.clone())),
                        slot,
                    );
                }
            }
        }
        // Patterns are unsupported in both modes. A range selector cannot
        // select a hash field; a hash interval cannot select a range field.
        (_, HashPattern(_) | RangePattern(_) | HashRangePattern { .. })
        | (OneDSlot::Hash, RangeKey(_) | RangePrefix(_) | RangeRange { .. })
        | (OneDSlot::Range, HashRange { .. }) => {}
    }
    HashRangeFilterResult::new(matches)
}

fn page(
    matches: &mut Matches,
    molecule: &MoleculeHashRange,
    slot: OneDSlot,
    offset: usize,
    limit: usize,
    after: Option<&str>,
) {
    extend(
        matches,
        all_atoms(molecule, slot)
            .into_iter()
            .filter(|(key, _)| after.is_none_or(|after| key.as_str() > after))
            .skip(offset)
            .take(limit),
        slot,
    );
}

fn range_prefix(molecule: &MoleculeHashRange, prefix: &str) -> Vec<(String, String)> {
    if prefix.is_empty() {
        return all_atoms(molecule, OneDSlot::Range);
    }
    molecule
        .atom_entries_for_hash("")
        .map(|entries| {
            entries
                .range::<str, _>((Included(prefix), Unbounded))
                .take_while(|(key, _)| key.starts_with(prefix))
                .map(|(key, entry)| (key.clone(), entry.atom_uuid.clone()))
                .collect()
        })
        .unwrap_or_default()
}

fn projected_key(key: String, slot: OneDSlot) -> KeyValue {
    match slot {
        OneDSlot::Hash => KeyValue::new(Some(key), None),
        OneDSlot::Range => KeyValue::new(None, Some(key)),
    }
}

fn lookup(matches: &mut Matches, molecule: &MoleculeHashRange, key: String, slot: OneDSlot) {
    let (hash, range) = match slot {
        OneDSlot::Hash => (key.as_str(), ""),
        OneDSlot::Range => ("", key.as_str()),
    };
    if let Some(uuid) = molecule.get_atom_uuid(hash, range) {
        matches.insert(projected_key(key, slot), uuid.clone());
    }
}

fn extend(
    matches: &mut Matches,
    entries: impl IntoIterator<Item = (String, String)>,
    slot: OneDSlot,
) {
    matches.extend(
        entries
            .into_iter()
            .map(|(key, uuid)| (projected_key(key, slot), uuid)),
    );
}

fn all_atoms(molecule: &MoleculeHashRange, slot: OneDSlot) -> Vec<(String, String)> {
    let mut entries: Vec<_> = molecule
        .iter_all_atoms()
        .map(|(hash, range, uuid)| {
            let key = match slot {
                OneDSlot::Hash => hash,
                OneDSlot::Range => range,
            };
            (key.clone(), uuid.clone())
        })
        .collect();
    // Preserve both existing orders, including projection of unusual slots:
    // hash mode explicitly sorts by hash; range mode retains molecule order.
    if slot == OneDSlot::Hash {
        entries.sort_by(|a, b| a.0.cmp(&b.0));
    }
    entries
}
