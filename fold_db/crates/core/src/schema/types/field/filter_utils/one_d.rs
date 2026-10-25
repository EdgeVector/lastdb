//! One-dimensional key selection over the shared HashRange molecule.

use super::DEFAULT_UNFILTERED_PAGE_LIMIT;
use crate::atom::MoleculeHashRange;
use crate::db_operations::OneDSlot;
use crate::schema::types::field::{HashRangeFilter, HashRangeFilterResult, SAMPLE_PEEK_CAP};
use crate::schema::types::key_value::KeyValue;
use std::collections::HashMap;
use std::ops::Bound::{Excluded, Included, Unbounded};

type Matches = HashMap<KeyValue, String>;

enum Selection {
    Key(String),
    Keys(Vec<(String, String)>),
    Page {
        offset: usize,
        limit: usize,
        after: Option<String>,
    },
    Prefix(String),
    Between {
        start: String,
        end: String,
    },
    Empty,
}

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
fn selection(filter: HashRangeFilter, slot: OneDSlot) -> Selection {
    use HashRangeFilter::*;
    match (slot, filter) {
        (_, SampleN(n)) => Selection::Page {
            offset: 0,
            limit: n.min(SAMPLE_PEEK_CAP),
            after: None,
        },
        (_, Page { offset, limit }) => Selection::Page {
            offset,
            limit,
            after: None,
        },
        (_, PageAfter { after, limit }) => Selection::Page {
            offset: 0,
            limit,
            after: Some(
                match slot {
                    OneDSlot::Hash => after.hash,
                    OneDSlot::Range => after.range,
                }
                .unwrap_or_default(),
            ),
        },
        (_, HashKey(key)) | (OneDSlot::Range, RangeKey(key)) => Selection::Key(key),
        (
            OneDSlot::Hash,
            HashRangeKey { hash, .. } | HashRangePrefix { hash, .. } | HashRangeRange { hash, .. },
        ) => Selection::Key(hash),
        (OneDSlot::Range, HashRangeKey { range, .. }) => Selection::Key(range),
        (_, HashRangeKeys(keys)) => Selection::Keys(keys),
        (OneDSlot::Range, RangePrefix(prefix) | HashRangePrefix { prefix, .. }) => {
            Selection::Prefix(prefix)
        }
        (OneDSlot::Hash, HashRange { start, end })
        | (OneDSlot::Range, RangeRange { start, end } | HashRangeRange { start, end, .. }) => {
            Selection::Between { start, end }
        }
        // Patterns are unsupported in both modes. A range selector cannot
        // select a hash field; a hash interval cannot select a range field.
        (_, HashPattern(_) | RangePattern(_) | HashRangePattern { .. })
        | (OneDSlot::Hash, RangeKey(_) | RangePrefix(_) | RangeRange { .. })
        | (OneDSlot::Range, HashRange { .. }) => Selection::Empty,
    }
}

fn apply_one_d(
    molecule: &MoleculeHashRange,
    filter: Option<HashRangeFilter>,
    slot: OneDSlot,
) -> HashRangeFilterResult {
    let filter = filter.unwrap_or(HashRangeFilter::Page {
        offset: 0,
        limit: DEFAULT_UNFILTERED_PAGE_LIMIT,
    });
    let mut matches = HashMap::new();
    match selection(filter, slot) {
        Selection::Key(key) => lookup(&mut matches, molecule, key, slot),
        Selection::Keys(keys) => {
            for (hash, range) in keys {
                let key = match slot {
                    OneDSlot::Hash => hash,
                    OneDSlot::Range => range,
                };
                lookup(&mut matches, molecule, key, slot);
            }
        }
        Selection::Page {
            offset,
            limit,
            after,
        } => extend(
            &mut matches,
            all_atoms(molecule, slot)
                .into_iter()
                .filter(|(key, _)| after.as_ref().is_none_or(|after| key > after))
                .skip(offset)
                .take(limit),
            slot,
        ),
        Selection::Prefix(prefix) => {
            if prefix.is_empty() {
                extend(&mut matches, all_atoms(molecule, slot), slot);
            } else if let Some(entries) = molecule.atom_entries_for_hash("") {
                extend(
                    &mut matches,
                    entries
                        .range::<str, _>((Included(prefix.as_str()), Unbounded))
                        .take_while(|(key, _)| key.starts_with(&prefix))
                        .map(|(key, entry)| (key.clone(), entry.atom_uuid.clone())),
                    slot,
                );
            }
        }
        Selection::Between { start, end } => {
            if start <= end {
                match slot {
                    OneDSlot::Hash => extend(
                        &mut matches,
                        all_atoms(molecule, slot)
                            .into_iter()
                            .filter(|(key, _)| key >= &start && key < &end),
                        slot,
                    ),
                    OneDSlot::Range => {
                        if let Some(entries) = molecule.atom_entries_for_hash("") {
                            extend(
                                &mut matches,
                                entries
                                    .range::<str, _>((
                                        Included(start.as_str()),
                                        Excluded(end.as_str()),
                                    ))
                                    .map(|(key, entry)| (key.clone(), entry.atom_uuid.clone())),
                                slot,
                            );
                        }
                    }
                }
            }
        }
        Selection::Empty => {}
    }
    HashRangeFilterResult::new(matches)
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
