use std::collections::HashMap;

use crate::schema::types::field::{
    apply_keyed_filter, HashRangeFilter, HashRangeFilterResult, KeyedFilterMode,
};
use crate::schema::types::key_value::KeyValue;

use super::{FieldKind, FieldVariant};

impl FieldVariant {
    /// Select this field's slot components without an encoding change.
    /// API coordinates stay API coordinates; stored coordinates stay stored.
    #[must_use]
    pub(crate) fn slot_coordinates(&self, kv: &KeyValue) -> Option<(String, String)> {
        match self.kind {
            FieldKind::Single => Some((String::new(), String::new())),
            FieldKind::Hash => kv.hash.clone().map(|h| (h, String::new())),
            FieldKind::Range => kv.range.clone().map(|r| (String::new(), r)),
            FieldKind::HashRange => kv.hash.clone().zip(kv.range.clone()),
        }
    }

    /// Apply a filter over the hydrated molecule.
    ///
    /// Storage is unified (`MoleculeHashRange` for every keyed field). Filter
    /// **projection** (hash-only / range-only / full hash+range `KeyValue`s)
    /// follows [`FieldKind`] via [`KeyedFilterMode`].
    pub fn apply_filter(&self, filter: Option<HashRangeFilter>) -> HashRangeFilterResult {
        match (&self.kind, &self.molecule) {
            (FieldKind::Single, Some(molecule)) => apply_single_filter(molecule, filter),
            (kind, Some(molecule)) => {
                let mode = match kind {
                    FieldKind::Hash => KeyedFilterMode::Hash,
                    FieldKind::Range => KeyedFilterMode::Range,
                    FieldKind::HashRange => KeyedFilterMode::HashRange,
                    FieldKind::Single => unreachable!("handled above"),
                };
                apply_keyed_filter(molecule, filter, mode)
            }
            // No molecule, or kind/data mismatch.
            _ => HashRangeFilterResult::empty(),
        }
    }

    /// Returns all keys present in this field's molecule.
    pub fn get_all_keys(&self) -> Vec<KeyValue> {
        match (&self.kind, &self.molecule) {
            (FieldKind::Single, Some(_)) => vec![KeyValue::new(None, None)],
            // Unified HashRange-backed Hash: project (h,"") → KeyValue(hash only).
            (FieldKind::Hash, Some(m)) => {
                let mut keys: Vec<String> = m.iter_all_atoms().map(|(h, _, _)| h.clone()).collect();
                keys.sort();
                keys.into_iter()
                    .map(|hash| KeyValue::new(Some(hash), None))
                    .collect()
            }
            // Unified HashRange-backed Range: project ("",r) → KeyValue(range only).
            (FieldKind::Range, Some(m)) => m
                .iter_all_atoms()
                .map(|(_, r, _)| KeyValue::new(None, Some(r.clone())))
                .collect(),
            (FieldKind::HashRange, Some(m)) => {
                let mut keys: Vec<KeyValue> = m
                    .iter_all_atoms()
                    .map(|(hash_value, range_key, _)| {
                        KeyValue::new(Some(hash_value.clone()), Some(range_key.clone()))
                    })
                    .collect();
                keys.sort_by(|a, b| {
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
                keys
            }
            // Unwritten Single, empty molecule, or kind/data mismatch.
            _ => vec![],
        }
    }

    /// Atom UUID currently at the molecule head for `kv`.
    #[must_use]
    pub fn current_atom_uuid(&self, kv: &KeyValue) -> Option<String> {
        match (&self.kind, &self.molecule) {
            (FieldKind::Single, Some(m)) => m.get_atom_uuid("", "").cloned(),
            (FieldKind::Hash, Some(m)) => kv
                .hash
                .as_ref()
                .and_then(|h| m.get_atom_uuid(h, "").cloned()),
            (FieldKind::Range, Some(m)) => kv
                .range
                .as_ref()
                .and_then(|r| m.get_atom_uuid("", r).cloned()),
            (FieldKind::HashRange, Some(m)) => kv
                .hash
                .as_ref()
                .zip(kv.range.as_ref())
                .and_then(|(h, r)| m.get_atom_uuid(h, r).cloned()),
            _ => None,
        }
    }

    /// Remove the entry for `kv` from the molecule. Returns true if something dropped.
    pub fn remove_key(&mut self, kv: &KeyValue) -> bool {
        match (&self.kind, &mut self.molecule) {
            (FieldKind::Single, Some(m)) => m.remove_atom_uuid("", "").is_some(),
            (FieldKind::Hash, Some(m)) => match kv.hash.as_ref() {
                Some(h) => m.remove_atom_uuid(h, "").is_some(),
                None => false,
            },
            (FieldKind::Range, Some(m)) => match kv.range.as_ref() {
                Some(r) => m.remove_atom_uuid("", r).is_some(),
                None => false,
            },
            (FieldKind::HashRange, Some(m)) => match kv.hash.as_ref().zip(kv.range.as_ref()) {
                Some((h, r)) => m.remove_atom_uuid(h, r).is_some(),
                None => false,
            },
            _ => false,
        }
    }

    /// Derive the per-key change unit for a write at `key_value`.
    #[must_use]
    pub(crate) fn changed_key_for(
        &self,
        key_value: &KeyValue,
    ) -> Option<crate::db_operations::ChangedKey> {
        use crate::db_operations::ChangedKey;
        match self.kind {
            // Empty both components → disk slot ("","").
            FieldKind::Single => Some(ChangedKey {
                hash: None,
                range: None,
            }),
            FieldKind::Hash => key_value.hash.clone().map(ChangedKey::hash),
            FieldKind::Range => key_value.range.clone().map(ChangedKey::range),
            FieldKind::HashRange => key_value
                .hash
                .clone()
                .zip(key_value.range.clone())
                .map(|(hash, range)| ChangedKey::hash_range(hash, range)),
        }
    }
}

/// Single-field filter semantics (ported from former SingleField::apply_filter).
#[allow(
    clippy::needless_pass_by_value,
    reason = "by-value matches sibling apply_*_filter signatures so FieldVariant::apply_filter dispatches uniformly"
)]
fn apply_single_filter(
    molecule: &crate::atom::MoleculeHashRange,
    filter: Option<HashRangeFilter>,
) -> HashRangeFilterResult {
    if matches!(filter, Some(HashRangeFilter::SampleN(0))) {
        return HashRangeFilterResult::new(HashMap::new());
    }
    if let Some(HashRangeFilter::Page { offset, limit }) = filter {
        if offset > 0 || limit == 0 {
            return HashRangeFilterResult::new(HashMap::new());
        }
    }
    let Some(uuid) = molecule.get_atom_uuid("", "").cloned() else {
        return HashRangeFilterResult::empty();
    };
    let mut matches = HashMap::new();
    matches.insert(KeyValue::new(None, None), uuid);
    HashRangeFilterResult::new(matches)
}
