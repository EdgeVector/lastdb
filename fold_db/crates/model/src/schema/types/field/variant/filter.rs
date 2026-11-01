use std::collections::HashMap;

use crate::schema::types::field::{
    apply_keyed_filter, HashRangeFilter, HashRangeFilterResult, KeyedFilterMode,
};
use crate::schema::types::key_value::KeyValue;

use super::{FieldKind, FieldVariant};

impl FieldVariant {
    /// Owned slot coordinates for persistence and cross-field plans.
    #[must_use]
    pub fn slot_coordinates(&self, key: &KeyValue) -> Option<(String, String)> {
        self.slot_coordinates_ref(key)
            .map(|(hash, range)| (hash.to_string(), range.to_string()))
    }

    /// Select this field's slot components without an encoding change.
    /// API coordinates stay API coordinates; stored coordinates stay stored.
    #[must_use]
    pub fn slot_coordinates_ref<'a>(&self, kv: &'a KeyValue) -> Option<(&'a str, &'a str)> {
        match self.kind {
            FieldKind::Single => Some(("", "")),
            FieldKind::Hash => kv.hash.as_deref().map(|hash| (hash, "")),
            FieldKind::Range => kv.range.as_deref().map(|range| ("", range)),
            FieldKind::HashRange => kv.hash.as_deref().zip(kv.range.as_deref()),
        }
    }

    /// Refuse a read that this field's layout or the filter cannot serve.
    pub fn require_supported_read(
        &self,
        filter: Option<&HashRangeFilter>,
    ) -> Result<(), crate::schema::types::SchemaError> {
        use crate::schema::types::schema::DeclarativeSchemaType;

        let layout = match self.kind {
            FieldKind::Single => DeclarativeSchemaType::Single,
            FieldKind::Hash => DeclarativeSchemaType::Hash,
            FieldKind::Range => DeclarativeSchemaType::Range,
            FieldKind::HashRange => DeclarativeSchemaType::HashRange,
        };
        crate::schema::require_logical_layout(&layout)?;
        crate::schema::require_logical_filter(filter)
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
        let molecule = self.molecule.as_ref()?;
        let (hash, range) = self.slot_coordinates_ref(kv)?;
        molecule.get_atom_uuid(hash, range).cloned()
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
