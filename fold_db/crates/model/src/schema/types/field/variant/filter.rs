use std::collections::HashMap;

use crate::schema::types::field::{
    apply_keyed_filter, HashRangeFilter, HashRangeFilterResult, KeyedFilterMode,
};
use crate::schema::types::key_value::KeyValue;

use super::{FieldKind, FieldVariant};

impl FieldVariant {
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
