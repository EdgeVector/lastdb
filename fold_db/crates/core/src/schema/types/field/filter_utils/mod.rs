//! Shared utilities for filter application across keyed molecule fields.
//!
//! Storage is one shape ([`MoleculeHashRange`]). Filter **semantics** still
//! follow the field's key projection ([`KeyedFilterMode`]): hash-only,
//! range-only, or full hash+range. Production entry point:
//! [`apply_keyed_filter`]. The `apply_hash_filter` / `apply_range_filter` /
//! `apply_hash_range_filter` names remain as thin mode-specific wrappers for
//! call sites that already know the mode.

use crate::atom::MoleculeHashRange;
use crate::schema::types::field::{HashRangeFilter, HashRangeFilterResult};

/// Page size for an unfiltered field resolve when the caller passes no filter.
/// Matches the historical `SampleN(100)` default cap (now that SampleN is a peek
/// capped at [`SAMPLE_PEEK_CAP`], the bulk default must be a real `Page`). The
/// node query handler always supplies an explicit `Page`/key filter, so this
/// only governs direct in-core callers.
pub(super) const DEFAULT_UNFILTERED_PAGE_LIMIT: usize = 100;

/// How to project `(hash, range)` storage slots into public [`KeyValue`]s and
/// which filter arms are meaningful for this field kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyedFilterMode {
    /// Hash-only field: slots live at `(hash, "")`; API keys are `hash` only.
    Hash,
    /// Range-only field: slots live at `("", range)`; API keys are `range` only.
    Range,
    /// Full composite: slots and API keys are `(hash, range)`.
    HashRange,
}

/// Single entry point for applying a [`HashRangeFilter`] to a keyed molecule.
///
/// Dispatches to the mode-specific filter body. Prefer this over calling the
/// `apply_hash_*` wrappers from new production code.
#[must_use]
pub fn apply_keyed_filter(
    molecule: &MoleculeHashRange,
    optional_filter: Option<HashRangeFilter>,
    mode: KeyedFilterMode,
) -> HashRangeFilterResult {
    match mode {
        KeyedFilterMode::Hash => apply_hash_filter(molecule, optional_filter),
        KeyedFilterMode::Range => apply_range_filter(molecule, optional_filter),
        KeyedFilterMode::HashRange => apply_hash_range_filter(molecule, optional_filter),
    }
}

/// Common filter application utilities
pub struct FilterUtils;

impl FilterUtils {
    /// Return the finite exclusive upper bound for a physical prefix.
    ///
    /// Maximum trailing scalars carry into the preceding scalar. An empty or
    /// all-maximum prefix has no finite upper bound and receives a refusal.
    /// User prefixes should use prefix selection or include the physical
    /// namespace before this conversion.
    pub fn create_prefix_end(prefix: &str) -> Result<String, crate::schema::SchemaError> {
        let mut prefix_end = prefix.to_string();
        while let Some(last_char) = prefix_end.pop() {
            let next_cp = match last_char as u32 + 1 {
                0xD800 => 0xE000,
                n => n,
            };
            if let Some(next_char) = char::from_u32(next_cp) {
                prefix_end.push(next_char);
                return Ok(prefix_end);
            }
        }
        Err(crate::schema::SchemaError::InvalidData(
            "the prefix has no finite upper bound; select by prefix instead".into(),
        ))
    }
}

mod fetch;
mod hash;
mod hash_range;
mod range;
mod storage_bounds;

pub use fetch::{fetch_atoms_with_key_metadata_async_with_prefix, KeyedAtomMatch};
pub(crate) use fetch::{fetch_field_atom_batches, field_value_from_atom, FieldAtomBatch};
pub use hash::apply_hash_filter;
pub use hash_range::apply_hash_range_filter;
pub use range::apply_range_filter;
pub use storage_bounds::filter_range_bounds_for_apply;
