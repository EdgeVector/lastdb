//! Shared utilities for filter application across keyed molecule fields.
//!
//! Storage is one shape ([`MoleculeHashRange`]). Filter **semantics** still
//! follow the field's key projection ([`KeyedFilterMode`]): hash-only,
//! range-only, or full hash+range. Production entry point:
//! [`apply_keyed_filter`]. The `apply_hash_filter` / `apply_range_filter` /
//! `apply_hash_range_filter` names remain as thin mode-specific wrappers for
//! unit tests and call sites that already know the mode.

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
    /// Creates a prefix end boundary for efficient range queries
    /// This is used for BTree range operations to find all keys with a given prefix
    pub fn create_prefix_end(prefix: &str) -> String {
        let mut prefix_end = prefix.to_string();
        if let Some(last_char) = prefix_end.chars().last() {
            // Skip the surrogate range (U+D800..=U+DFFF): those code points
            // are not valid Rust scalar values, so `char::from_u32(0xD800)`
            // returns `None`. The previous NUL-terminated fallback produced
            // a bound `"…\u{D7FF}\0"` that strictly precedes any
            // `"…\u{D7FF}<x>"` in byte order, silently dropping every key
            // continuing past the boundary. Hopping to U+E000 — the next
            // valid scalar — yields a correct half-open upper bound.
            let next_cp = match last_char as u32 + 1 {
                0xD800 => 0xE000,
                n => n,
            };
            if let Some(next_char) = char::from_u32(next_cp) {
                prefix_end.pop();
                prefix_end.push(next_char);
            } else {
                // If we can't increment the last character, append a null character
                prefix_end.push('\0');
            }
        } else {
            // Empty prefix: there is no string upper bound greater than every
            // string, so the `start..end` range trick cannot express "match
            // every key". Callers that mean "all keys" must special-case the
            // empty prefix before calling here — `"\0"` yields the half-open
            // range `"".."\0"`, which matches only the empty-string key.
            prefix_end = "\0".to_string();
        }
        prefix_end
    }
}

mod fetch;
mod hash;
mod hash_range;
mod range;
mod storage_bounds;

pub use fetch::{fetch_atoms_with_key_metadata_async_with_prefix, KeyedAtomMatch};
pub use hash::apply_hash_filter;
pub use hash_range::apply_hash_range_filter;
pub use range::apply_range_filter;
pub use storage_bounds::expand_filter_range_bounds_for_apply;
