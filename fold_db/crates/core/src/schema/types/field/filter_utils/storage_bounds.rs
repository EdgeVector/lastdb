//! Rewrite API-space range bounds to storage-space before in-memory apply.
//!
//! `load_filtered_molecule` scans storage with OPE/blind candidates, then builds
//! a transient molecule whose **range** slots remain in storage form (OPE hex
//! under `ope_v1`). `apply_hash_range_filter` is pure string comparison and has
//! no codec — so a second pass with plain `"todo#"` against OPE ranges drops
//! every match (live `kanban list --column todo` → 0 while `HashKey` lists OK).
//!
//! Call [`expand_filter_range_bounds_for_apply`] from `collect_matches` so apply
//! compares storage-form prefix/range bounds. Dual-read encodings expand to
//! multiple filters (OPE then plain) and results are unioned.

use crate::atom::{MoleculeKeyCodec, MoleculeKeyCodecError};
use crate::schema::types::field::HashRangeFilter;

/// Expand an API-space filter into one or more storage-space filters for
/// in-memory apply against molecule keys that still hold storage-form ranges.
///
/// - **Plain** range encoding: single clone (no rewrite).
/// - **OPE / migrating OPE**: range bounds rewritten via
///   [`MoleculeKeyCodec::storage_range_read_candidates`] (primary first, plain
///   fallback when migrating).
/// - **Point `HashRangeKey`** and **multi-get `HashRangeKeys`**: left alone —
///   load remaps each range to API form (Option I). Left alone here is only
///   correct BECAUSE load narrows them; a variant that reaches the O(field)
///   full path with no rewrite here compares API bounds to storage-form keys
///   and drops rows without an error.
/// - **Hash-only / Page / SampleN**: no range bounds to rewrite.
pub fn expand_filter_range_bounds_for_apply(
    filter: &HashRangeFilter,
    codec: &MoleculeKeyCodec,
    molecule_uuid: &str,
) -> Result<Vec<HashRangeFilter>, MoleculeKeyCodecError> {
    use HashRangeFilter::*;

    // Fast path: nothing to rewrite when ranges are stored as API plaintext.
    if !codec.range_encoding().writes_ope() {
        return Ok(vec![filter.clone()]);
    }

    match filter {
        HashRangePrefix { hash, prefix } => {
            if prefix.is_empty() {
                return Ok(vec![filter.clone()]);
            }
            let cands = codec.storage_range_read_candidates(molecule_uuid, prefix)?;
            Ok(cands
                .into_iter()
                .map(|p| HashRangePrefix {
                    hash: hash.clone(),
                    prefix: p,
                })
                .collect())
        }
        RangePrefix(prefix) => {
            if prefix.is_empty() {
                return Ok(vec![filter.clone()]);
            }
            let cands = codec.storage_range_read_candidates(molecule_uuid, prefix)?;
            Ok(cands.into_iter().map(RangePrefix).collect())
        }
        HashRangeRange { hash, start, end } => {
            let starts = codec.storage_range_read_candidates(molecule_uuid, start)?;
            let ends = codec.storage_range_read_candidates(molecule_uuid, end)?;
            let n = starts.len().min(ends.len());
            Ok((0..n)
                .map(|i| HashRangeRange {
                    hash: hash.clone(),
                    start: starts[i].clone(),
                    end: ends[i].clone(),
                })
                .collect())
        }
        RangeRange { start, end } => {
            let starts = codec.storage_range_read_candidates(molecule_uuid, start)?;
            let ends = codec.storage_range_read_candidates(molecule_uuid, end)?;
            let n = starts.len().min(ends.len());
            Ok((0..n)
                .map(|i| RangeRange {
                    start: starts[i].clone(),
                    end: ends[i].clone(),
                })
                .collect())
        }
        // Exact range lookup against storage-form keys (not remapped by load).
        RangeKey(range) => {
            if range.is_empty() {
                return Ok(vec![filter.clone()]);
            }
            let cands = codec.storage_range_read_candidates(molecule_uuid, range)?;
            Ok(cands.into_iter().map(RangeKey).collect())
        }
        // Point HashRangeKey and multi-get HashRangeKeys: load_filtered remaps
        // each range → API (Option I), so apply already compares API to API.
        // HashKey / paging / patterns: no range bounds to rewrite at all.
        other => Ok(vec![other.clone()]),
    }
}
