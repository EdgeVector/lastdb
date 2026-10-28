//! Encode API range bounds before applying a filter to storage-form slots.
//!
//! Exact HashRange keys are remapped to API form by the narrowed loader, so
//! only range/prefix filters and RangeKey require storage-form bounds here.

use crate::atom::{MoleculeKeyCodec, MoleculeKeyCodecError};
use crate::schema::types::field::HashRangeFilter;

pub fn filter_range_bounds_for_apply(
    filter: &HashRangeFilter,
    codec: &MoleculeKeyCodec,
    molecule_uuid: &str,
) -> Result<HashRangeFilter, MoleculeKeyCodecError> {
    use HashRangeFilter::*;

    if matches!(filter, HashRangePrefix { .. } | RangePrefix(_)) {
        codec.require_text_range_prefix()?;
    }
    if codec.range_profile().is_none() {
        return Ok(filter.clone());
    }
    let encode = |value: &str| codec.storage_range(molecule_uuid, value);
    Ok(match filter {
        HashRangePrefix { hash, prefix } if !prefix.is_empty() => HashRangePrefix {
            hash: hash.clone(),
            prefix: encode(prefix)?,
        },
        RangePrefix(prefix) if !prefix.is_empty() => RangePrefix(encode(prefix)?),
        HashRangeRange { hash, start, end } => HashRangeRange {
            hash: hash.clone(),
            start: encode(start)?,
            end: encode(end)?,
        },
        RangeRange { start, end } => RangeRange {
            start: encode(start)?,
            end: encode(end)?,
        },
        RangeKey(range) if !range.is_empty() => RangeKey(encode(range)?),
        other => other.clone(),
    })
}
