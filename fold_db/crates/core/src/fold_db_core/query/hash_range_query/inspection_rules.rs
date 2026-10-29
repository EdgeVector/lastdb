//! Storage-free bounded inspection growth and final page selection.

use crate::schema::types::field::{HashRangeFilter, SAMPLE_PEEK_CAP};
use crate::schema::types::key_value::KeyValue;
use crate::schema::SchemaError;

/// Pages/counts require completeness; a sample can stop at enough valid members.
pub(super) const INSPECTION_KEY_CAP: usize = 10_000;

/// Grow only within the allowance left after all previously examined scopes.
pub(super) fn next_candidate_cap(current: usize, examined: usize) -> Result<usize, SchemaError> {
    let remaining = INSPECTION_KEY_CAP.saturating_sub(examined);
    let next = current.saturating_mul(2).min(remaining);
    if next == 0 {
        return Err(inspection_limit());
    }
    Ok(next)
}

/// Call only after candidate completeness and coordinate recovery succeed.
pub(super) fn page_keys(
    mut keys: Vec<KeyValue>,
    filter: Option<&HashRangeFilter>,
) -> Result<Vec<KeyValue>, SchemaError> {
    keys.sort_by(KeyValue::cmp_page_order);
    let (offset, limit) = match filter {
        Some(HashRangeFilter::Page { offset, limit }) => (*offset, *limit),
        Some(HashRangeFilter::SampleN(n)) => (0, (*n).min(SAMPLE_PEEK_CAP)),
        None => (0, INSPECTION_KEY_CAP),
        _ => {
            return Err(SchemaError::InvalidData(
                "unsupported inspection selector".into(),
            ))
        }
    };
    Ok(keys.into_iter().skip(offset).take(limit).collect())
}

pub(super) fn inspection_limit() -> SchemaError {
    SchemaError::ReadResourceLimit {
        detail: "administrative inspection exceeds 10000 examined candidate keys; query one hash or an exact key batch".into(),
    }
}
