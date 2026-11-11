//! Storage-free response order.

use std::cmp::Ordering;

use fold_db::schema::types::key_comparison::RangeKeyComparison;
use fold_db::schema::types::operations::SortOrder;
use fold_db::schema::types::SchemaError;
use serde_json::Value;

use crate::lastdb_host::HostError;

/// Sort with the caller's captured range rule, without ambient request state.
/// Validate even a one-row response before any comparator can see a bad range.
pub(super) fn sort_rows(
    rows: &mut [Value],
    sort_order: Option<&SortOrder>,
    range_comparison: Option<&RangeKeyComparison>,
) -> Result<(), HostError> {
    if let Some(comparison) = range_comparison {
        for row in rows.iter() {
            comparison
                .validate_value(row["key"]["range"].as_str().unwrap_or(""))
                .map_err(SchemaError::InvalidData)?;
        }
    }
    let mut error = None;
    rows.sort_by(
        |left, right| match compare_rows(left, right, sort_order, range_comparison) {
            Ok(order) => order,
            Err(detail) => {
                error.get_or_insert(detail);
                Ordering::Equal
            }
        },
    );
    match error {
        Some(error) => Err(error.into()),
        None => Ok(()),
    }
}

/// Compare response rows using only the supplied key coordinates and range rule.
/// Equal primary range values use ascending range and then ascending hash ties,
/// even when the caller requests descending order.
fn compare_rows(
    a: &Value,
    b: &Value,
    sort_order: Option<&SortOrder>,
    range_comparison: Option<&RangeKeyComparison>,
) -> Result<Ordering, SchemaError> {
    let a_range = a["key"]["range"].as_str().unwrap_or("");
    let b_range = b["key"]["range"].as_str().unwrap_or("");
    let range_order = match range_comparison {
        Some(comparison) => comparison
            .compare(a_range, b_range)
            .map_err(SchemaError::InvalidData)?,
        None => a_range.cmp(b_range),
    };
    let primary = match sort_order {
        Some(SortOrder::Desc) => range_order.reverse(),
        _ => range_order,
    };
    Ok(primary.then(range_order).then_with(|| {
        let a_hash = a["key"]["hash"].as_str().unwrap_or("");
        let b_hash = b["key"]["hash"].as_str().unwrap_or("");
        a_hash.cmp(b_hash)
    }))
}
