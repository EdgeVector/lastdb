//! Storage-free response order and sort-helper projection rules.

use std::cmp::Ordering;

use fold_db::schema::types::key_comparison::RangeKeyComparison;
use fold_db::schema::types::operations::{Query, QueryOrderBy, SortOrder};
use fold_db::schema::types::SchemaError;
use serde_json::Value;

use crate::HostError;

/// Sort with the caller's captured range rule, without ambient request state.
/// Validate even a one-row response before any comparator can see a bad range.
pub(super) fn sort_rows(
    rows: &mut [Value],
    sort_order: Option<&SortOrder>,
    order_by: Option<&QueryOrderBy>,
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
    rows.sort_by(|left, right| {
        match compare_rows(left, right, sort_order, order_by, range_comparison) {
            Ok(order) => order,
            Err(detail) => {
                error.get_or_insert(detail);
                Ordering::Equal
            }
        }
    });
    match error {
        Some(error) => Err(error.into()),
        None => Ok(()),
    }
}

/// Compare response rows using only the supplied values and range rule.
/// Field ordering uses the core's JSON order; otherwise the range is primary.
/// Equal primary values use ascending range and then ascending hash ties, even
/// when the caller requests descending order.
fn compare_rows(
    a: &Value,
    b: &Value,
    sort_order: Option<&SortOrder>,
    order_by: Option<&QueryOrderBy>,
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
    let primary = if let Some(order_by) = order_by {
        let af = a
            .get("fields")
            .and_then(|fields| fields.get(&order_by.field));
        let bf = b
            .get("fields")
            .and_then(|fields| fields.get(&order_by.field));
        let ordering = fold_db::json_order::compare_json_values(af, bf);
        match order_by.order.as_ref() {
            Some(SortOrder::Desc) => ordering.reverse(),
            _ => ordering,
        }
    } else {
        match sort_order {
            Some(SortOrder::Desc) => range_order.reverse(),
            _ => range_order,
        }
    };
    Ok(primary.then(range_order).then_with(|| {
        let a_hash = a["key"]["hash"].as_str().unwrap_or("");
        let b_hash = b["key"]["hash"].as_str().unwrap_or("");
        a_hash.cmp(b_hash)
    }))
}

/// Hide an unprojected sort value only after the complete result order is set.
pub(super) fn remove_sort_helper(results: &mut [Value], sort_helper: Option<&str>) {
    if let Some(field) = sort_helper {
        for row in results {
            if let Some(fields) = row.get_mut("fields").and_then(Value::as_object_mut) {
                fields.remove(field);
            }
        }
    }
}

/// The core retains this sort value without changing projection key discovery.
pub(super) fn unprojected_sort_field(query: &Query) -> Option<String> {
    let field = &query.order_by.as_ref()?.field;
    if query.fields.is_empty() || query.fields.contains(field) {
        return None;
    }
    Some(field.clone())
}
