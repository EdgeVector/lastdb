//! Pure query option, keyed-page, and row-order rules.
//! No host, storage, or task access.

use std::cmp::Ordering;

use fold_db::schema::types::field::{HashRangeFilter, KeyWindow};
use fold_db::schema::types::operations::{Query, QueryOrderBy, SortOrder};
use fold_db::schema::types::KeyValue;
use serde_json::Value;

use crate::HostError;

use super::paging::{
    can_push_down_key_restricted, descending_cross_hash_batch, is_key_restricted_filter,
    page_filter_for, reject_wrong_expected_total_count,
};

/// Check options after schema resolution and the product keyed-access check,
/// preserving the order in which invalid combinations are rejected.
pub(super) fn validate_query_options(
    query: &Query,
    cursor: Option<&KeyValue>,
) -> Result<(), HostError> {
    if query.expected_total_count.is_some() && !can_push_down_key_restricted(query) {
        return Err(HostError::new(
            400,
            "expected_total_count requires a keyed query with no post-key filters".to_string(),
        ));
    }
    if cursor.is_some() && !is_key_restricted_filter(query.filter.as_ref()) {
        return Err(HostError::new(
            400,
            "Cross-hash query cursors are unsupported; use an offset or query one hash with a cursor"
                .to_string(),
        ));
    }
    if cursor.is_some()
        && (!can_push_down_key_restricted(query)
            || matches!(query.sort_order, Some(SortOrder::Desc)))
    {
        return Err(HostError::new(
            400,
            "Query cursors require ascending keyed reads without field conditions or value ordering; use an offset for this query".to_string(),
        ));
    }
    Ok(())
}

/// Validate an optional count and select a keyed window from known inputs.
/// `None` requires the bounded fallback instead of a windowed read.
pub(super) fn keyed_window(
    query: &Query,
    limit: usize,
    offset: usize,
    cursor: Option<&KeyValue>,
    total: Option<usize>,
) -> Result<Option<KeyWindow>, HostError> {
    if let Some(total) = total {
        if let Some(expected) = query.expected_total_count {
            if expected != total {
                return Err(reject_wrong_expected_total_count(expected, total));
            }
        }
    } else if query.expected_total_count.is_some() {
        return Err(HostError::new(
            400,
            "expected_total_count is unavailable for this query".to_string(),
        ));
    }

    // A reversed ascending window cannot preserve hash ties across hashes.
    // Descending reads without a live count also need the selected set before
    // an offset can be chosen. Both retain the existing bounded fallback.
    if descending_cross_hash_batch(query)
        || (matches!(query.sort_order, Some(SortOrder::Desc)) && total.is_none())
    {
        return Ok(None);
    }

    let overfetch_limit = limit.saturating_add(1);
    let window = if let Some(after) = cursor {
        KeyWindow::after(after.clone(), overfetch_limit)
    } else if let Some(total) = total {
        // Descending offset pages select their window from the high end.
        match page_filter_for(query.sort_order.as_ref(), total, offset, overfetch_limit) {
            HashRangeFilter::Page { offset, limit } => KeyWindow::offset(offset, limit),
            _ => KeyWindow::offset(0, total),
        }
    } else {
        KeyWindow::offset(offset, overfetch_limit)
    };
    Ok(Some(window))
}

/// Compare response rows independently of reads, formatting, and phase timers.
/// Field ordering uses the core's JSON order; otherwise the range is primary.
/// Equal primary values use ascending range and then ascending hash ties, even
/// when the caller requests descending order.
pub(super) fn compare_rows(
    a: &Value,
    b: &Value,
    sort_order: Option<&SortOrder>,
    order_by: Option<&QueryOrderBy>,
) -> Ordering {
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
        let a_range = a["key"]["range"].as_str().unwrap_or("");
        let b_range = b["key"]["range"].as_str().unwrap_or("");
        match sort_order {
            Some(SortOrder::Desc) => b_range.cmp(a_range),
            _ => a_range.cmp(b_range),
        }
    };
    primary
        .then_with(|| {
            let a_range = a["key"]["range"].as_str().unwrap_or("");
            let b_range = b["key"]["range"].as_str().unwrap_or("");
            a_range.cmp(b_range)
        })
        .then_with(|| {
            let a_hash = a["key"]["hash"].as_str().unwrap_or("");
            let b_hash = b["key"]["hash"].as_str().unwrap_or("");
            a_hash.cmp(b_hash)
        })
}
