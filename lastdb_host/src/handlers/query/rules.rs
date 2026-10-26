//! Query selection, admission, keyed pages, and captured-context row order.

use std::cmp::Ordering;

use fold_db::schema::types::field::{HashRangeFilter, KeyWindow};
use fold_db::schema::types::key_comparison::RangeKeyComparison;
use fold_db::schema::types::operations::{Query, QueryOrderBy, SortOrder};
use fold_db::schema::types::KeyValue;
use serde_json::Value;

use crate::qos::Lane;
use crate::HostError;

/// Check options after schema resolution and the product keyed-access check,
/// preserving the order in which invalid combinations are rejected.
pub(super) fn validate_query_options(query: &Query, has_cursor: bool) -> Result<(), HostError> {
    if query.expected_total_count.is_some() && !can_push_down_key_restricted(query) {
        return Err(HostError::new(
            400,
            "expected_total_count requires a keyed query with no post-key filters".to_string(),
        ));
    }
    if has_cursor && !is_key_restricted_filter(query.filter.as_ref()) {
        return Err(HostError::new(
            400,
            "Cross-hash query cursors are unsupported; use an offset or query one hash with a cursor"
                .to_string(),
        ));
    }
    if has_cursor
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
                return Err(HostError::new(
                    409,
                    "expected_total_count did not match the keyed count".to_string(),
                ));
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

/// Compare response rows using only the supplied values and range rule.
/// Field ordering uses the core's JSON order; otherwise the range is primary.
/// Equal primary values use ascending range and then ascending hash ties, even
/// when the caller requests descending order.
pub(super) fn compare_rows(
    a: &Value,
    b: &Value,
    sort_order: Option<&SortOrder>,
    order_by: Option<&QueryOrderBy>,
    range_comparison: Option<&RangeKeyComparison>,
) -> Ordering {
    let compare_ranges = |left: &str, right: &str| match range_comparison {
        Some(comparison) => comparison.compare(left, right),
        None => left.cmp(right),
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
        let a_range = a["key"]["range"].as_str().unwrap_or("");
        let b_range = b["key"]["range"].as_str().unwrap_or("");
        match sort_order {
            Some(SortOrder::Desc) => compare_ranges(b_range, a_range),
            _ => compare_ranges(a_range, b_range),
        }
    };
    primary
        .then_with(|| {
            let a_range = a["key"]["range"].as_str().unwrap_or("");
            let b_range = b["key"]["range"].as_str().unwrap_or("");
            compare_ranges(a_range, b_range)
        })
        .then_with(|| {
            let a_hash = a["key"]["hash"].as_str().unwrap_or("");
            let b_hash = b["key"]["hash"].as_str().unwrap_or("");
            a_hash.cmp(b_hash)
        })
}

/// Key/partition filters that do **not** require a full-schema product scan.
/// `Page` / `PageAfter` / `SampleN` alone still walk the schema and are treated
/// as unfiltered product scans for the deprecation gate.
///
/// The predicate itself lives on [`HashRangeFilter`] because the count path
/// keys off the same distinction: a key-restricted read counts and pages its
/// partition, an unfiltered one the whole schema. Two copies drifting apart is
/// what let a key-restricted read pass the scan gate but miss the push-down.
pub(super) fn is_key_restricted_filter(filter: Option<&HashRangeFilter>) -> bool {
    filter.is_some_and(HashRangeFilter::is_key_restricted)
}

/// An unfiltered administrative query needs no post-key stage.
pub(super) fn can_push_down(query: &Query) -> bool {
    query.filter.is_none() && !query.requires_post_key_work()
}

/// Can this key-restricted read be counted and windowed instead of
/// materialized whole?
///
/// The filter has to select a **key set** (so the count describes the same rows
/// the page will), and nothing may decide row membership after the keys are
/// read. `value_filters` drop rows by their hydrated content,
/// `field_predicates` / `order_by` / `predicate_limit` run the two-pass plan
/// over every candidate — each would be answering from an arbitrary slice of
/// its input if a window were taken first. Those shapes keep the
/// materialize-then-slice fallback.
pub(super) fn can_push_down_key_restricted(query: &Query) -> bool {
    is_key_restricted_filter(query.filter.as_ref()) && !query.requires_post_key_work()
}

/// Reversing one ascending key window also reverses its hash ties.
fn descending_cross_hash_batch(query: &Query) -> bool {
    if !matches!(query.sort_order, Some(SortOrder::Desc)) {
        return false;
    }
    let Some(HashRangeFilter::HashRangeKeys(keys)) = &query.filter else {
        return false;
    };
    keys.first()
        .is_some_and(|(hash, _)| keys.iter().any(|(other, _)| other != hash))
}

/// The exact keyed count is required only for a descending offset page, where
/// it chooses the window from the high end, or when a caller asks the node to
/// verify a keyed COUNT assertion. Ordinary ascending keyed pages use a cursor
/// and must not pay a partition count merely to populate metadata.
pub(super) fn key_restricted_count_is_required(query: &Query, cursor: Option<&KeyValue>) -> bool {
    query.expected_total_count.is_some()
        || (cursor.is_none() && matches!(query.sort_order.as_ref(), Some(SortOrder::Desc)))
}

/// A bare page can use count-and-fetch only without a post-key stage.
pub(super) fn caller_supplied_page(query: &Query) -> Option<(usize, usize)> {
    if query.requires_post_key_work() {
        return None;
    }
    match &query.filter {
        Some(HashRangeFilter::Page { offset, limit }) => Some((*offset, *limit)),
        _ => None,
    }
}

/// Translate a caller's `(offset, limit)` page request into the ascending
/// [`HashRangeFilter::Page`] that selects it, honouring `sort_order`. Records are
/// stored/sliced in ascending `(range, hash)` order; a descending page counts
/// from the high end, so map it to the matching ascending window (the fetched
/// page is then re-ordered descending by [`super::rows::format_rows`]).
pub(super) fn page_filter_for(
    sort_order: Option<&SortOrder>,
    total: usize,
    offset: usize,
    limit: usize,
) -> HashRangeFilter {
    match sort_order {
        Some(SortOrder::Desc) => {
            let asc_offset = total.saturating_sub(offset.saturating_add(limit));
            let asc_limit = total.saturating_sub(offset).saturating_sub(asc_offset);
            HashRangeFilter::Page {
                offset: asc_offset,
                limit: asc_limit,
            }
        }
        _ => HashRangeFilter::Page { offset, limit },
    }
}

/// Select fallback admission from the key filter and page limit.
/// Unkeyed administrative reads always use Bulk; other shapes retain the
/// existing page-size policy.
pub(super) fn lane_for_unknown_cardinality(query: &Query) -> Lane {
    if !is_key_restricted_filter(query.filter.as_ref()) {
        // Administrative inspection may need to reject many other-layout
        // candidates before it can return even one logical record.
        return Lane::Bulk;
    }
    let limit = match &query.filter {
        Some(HashRangeFilter::Page { limit, .. } | HashRangeFilter::PageAfter { limit, .. }) => {
            *limit
        }
        _ => return Lane::Interactive,
    };
    Lane::for_page_limit(limit)
}
