//! Query selection, admission, and keyed-page rules.

use fold_db::access::AccessContext;
use fold_db::schema::types::field::{HashRangeFilter, KeyWindow};
use fold_db::schema::types::key_comparison::KeyComparisonContext;
use fold_db::schema::types::operations::{Query, SortOrder};
use fold_db::schema::types::{KeyValue, QueryCursor};

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
    if has_cursor
        && !query
            .filter
            .as_ref()
            .is_some_and(HashRangeFilter::is_key_restricted)
    {
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

/// Canonical request coordinates and their continuation binding. No host state.
pub(super) struct PreparedQuery {
    pub(super) query: Query,
    pub(super) cursor: Option<KeyValue>,
    pub(super) binding: Option<String>,
}

/// Prepare the filter, validate the cursor tag, then normalize its key.
/// The request supplies the captured contract after option validation.
pub(super) fn prepare_comparison(
    mut query: Query,
    cursor: Option<QueryCursor>,
    ctx: &AccessContext,
    comparison: Option<&KeyComparisonContext>,
) -> Result<PreparedQuery, HostError> {
    if let Some(comparison) = comparison {
        query.filter = comparison
            .canonical_filter(query.filter.take())
            .map_err(HostError::from)?;
    }
    let binding = comparison
        .map(|comparison| {
            comparison
                .cursor_binding(&(
                    &query.filter,
                    &query.fields,
                    query.include_tombstones,
                    &query.sort_order,
                    &ctx.db_locator,
                    &ctx.storage_prefix,
                ))
                .map_err(HostError::from)
        })
        .transpose()?;
    if let Some(cursor) = cursor.as_ref() {
        match cursor.comparison.as_deref() {
            Some(received) if binding.as_deref() != Some(received) => {
                return Err(HostError::new(
                    409,
                    "cursor comparison context changed; restart from the first page",
                ));
            }
            None if comparison.is_some() => {
                return Err(HostError::new(
                    409,
                    "a cursor without its comparison tag cannot continue; restart from the first page",
                ));
            }
            _ => {}
        }
    }
    let cursor = cursor.map(QueryCursor::into_key);
    let cursor = match comparison {
        Some(comparison) => cursor
            .map(|key| comparison.canonical_key(key))
            .transpose()
            .map_err(HostError::from)?,
        None => cursor,
    };
    Ok(PreparedQuery {
        query,
        cursor,
        binding,
    })
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
    query
        .filter
        .as_ref()
        .is_some_and(HashRangeFilter::is_key_restricted)
        && !query.requires_post_key_work()
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
/// page is then re-ordered descending by [`super::row_rules::sort_rows`]).
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
    if !query
        .filter
        .as_ref()
        .is_some_and(HashRangeFilter::is_key_restricted)
    {
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
