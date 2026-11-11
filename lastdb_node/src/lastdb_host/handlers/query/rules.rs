//! Query selection and keyed-page rules.

use fold_db::schema::types::field::{HashRangeFilter, KeyWindow};
use fold_db::schema::types::key_comparison::KeyComparisonContext;
use fold_db::schema::types::operations::{Query, SortOrder};
use fold_db::schema::types::{KeyValue, QueryCursor};

use crate::lastdb_host::HostError;

/// Check options after schema resolution and the keyed-access check,
/// preserving the order in which invalid combinations are rejected.
pub(super) fn validate_query_options(query: &Query, has_cursor: bool) -> Result<(), HostError> {
    if query.expected_total_count.is_some() && !can_push_down_key_restricted(query) {
        return Err(HostError::new(
            400,
            "expected_total_count requires a keyed query".to_string(),
        ));
    }
    if has_cursor
        && (!can_push_down_key_restricted(query)
            || matches!(query.sort_order, Some(SortOrder::Desc)))
    {
        return Err(HostError::new(
            400,
            "Query cursors require ascending keyed reads; use an offset for descending reads"
                .to_string(),
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
pub(super) fn prepare_comparison(
    mut query: Query,
    cursor: Option<QueryCursor>,
    comparison: Option<&KeyComparisonContext>,
) -> Result<PreparedQuery, HostError> {
    if let Some(comparison) = comparison {
        query.filter = comparison
            .canonical_filter(query.filter.take())
            .map_err(HostError::from)?;
    }
    let binding = comparison
        .map(|comparison| {
            let request = (&query.filter, query.include_tombstones, &query.sort_order);
            comparison.cursor_binding(&request).map_err(HostError::from)
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
    let record = cursor
        .as_ref()
        .map(QueryCursor::record_position)
        .transpose()
        .map_err(|error| HostError::new(400, error))?
        .flatten();
    if record.is_some() {
        return Err(HostError::new(
            400,
            "record cursors are unsupported on /api/query; continue a keyed read with its next_cursor",
        ));
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
    // an offset can be chosen. Both retain the bounded fallback.
    if descending_cross_hash_batch(query)
        || (matches!(query.sort_order, Some(SortOrder::Desc)) && total.is_none())
    {
        return Ok(None);
    }

    let overfetch_limit = limit.saturating_add(1);
    let window = if let Some(after) = cursor {
        KeyWindow::after(after.clone(), overfetch_limit)
    } else if let Some(total) = total {
        let (asc_offset, asc_limit) =
            page_window_for(query.sort_order.as_ref(), total, offset, overfetch_limit);
        KeyWindow::offset(asc_offset, asc_limit)
    } else {
        KeyWindow::offset(offset, overfetch_limit)
    };
    Ok(Some(window))
}

/// True when the query carries a key-restricted filter.
pub(super) fn can_push_down_key_restricted(query: &Query) -> bool {
    query
        .filter
        .as_ref()
        .is_some_and(HashRangeFilter::is_key_restricted)
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

/// Translate a caller's `(offset, limit)` page request into the ascending
/// `(offset, limit)` window that selects it, honouring `sort_order`.
fn page_window_for(
    sort_order: Option<&SortOrder>,
    total: usize,
    offset: usize,
    limit: usize,
) -> (usize, usize) {
    match sort_order {
        Some(SortOrder::Desc) => {
            let asc_offset = total.saturating_sub(offset.saturating_add(limit));
            let asc_limit = total.saturating_sub(offset).saturating_sub(asc_offset);
            (asc_offset, asc_limit)
        }
        _ => (offset, limit),
    }
}
