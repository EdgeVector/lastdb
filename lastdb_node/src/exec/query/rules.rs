//! Query selection, keyed-page rules, no-scan guard, and storage-free row order.

use std::cmp::Ordering;

use fold_db::schema::types::field::{HashRangeFilter, KeyWindow};
use fold_db::schema::types::key_comparison::{KeyComparisonContext, RangeKeyComparison};
use fold_db::schema::types::operations::{Query, SortOrder};
use fold_db::schema::types::{KeyValue, QueryCursor, Schema, SchemaError};
use serde_json::Value;

use crate::exec::reply::HostError;

const KEYED_FILTER_VARIANTS: &[&str] = &[
    "HashKey",
    "HashRangeKey",
    "HashRangePrefix",
    "HashRangeRange",
    "HashRangeKeys",
];

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

pub(super) struct PreparedQuery {
    pub(super) query: Query,
    pub(super) cursor: Option<KeyValue>,
    pub(super) binding: Option<String>,
}

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
    let cursor = if let Some(comparison) = comparison {
        cursor
            .map(|key| comparison.canonical_key(key))
            .transpose()
            .map_err(HostError::from)?
    } else {
        cursor
    };
    Ok(PreparedQuery {
        query,
        cursor,
        binding,
    })
}

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

pub(super) fn can_push_down_key_restricted(query: &Query) -> bool {
    query
        .filter
        .as_ref()
        .is_some_and(HashRangeFilter::is_key_restricted)
}

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

pub(super) fn key_restricted_count_is_required(query: &Query, cursor: Option<&KeyValue>) -> bool {
    query.expected_total_count.is_some()
        || (cursor.is_none() && matches!(query.sort_order.as_ref(), Some(SortOrder::Desc)))
}

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

fn looks_like_schema_identity(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn product_schema_name(requested: &str, schema: Option<&Schema>) -> String {
    schema
        .and_then(|schema| schema.descriptive_name.clone())
        .or_else(|| (!looks_like_schema_identity(requested)).then(|| requested.to_string()))
        .or_else(|| {
            schema.and_then(|schema| {
                (!looks_like_schema_identity(&schema.name)).then(|| schema.name.clone())
            })
        })
        .or_else(|| {
            schema
                .and_then(|schema| schema.owner_app_id.as_deref())
                .map(|owner| format!("{owner}/<unnamed-schema>"))
        })
        .unwrap_or_else(|| "<unnamed-product-schema>".to_string())
}

pub(super) fn full_schema_scan_rejection(requested: &str, schema: Option<&Schema>) -> HostError {
    let schema_name = product_schema_name(requested, schema);
    let schema_id = schema.map_or(requested, |schema| schema.name.as_str());
    let record_keys = schema.and_then(|schema| schema.record_keys.as_ref());
    let message = format!(
        "full_schema_scan_not_allowed: unfiltered query on product schema \
         '{schema_name}' is not supported; add a keyed filter using the \
         structured remediation"
    );
    let body = serde_json::json!({
        "ok": false,
        "kind": "full_schema_scan_not_allowed",
        "error": "product apps are keyed-access only; unfiltered schema queries are not supported",
        "message": message,
        "schema_name": schema_name,
        "schema_id": schema_id,
        "remediation": {
            "action": "add_key_filter",
            "request_key": "filter",
            "record_keys": record_keys,
            "filter_variants": KEYED_FILTER_VARIANTS,
        }
    });
    HostError::structured(400, message, body)
}

pub(super) fn sort_rows(
    rows: &mut [Value],
    sort_order: Option<&SortOrder>,
    range_comparison: Option<&RangeKeyComparison>,
) -> Result<(), HostError> {
    if let Some(comparison) = range_comparison {
        for row in &*rows {
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
    error.map_or(Ok(()), |error| Err(error.into()))
}

fn compare_rows(
    a: &Value,
    b: &Value,
    sort_order: Option<&SortOrder>,
    range_comparison: Option<&RangeKeyComparison>,
) -> Result<Ordering, SchemaError> {
    let a_range = a["key"]["range"].as_str().unwrap_or("");
    let b_range = b["key"]["range"].as_str().unwrap_or("");
    let range_order = if let Some(comparison) = range_comparison {
        comparison
            .compare(a_range, b_range)
            .map_err(SchemaError::InvalidData)?
    } else {
        a_range.cmp(b_range)
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
