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
            "expected_total_count requires a keyed query",
        ));
    }
    if has_cursor
        && (!can_push_down_key_restricted(query)
            || matches!(query.sort_order, Some(SortOrder::Desc)))
    {
        return Err(HostError::new(
            400,
            "Query cursors require ascending keyed reads; use an offset for descending reads",
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
    if let Some(cmp) = comparison {
        query.filter = cmp
            .canonical_filter(query.filter.take())
            .map_err(HostError::from)?;
    }
    let binding = comparison
        .map(|cmp| {
            cmp.cursor_binding(&(&query.filter, query.include_tombstones, &query.sort_order))
        })
        .transpose()
        .map_err(HostError::from)?;
    if let Some(c) = cursor.as_ref() {
        match c.comparison.as_deref() {
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
    if cursor
        .as_ref()
        .map(QueryCursor::record_position)
        .transpose()
        .map_err(|e| HostError::new(400, e))?
        .flatten()
        .is_some()
    {
        return Err(HostError::new(
            400,
            "record cursors are unsupported on /api/query; continue a keyed read with its next_cursor",
        ));
    }
    let cursor = match (cursor.map(QueryCursor::into_key), comparison) {
        (Some(key), Some(cmp)) => Some(cmp.canonical_key(key).map_err(HostError::from)?),
        (cursor, _) => cursor,
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
        if query
            .expected_total_count
            .is_some_and(|expected| expected != total)
        {
            return Err(HostError::new(
                409,
                "expected_total_count did not match the keyed count",
            ));
        }
    } else if query.expected_total_count.is_some() {
        return Err(HostError::new(
            400,
            "expected_total_count is unavailable for this query",
        ));
    }
    let desc = matches!(query.sort_order, Some(SortOrder::Desc));
    let cross_hash = desc
        && matches!(&query.filter, Some(HashRangeFilter::HashRangeKeys(keys))
            if keys.first().is_some_and(|(h, _)| keys.iter().any(|(o, _)| o != h)));
    if cross_hash || (desc && total.is_none()) {
        return Ok(None);
    }
    let overfetch = limit.saturating_add(1);
    let window = if let Some(after) = cursor {
        KeyWindow::after(after.clone(), overfetch)
    } else if let (true, Some(total)) = (desc, total) {
        let asc_offset = total.saturating_sub(offset.saturating_add(overfetch));
        let asc_limit = total.saturating_sub(offset).saturating_sub(asc_offset);
        KeyWindow::offset(asc_offset, asc_limit)
    } else {
        KeyWindow::offset(offset, overfetch)
    };
    Ok(Some(window))
}

pub(super) fn can_push_down_key_restricted(query: &Query) -> bool {
    query
        .filter
        .as_ref()
        .is_some_and(HashRangeFilter::is_key_restricted)
}

pub(super) fn key_restricted_count_is_required(query: &Query, cursor: Option<&KeyValue>) -> bool {
    query.expected_total_count.is_some()
        || (cursor.is_none() && matches!(query.sort_order.as_ref(), Some(SortOrder::Desc)))
}

fn looks_like_identity(v: &str) -> bool {
    v.len() == 64 && v.bytes().all(|b| b.is_ascii_hexdigit())
}

pub(super) fn full_schema_scan_rejection(requested: &str, schema: Option<&Schema>) -> HostError {
    let schema_name = schema
        .and_then(|s| s.descriptive_name.clone())
        .or_else(|| (!looks_like_identity(requested)).then(|| requested.to_string()))
        .or_else(|| schema.and_then(|s| (!looks_like_identity(&s.name)).then(|| s.name.clone())))
        .or_else(|| {
            schema
                .and_then(|s| s.owner_app_id.as_deref())
                .map(|o| format!("{o}/<unnamed-schema>"))
        })
        .unwrap_or_else(|| "<unnamed-product-schema>".to_string());
    let schema_id = schema.map_or(requested, |s| s.name.as_str());
    let record_keys = schema.and_then(|s| s.record_keys.as_ref());
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
    if let Some(cmp) = range_comparison {
        for row in &*rows {
            cmp.validate_value(row["key"]["range"].as_str().unwrap_or(""))
                .map_err(SchemaError::InvalidData)?;
        }
    }
    let mut error = None;
    rows.sort_by(
        |a, b| match compare_rows(a, b, sort_order, range_comparison) {
            Ok(order) => order,
            Err(detail) => {
                error.get_or_insert(detail);
                Ordering::Equal
            }
        },
    );
    error.map_or(Ok(()), |e| Err(e.into()))
}

fn compare_rows(
    a: &Value,
    b: &Value,
    sort_order: Option<&SortOrder>,
    range_comparison: Option<&RangeKeyComparison>,
) -> Result<Ordering, SchemaError> {
    let (ar, br) = (
        a["key"]["range"].as_str().unwrap_or(""),
        b["key"]["range"].as_str().unwrap_or(""),
    );
    let range_order = match range_comparison {
        Some(cmp) => cmp.compare(ar, br).map_err(SchemaError::InvalidData)?,
        None => ar.cmp(br),
    };
    let primary = if matches!(sort_order, Some(SortOrder::Desc)) {
        range_order.reverse()
    } else {
        range_order
    };
    Ok(primary.then(range_order).then_with(|| {
        a["key"]["hash"]
            .as_str()
            .unwrap_or("")
            .cmp(b["key"]["hash"].as_str().unwrap_or(""))
    }))
}
