//! Query routes, batch query execution, and keyed/alternate read flows.

use std::sync::Arc;

use fold_db::access::AccessContext;
use fold_db::db_operations::{PageEnd, PageResume};
use fold_db::schema::types::field::{HashRangeFilter, KeyWindow};
use fold_db::schema::types::key_comparison::KeyComparisonContext;
use fold_db::schema::types::operations::SortOrder;
use fold_db::schema::types::{KeyValue, Query, QueryCursor};
use serde::Deserialize;
use serde_json::Value;

use super::reply::{render, HostError};
use super::schema_resolve::resolve_schema_name;
use super::wire::{body_object, parse_query_body};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

mod format;

pub(in crate::exec) use format::{
    batch_results, execute_query_batch_route, format_alternate_page, parse_query_batch,
    verified_query_page,
};
use format::{
    cursor_payload, format_rows, full_schema_scan_rejection, materialized_page_payload,
    overfetched_offset_payload, sort_rows,
};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::exec) enum QueryResponseFormat {
    #[default]
    Full,
    Values,
    Count,
}

#[must_use]
pub(in crate::exec) fn clamp_request_limit(limit: Option<usize>) -> usize {
    limit.unwrap_or(100).min(1000)
}

pub(in crate::exec) async fn execute_query_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    match body_object(req).and_then(parse_query_body) {
        Ok(q) => render(
            execute_query(
                host,
                q.query,
                q.limit,
                q.offset,
                q.cursor,
                q.response_format,
            )
            .await,
            ctx,
        ),
        Err(resp) => resp,
    }
}

pub(in crate::exec) async fn execute_query(
    host: &Host,
    query: Query,
    limit: Option<usize>,
    offset: Option<usize>,
    cursor: Option<QueryCursor>,
    response_format: QueryResponseFormat,
) -> Result<Value, HostError> {
    host.records()
        .db_ops()
        .read_scope()
        .await
        .map_err(|e| HostError::new(503, e.to_string()))?
        .run_result(Box::pin(execute_query_scoped(
            host,
            query,
            limit,
            offset,
            cursor,
            response_format,
        )))
        .await
}

async fn execute_query_scoped(
    host: &Host,
    mut query: Query,
    limit: Option<usize>,
    offset: Option<usize>,
    cursor: Option<QueryCursor>,
    response_format: QueryResponseFormat,
) -> Result<Value, HostError> {
    let requested_schema = query.schema_name.clone();
    if let Some(canonical) = resolve_schema_name(host, &query.schema_name)? {
        query.schema_name = canonical;
    } else if query
        .schema_name
        .trim()
        .split_once('/')
        .is_some_and(|(app, local)| !app.is_empty() && !local.is_empty() && !local.contains('/'))
    {
        return Err(HostError::new(
            404,
            format!(
                "App schema not loaded: {}; declare or load the app schema before querying",
                query.schema_name
            ),
        ));
    }

    if query.filter.is_none() {
        let schema = host
            .records()
            .schema_manager()
            .get_schema_metadata(&query.schema_name)
            .map_err(|e| HostError::internal(e.to_string()))?;
        return Err(full_schema_scan_rejection(
            &requested_schema,
            schema.as_ref(),
        ));
    }
    if query.key_name.is_some() {
        return alternate_execute(host, query, limit, offset, cursor, response_format).await;
    }
    if cursor.is_some() && matches!(query.sort_order, Some(SortOrder::Desc)) {
        return Err(HostError::new(
            400,
            "Query cursors require ascending keyed reads; use an offset for descending reads",
        ));
    }
    let comparison = capture_key_comparison(host, &query.schema_name).await?;
    let (query, cursor, binding) = prepare_comparison(query, cursor, comparison.as_deref())?;
    let future = execute_primary_query(
        host,
        query,
        clamp_request_limit(limit),
        offset.unwrap_or(0),
        cursor,
        binding,
        response_format,
    );
    match comparison {
        Some(comparison) => comparison.run(future).await,
        None => future.await,
    }
}

async fn alternate_execute(
    host: &Host,
    query: Query,
    limit: Option<usize>,
    offset: Option<usize>,
    cursor: Option<QueryCursor>,
    response_format: QueryResponseFormat,
) -> Result<Value, HostError> {
    if offset.unwrap_or_default() != 0 {
        return Err(HostError::new(
            400,
            "alternate pages use cursors rather than offsets",
        ));
    }
    if cursor.as_ref().is_some_and(|c| c.record.is_some()) {
        return Err(HostError::new(
            400,
            "a record cursor continues only an unfiltered read; continue an alternate page with the next_cursor of that page",
        ));
    }
    if response_format == QueryResponseFormat::Count {
        if cursor.is_some() {
            return Err(HostError::new(400, "alternate counts do not use a cursor"));
        }
        let count = host
            .records()
            .query_executor()
            .count_alternate_rows(&query)
            .await?;
        return Ok(serde_json::json!({"total_count": count, "exact": true}));
    }
    let limit = clamp_request_limit(limit);
    let page = host
        .records()
        .query_executor()
        .query_alternate_page(query, limit, cursor)
        .await?;
    format_alternate_page(page, response_format == QueryResponseFormat::Values, limit)
}

async fn capture_key_comparison(
    host: &Host,
    name: &str,
) -> Result<Option<Arc<KeyComparisonContext>>, HostError> {
    let Some(schema) = host.records().schema_manager().resolve_schema(name).await? else {
        return Ok(None);
    };
    host.records()
        .db_ops()
        .records()
        .key_comparison_context(&schema)
        .map(Some)
        .map_err(HostError::from)
}

async fn execute_primary_query(
    host: &Host,
    query: Query,
    limit: usize,
    offset: usize,
    cursor: Option<KeyValue>,
    binding: Option<String>,
    response_format: QueryResponseFormat,
) -> Result<Value, HostError> {
    if response_format == QueryResponseFormat::Count {
        if query.include_tombstones || cursor.is_some() || offset != 0 {
            return Err(HostError::new(
                400,
                "count replies require keyed live reads without continuation",
            ));
        }
        let count = host
            .records()
            .query_executor()
            .count_query_rows(&query)
            .await?
            .ok_or_else(|| HostError::new(400, "exact keyed count is unavailable"))?;
        if query
            .expected_total_count
            .is_some_and(|expected| expected != count)
        {
            return Err(HostError::new(
                409,
                "expected_total_count differs from the exact keyed count",
            ));
        }
        return Ok(serde_json::json!({"total_count": count, "exact": true}));
    }
    let desc = matches!(query.sort_order, Some(SortOrder::Desc));
    let total = if query.expected_total_count.is_some() || (cursor.is_none() && desc) {
        host.records()
            .query_executor()
            .count_query_rows(&query)
            .await?
    } else {
        None
    };
    let Some(window) = keyed_window(&query, limit, offset, cursor.as_ref(), total)? else {
        let (all_results, _) =
            execute_query_rows_windowed(host, query, None, response_format).await?;
        return Ok(materialized_page_payload(all_results, limit, offset));
    };
    let (page, end) =
        execute_query_rows_windowed(host, query, Some(window), response_format).await?;
    Ok(match (!desc, total) {
        (false, Some(total)) => overfetched_offset_payload(page, total, limit, offset),
        (_, total) => cursor_payload(
            page,
            total,
            limit,
            offset,
            end.resume.map(|PageResume::Key(key)| key),
            binding,
        ),
    })
}

async fn execute_query_rows_windowed(
    host: &Host,
    query: Query,
    window: Option<KeyWindow>,
    response_format: QueryResponseFormat,
) -> Result<(Vec<Value>, PageEnd), HostError> {
    let sort_order = query.sort_order.clone();
    let db = host.records();
    let executor = db.query_executor();
    let resumable = window.is_some() && !matches!(sort_order, Some(SortOrder::Desc));
    let (result, end) = match window {
        Some(w) if resumable => {
            fold_db::db_operations::with_query_page_tally(executor.query_window(query, w)).await
        }
        Some(w) => (executor.query_window(query, w).await, PageEnd::default()),
        None => (executor.query(query).await, PageEnd::default()),
    };
    let mut results = format_rows(result?, response_format == QueryResponseFormat::Values);
    let range_cmp = KeyComparisonContext::capture_current_range_comparison();
    sort_rows(&mut results, sort_order.as_ref(), range_cmp.as_ref())?;
    Ok((results, end))
}

fn prepare_comparison(
    mut query: Query,
    cursor: Option<QueryCursor>,
    comparison: Option<&KeyComparisonContext>,
) -> Result<(Query, Option<KeyValue>, Option<String>), HostError> {
    if let Some(cmp) = comparison {
        query.filter = cmp.canonical_filter(query.filter.take())?;
    }
    let binding = comparison
        .map(|cmp| {
            cmp.cursor_binding(&(&query.filter, query.include_tombstones, &query.sort_order))
        })
        .transpose()?;
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
        if c.record_position()
            .map_err(|e| HostError::new(400, e))?
            .is_some()
        {
            return Err(HostError::new(
                400,
                "record cursors are unsupported on /api/query; continue a keyed read with its next_cursor",
            ));
        }
    }
    let cursor = match (cursor.map(QueryCursor::into_key), comparison) {
        (Some(key), Some(cmp)) => Some(cmp.canonical_key(key)?),
        (cursor, _) => cursor,
    };
    Ok((query, cursor, binding))
}

fn keyed_window(
    query: &Query,
    limit: usize,
    offset: usize,
    cursor: Option<&KeyValue>,
    total: Option<usize>,
) -> Result<Option<KeyWindow>, HostError> {
    match (total, query.expected_total_count) {
        (Some(total), Some(expected)) if expected != total => {
            return Err(HostError::new(
                409,
                "expected_total_count did not match the keyed count",
            ));
        }
        (None, Some(_)) => {
            return Err(HostError::new(
                400,
                "expected_total_count is unavailable for this query",
            ));
        }
        _ => {}
    }
    let desc = matches!(query.sort_order, Some(SortOrder::Desc));
    let cross_hash = desc
        && matches!(&query.filter, Some(HashRangeFilter::HashRangeKeys(keys))
            if keys.first().is_some_and(|(h, _)| keys.iter().any(|(o, _)| o != h)));
    if cross_hash || (desc && total.is_none()) {
        return Ok(None);
    }
    let overfetch = limit.saturating_add(1);
    Ok(Some(if let Some(after) = cursor {
        KeyWindow::after(after.clone(), overfetch)
    } else if let (true, Some(total)) = (desc, total) {
        let asc_offset = total.saturating_sub(offset.saturating_add(overfetch));
        KeyWindow::offset(
            asc_offset,
            total.saturating_sub(offset).saturating_sub(asc_offset),
        )
    } else {
        KeyWindow::offset(offset, overfetch)
    }))
}
