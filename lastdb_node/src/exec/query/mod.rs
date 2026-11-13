//! Query routes, batch query execution, and keyed/alternate read flows.

use std::sync::Arc;

use fold_db::access::AccessContext;
use fold_db::db_operations::{PageEnd, PageResume};
use fold_db::schema::types::field::{HashRangeFilter, KeyWindow};
use fold_db::schema::types::key_comparison::KeyComparisonContext;
use fold_db::schema::types::operations::SortOrder;
use fold_db::schema::types::{KeyValue, Query, QueryCursor};
use serde::Deserialize;
use serde_json::{json, Value};

use super::reply::{render, HostError};
use super::schema_resolve::resolve_schema_name;
use super::wire::{body_object, parse_query_body, QueryRequest};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

mod format;

pub(in crate::exec) use format::{
    batch_results, format_alternate_page, parse_query_batch, query_batch_route, verified_query_page,
};
use format::{
    cursor_payload, format_rows, full_schema_scan_rejection, materialized_page_payload, sort_rows,
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

pub(in crate::exec) async fn query_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    match body_object(req).and_then(parse_query_body) {
        Ok(q) => render(execute_query(host, q).await, ctx),
        Err(resp) => resp,
    }
}

pub(in crate::exec) async fn execute_query(
    host: &Host,
    req: QueryRequest,
) -> Result<Value, HostError> {
    host.records()
        .db_ops()
        .read_scope()
        .await
        .map_err(|e| HostError::new(503, e.to_string()))?
        .run_result(Box::pin(execute_query_scoped(host, req)))
        .await
}

async fn execute_query_scoped(host: &Host, mut req: QueryRequest) -> Result<Value, HostError> {
    let requested = req.query.schema_name.clone();
    if let Some(canonical) = resolve_schema_name(host, &req.query.schema_name)? {
        req.query.schema_name = canonical;
    } else if req
        .query
        .schema_name
        .trim()
        .split_once('/')
        .is_some_and(|(a, l)| !a.is_empty() && !l.is_empty() && !l.contains('/'))
    {
        return Err(HostError::new(
            404,
            format!(
                "App schema not loaded: {}; declare or load the app schema before querying",
                req.query.schema_name
            ),
        ));
    }
    if req.query.filter.is_none() {
        let schema = host
            .records()
            .schema_manager()
            .get_schema_metadata(&req.query.schema_name)
            .map_err(|e| HostError::internal(e.to_string()))?;
        return Err(full_schema_scan_rejection(&requested, schema.as_ref()));
    }
    if req.query.key_name.is_some() {
        return alternate_execute(host, req).await;
    }
    if req.cursor.is_some() && matches!(req.query.sort_order, Some(SortOrder::Desc)) {
        return Err(HostError::new(
            400,
            "Query cursors require ascending keyed reads; use an offset for descending reads",
        ));
    }
    let cmp: Option<Arc<KeyComparisonContext>> = host
        .records()
        .schema_manager()
        .resolve_schema(&req.query.schema_name)
        .await?
        .map(|s| host.records().db_ops().records().key_comparison_context(&s))
        .transpose()?;
    let (query, cursor, binding) = prepare_comparison(req.query, req.cursor, cmp.as_deref())?;
    let fut = execute_primary_query(
        host,
        query,
        clamp_request_limit(req.limit),
        req.offset.unwrap_or(0),
        cursor.as_ref(),
        binding,
        req.response_format,
    );
    if let Some(c) = cmp {
        c.run(fut).await
    } else {
        fut.await
    }
}

async fn alternate_execute(host: &Host, req: QueryRequest) -> Result<Value, HostError> {
    if req.offset.unwrap_or_default() != 0 {
        return Err(HostError::new(
            400,
            "alternate pages use cursors rather than offsets",
        ));
    }
    if req.cursor.as_ref().is_some_and(|c| c.record.is_some()) {
        return Err(HostError::new(
            400,
            "a record cursor continues only an unfiltered read; continue an alternate page with the next_cursor of that page",
        ));
    }
    let records = host.records();
    let exec = records.query_executor();
    if req.response_format == QueryResponseFormat::Count {
        if req.cursor.is_some() {
            return Err(HostError::new(400, "alternate counts do not use a cursor"));
        }
        return Ok(
            json!({"total_count": exec.count_alternate_rows(&req.query).await?, "exact": true}),
        );
    }
    let limit = clamp_request_limit(req.limit);
    format_alternate_page(
        exec.query_alternate_page(req.query, limit, req.cursor)
            .await?,
        req.response_format == QueryResponseFormat::Values,
        limit,
    )
}

async fn execute_primary_query(
    host: &Host,
    query: Query,
    limit: usize,
    offset: usize,
    cursor: Option<&KeyValue>,
    binding: Option<String>,
    fmt: QueryResponseFormat,
) -> Result<Value, HostError> {
    let records = host.records();
    let exec = records.query_executor();
    if fmt == QueryResponseFormat::Count {
        if query.include_tombstones || cursor.is_some() || offset != 0 {
            return Err(HostError::new(
                400,
                "count replies require keyed live reads without continuation",
            ));
        }
        let count = exec
            .count_query_rows(&query)
            .await?
            .ok_or_else(|| HostError::new(400, "exact keyed count is unavailable"))?;
        if query.expected_total_count.is_some_and(|exp| exp != count) {
            return Err(HostError::new(
                409,
                "expected_total_count differs from the exact keyed count",
            ));
        }
        return Ok(json!({"total_count": count, "exact": true}));
    }
    let desc = matches!(query.sort_order, Some(SortOrder::Desc));
    let total = if query.expected_total_count.is_some() || (cursor.is_none() && desc) {
        exec.count_query_rows(&query).await?
    } else {
        None
    };
    let Some(window) = keyed_window(&query, limit, offset, cursor, total)? else {
        let (all, _) = execute_query_rows(host, query, None, fmt).await?;
        return Ok(materialized_page_payload(all, limit, offset));
    };
    let (page, end) = execute_query_rows(host, query, Some(window), fmt).await?;
    Ok(if desc && total.is_some() {
        cursor_payload(page, total, (limit, offset), None, None, false)
    } else {
        cursor_payload(
            page,
            total,
            (limit, offset),
            end.resume.map(|PageResume::Key(k)| k),
            binding,
            true,
        )
    })
}

async fn execute_query_rows(
    host: &Host,
    query: Query,
    window: Option<KeyWindow>,
    fmt: QueryResponseFormat,
) -> Result<(Vec<Value>, PageEnd), HostError> {
    let sort_order = query.sort_order.clone();
    let records = host.records();
    let exec = records.query_executor();
    let (result, end) = match window {
        Some(w) if !matches!(sort_order, Some(SortOrder::Desc)) => {
            fold_db::db_operations::with_query_page_tally(exec.query_window(query, w)).await
        }
        Some(w) => (exec.query_window(query, w).await, PageEnd::default()),
        None => (exec.query(query).await, PageEnd::default()),
    };
    let mut results = format_rows(result?, fmt == QueryResponseFormat::Values);
    sort_rows(
        &mut results,
        sort_order.as_ref(),
        KeyComparisonContext::capture_current_range_comparison().as_ref(),
    )?;
    Ok((results, end))
}

fn prepare_comparison(
    mut query: Query,
    cursor: Option<QueryCursor>,
    cmp: Option<&KeyComparisonContext>,
) -> Result<(Query, Option<KeyValue>, Option<String>), HostError> {
    if let Some(c) = cmp {
        query.filter = c.canonical_filter(query.filter.take())?;
    }
    let binding = cmp
        .map(|c| c.cursor_binding(&(&query.filter, query.include_tombstones, &query.sort_order)))
        .transpose()?;
    if let Some(c) = cursor.as_ref() {
        match c.comparison.as_deref() {
            Some(recv) if binding.as_deref() != Some(recv) => {
                return Err(HostError::new(
                    409,
                    "cursor comparison context changed; restart from the first page",
                ));
            }
            None if cmp.is_some() => {
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
    let cursor = cursor
        .map(|c| {
            let k = c.into_key();
            if let Some(cx) = cmp {
                cx.canonical_key(k)
            } else {
                Ok(k)
            }
        })
        .transpose()?;
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
        (Some(t), Some(exp)) if exp != t => {
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
    } else if let (true, Some(t)) = (desc, total) {
        let asc_off = t.saturating_sub(offset.saturating_add(overfetch));
        KeyWindow::offset(asc_off, t.saturating_sub(offset).saturating_sub(asc_off))
    } else {
        KeyWindow::offset(offset, overfetch)
    }))
}
