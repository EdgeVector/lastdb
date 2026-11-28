//! Keyed and alternate reads used by the app API.

use std::sync::Arc;

use fold_db::schema::types::field::KeyWindow;
use fold_db::schema::types::key_comparison::KeyComparisonContext;
use fold_db::schema::types::{KeyValue, Query, QueryCursor};
use serde_json::Value;

use super::reply::HostError;
use super::schema_routes::resolve_schema_name;
use super::wire::QueryRequest;
use crate::host::Host;

mod format;

use format::{
    cursor_payload, format_alternate_keys, format_alternate_page, format_rows, key_rows, sort_rows,
};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec) enum QueryResponseFormat {
    #[default]
    Full,
    Values,
    /// Each row holds only its `key` (and `matched_key` through an alternate).
    /// The records are still read and checked; their fields are not copied.
    Keys,
}

fn clamp_request_limit(limit: Option<usize>) -> usize {
    limit.unwrap_or(100).min(1000)
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
    if req.offset.unwrap_or(0) != 0 {
        return Err(HostError::new(
            400,
            "offset paging is not supported; use a cursor",
        ));
    }
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
        return Err(HostError::new(400, "a query requires a key filter"));
    }
    if req.query.key_name.is_some() {
        return alternate_execute(host, req).await;
    }
    let cmp: Option<Arc<KeyComparisonContext>> = host
        .records()
        .schema_manager()
        .resolve_schema(&req.query.schema_name)
        .await?
        .map(|s| host.records().db_ops().records().key_comparison_context(&s))
        .transpose()?;
    let (query, cursor, binding) = prepare_comparison(req.query, req.cursor, cmp.as_deref())?;
    let limit = clamp_request_limit(req.limit);
    let fut = execute_primary_query(host, query, limit, cursor, binding, req.response_format);
    if let Some(c) = cmp {
        c.run(fut).await
    } else {
        fut.await
    }
}

async fn alternate_execute(host: &Host, req: QueryRequest) -> Result<Value, HostError> {
    let records = host.records();
    let exec = records.query_executor();
    let limit = clamp_request_limit(req.limit);
    if req.response_format == QueryResponseFormat::Keys {
        let page = exec
            .query_alternate_key_page(req.query, limit, req.cursor)
            .await?;
        return format_alternate_keys(page, limit);
    }
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
    cursor: Option<KeyValue>,
    binding: Option<String>,
    fmt: QueryResponseFormat,
) -> Result<Value, HostError> {
    let overfetch = limit.saturating_add(1);
    let window = match cursor {
        Some(after) => KeyWindow::after(after, overfetch),
        None => KeyWindow::offset(0, overfetch),
    };
    let records = host.records();
    let exec = records.query_executor();
    let (mut results, end) = if fmt == QueryResponseFormat::Keys {
        let (keys, end) =
            fold_db::db_operations::with_query_page_tally(exec.query_window_keys(query, window))
                .await;
        (key_rows(keys?), end)
    } else {
        let (result, end) =
            fold_db::db_operations::with_query_page_tally(exec.query_window(query, window)).await;
        (
            format_rows(result?, fmt == QueryResponseFormat::Values),
            end,
        )
    };
    sort_rows(
        &mut results,
        KeyComparisonContext::capture_current_range_comparison().as_ref(),
    )?;
    Ok(cursor_payload(results, limit, end.resume, binding))
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
        .map(|c| c.cursor_binding(&(&query.filter, &query.sort_order)))
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
    }
    let cursor = cursor
        .map(|c| {
            if let Some(cx) = cmp {
                cx.canonical_key(c.into_key())
            } else {
                Ok(c.into_key())
            }
        })
        .transpose()?;
    Ok((query, cursor, binding))
}
