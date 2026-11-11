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
use super::wire::{body_object, parse_query_body, QueryRequest};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

mod format;
mod rules;

pub(in crate::exec) use format::{
    batch_results, execute_query_batch_route, format_alternate_page, parse_query_batch,
    verified_query_page,
};
use format::{cursor_payload, format_rows, materialized_page_payload, overfetched_offset_payload};

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
    let QueryRequest {
        query,
        limit,
        offset,
        cursor,
        response_format,
    } = match body_object(req).and_then(parse_query_body) {
        Ok(q) => q,
        Err(resp) => return resp,
    };
    render(
        execute_query(host, query, limit, offset, cursor, response_format).await,
        ctx,
    )
}

pub(in crate::exec) async fn execute_query(
    host: &Host,
    query: Query,
    limit: Option<usize>,
    offset: Option<usize>,
    cursor: Option<QueryCursor>,
    response_format: QueryResponseFormat,
) -> Result<Value, HostError> {
    let scope = host
        .records()
        .db_ops()
        .read_scope()
        .await
        .map_err(|e| HostError::new(503, e.to_string()))?;
    scope
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
    match resolve_schema_name(host, &query.schema_name)? {
        Some(canonical) => query.schema_name = canonical,
        None if is_app_owned_schema_ref(&query.schema_name) => {
            return Err(HostError::new(
                404,
                format!(
                    "App schema not loaded: {}; declare or load the app schema before querying",
                    query.schema_name
                ),
            ));
        }
        None => {}
    }

    if !query
        .filter
        .as_ref()
        .is_some_and(HashRangeFilter::is_key_restricted)
    {
        let schema = host
            .records()
            .schema_manager()
            .get_schema_metadata(&query.schema_name)
            .map_err(|e| HostError::internal(e.to_string()))?;
        return Err(rules::full_schema_scan_rejection(
            &requested_schema,
            schema.as_ref(),
        ));
    }

    if query.key_name.is_some() {
        return alternate_execute(host, query, limit, offset, cursor, response_format).await;
    }
    rules::validate_query_options(&query, cursor.is_some())?;
    let comparison = capture_key_comparison(host, &query.schema_name).await?;
    let rules::PreparedQuery {
        query,
        cursor,
        binding,
    } = rules::prepare_comparison(query, cursor, comparison.as_deref())?;
    let run = QueryRun {
        host,
        limit: clamp_request_limit(limit),
        offset: offset.unwrap_or(0),
        cursor,
        comparison: binding,
        response_format,
    };
    let future = run.execute(query);
    if let Some(comparison) = comparison {
        comparison.run(future).await
    } else {
        future.await
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
            .await
            .map_err(HostError::from)?;
        return Ok(serde_json::json!({"total_count": count, "exact": true}));
    }
    let limit = clamp_request_limit(limit);
    let page = host
        .records()
        .query_executor()
        .query_alternate_page(query, limit, cursor)
        .await
        .map_err(HostError::from)?;
    format_alternate_page(page, response_format == QueryResponseFormat::Values, limit)
}

async fn capture_key_comparison(
    host: &Host,
    name: &str,
) -> Result<Option<Arc<KeyComparisonContext>>, HostError> {
    let Some(schema) = host
        .records()
        .schema_manager()
        .resolve_schema(name)
        .await
        .map_err(HostError::from)?
    else {
        return Ok(None);
    };
    host.records()
        .db_ops()
        .records()
        .key_comparison_context(&schema)
        .map(Some)
        .map_err(HostError::from)
}

fn is_app_owned_schema_ref(name: &str) -> bool {
    name.trim()
        .split_once('/')
        .is_some_and(|(app, local)| !app.is_empty() && !local.is_empty() && !local.contains('/'))
}

async fn count_rows(host: &Host, query: &Query) -> Result<Option<usize>, HostError> {
    host.records()
        .query_executor()
        .count_query_rows(query)
        .await
        .map_err(HostError::from)
}

struct QueryRun<'a> {
    host: &'a Host,
    limit: usize,
    offset: usize,
    cursor: Option<KeyValue>,
    comparison: Option<String>,
    response_format: QueryResponseFormat,
}

impl QueryRun<'_> {
    async fn execute(self, query: Query) -> Result<Value, HostError> {
        if self.response_format == QueryResponseFormat::Count {
            if !rules::can_push_down_key_restricted(&query)
                || query.include_tombstones
                || self.cursor.is_some()
                || self.offset != 0
            {
                return Err(HostError::new(
                    400,
                    "count replies require keyed live reads without continuation",
                ));
            }
            let count = count_rows(self.host, &query)
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
        if !rules::can_push_down_key_restricted(&query) {
            return self.bounded_fallback(query).await;
        }
        let total = if rules::key_restricted_count_is_required(&query, self.cursor.as_ref()) {
            count_rows(self.host, &query).await?
        } else {
            None
        };
        let Some(window) =
            rules::keyed_window(&query, self.limit, self.offset, self.cursor.as_ref(), total)?
        else {
            return self.bounded_fallback(query).await;
        };
        let uses_cursor = !matches!(query.sort_order, Some(SortOrder::Desc));
        let (page, end) =
            execute_query_rows_windowed(self.host, query, Some(window), self.response_format)
                .await?;
        Ok(match (uses_cursor, total) {
            (false, Some(total)) => {
                overfetched_offset_payload(page, total, self.limit, self.offset)
            }
            (_, total) => {
                let resume = end.resume.map(|PageResume::Key(key)| key);
                cursor_payload(
                    page,
                    total,
                    self.limit,
                    self.offset,
                    resume,
                    self.comparison,
                )
            }
        })
    }

    async fn bounded_fallback(&self, query: Query) -> Result<Value, HostError> {
        let (all_results, _) =
            execute_query_rows_windowed(self.host, query, None, self.response_format).await?;
        Ok(materialized_page_payload(
            all_results,
            self.limit,
            self.offset,
        ))
    }
}

async fn execute_query_rows_windowed(
    host: &Host,
    query: Query,
    window: Option<KeyWindow>,
    response_format: QueryResponseFormat,
) -> Result<(Vec<Value>, PageEnd), HostError> {
    let comparison = capture_key_comparison(host, &query.schema_name).await?;
    let sort_order = query.sort_order.clone();
    let future = async move {
        let records = host.records();
        let executor = records.query_executor();
        let resumable = window.is_some() && !matches!(query.sort_order, Some(SortOrder::Desc));
        let (result, end) = match window {
            Some(w) if resumable => {
                fold_db::db_operations::with_query_page_tally(executor.query_window(query, w)).await
            }
            Some(w) => (executor.query_window(query, w).await, PageEnd::default()),
            None => (executor.query(query).await, PageEnd::default()),
        };
        let mut results = format_rows(
            result.map_err(HostError::from)?,
            response_format == QueryResponseFormat::Values,
        );
        let range_cmp = KeyComparisonContext::capture_current_range_comparison();
        rules::sort_rows(&mut results, sort_order.as_ref(), range_cmp.as_ref())?;
        Ok((results, end))
    };
    if let Some(comparison) = comparison {
        comparison.run(future).await
    } else {
        future.await
    }
}
