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

const DEFAULT_QUERY_LIMIT: usize = 100;
const MAX_QUERY_LIMIT: usize = 1000;

/// Reply shape for a query request.
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
    limit.unwrap_or(DEFAULT_QUERY_LIMIT).min(MAX_QUERY_LIMIT)
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
        Ok(query) => query,
        Err(resp) => return resp,
    };
    render(
        execute_query(host, query, limit, offset, cursor, response_format).await,
        ctx,
    )
}

/// Execute `POST /api/query`: resolve the schema, enforce keyed rules, and render the page.
pub(in crate::exec) async fn execute_query(
    host: &Host,
    query: Query,
    limit: Option<usize>,
    offset: Option<usize>,
    cursor: Option<QueryCursor>,
    response_format: QueryResponseFormat,
) -> Result<Value, HostError> {
    let scope = host
        .db
        .db_ops()
        .read_scope()
        .await
        .map_err(|error| HostError::new(503, error.to_string()))?;
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
    let requested_schema_name = query.schema_name.clone();
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
            .db
            .schema_manager()
            .get_schema_metadata(&query.schema_name)
            .map_err(|error| HostError::internal(error.to_string()))?;
        return Err(rules::full_schema_scan_rejection(
            &requested_schema_name,
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
    if cursor
        .as_ref()
        .is_some_and(|cursor| cursor.record.is_some())
    {
        return Err(HostError::new(
            400,
            "a record cursor continues only an unfiltered read; continue an alternate page with \
             the next_cursor of that page",
        ));
    }
    if response_format == QueryResponseFormat::Count {
        if cursor.is_some() {
            return Err(HostError::new(400, "alternate counts do not use a cursor"));
        }
        let count = host
            .db
            .query_executor()
            .count_alternate_rows(&query)
            .await
            .map_err(HostError::from)?;
        return Ok(serde_json::json!({"total_count": count, "exact": true}));
    }
    let limit = clamp_request_limit(limit);
    let page = host
        .db
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
        .db
        .schema_manager()
        .resolve_schema(name)
        .await
        .map_err(HostError::from)?
    else {
        return Ok(None);
    };
    host.db
        .db_ops()
        .records()
        .key_comparison_context(&schema)
        .map(Some)
        .map_err(HostError::from)
}

fn is_app_owned_schema_ref(name: &str) -> bool {
    let Some((owner_app_id, local_name)) = name.trim().split_once('/') else {
        return false;
    };
    !owner_app_id.is_empty() && !local_name.is_empty() && !local_name.contains('/')
}

async fn count_rows(host: &Host, query: &Query) -> Result<Option<usize>, HostError> {
    host.db
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
            return self.count_only(query).await;
        }
        if rules::can_push_down_key_restricted(&query) {
            return self.key_restricted(query).await;
        }
        self.bounded_fallback(query).await
    }

    async fn count_only(&self, query: Query) -> Result<Value, HostError> {
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
        Ok(serde_json::json!({"total_count": count, "exact": true}))
    }

    async fn key_restricted(&self, query: Query) -> Result<Value, HostError> {
        let host = self.host;
        let (limit, offset) = (self.limit, self.offset);
        let total = if rules::key_restricted_count_is_required(&query, self.cursor.as_ref()) {
            count_rows(host, &query).await?
        } else {
            None
        };
        let Some(window) = rules::keyed_window(&query, limit, offset, self.cursor.as_ref(), total)?
        else {
            return self.bounded_fallback(query).await;
        };

        let uses_cursor = !matches!(query.sort_order, Some(SortOrder::Desc));
        let (page, end) =
            execute_query_rows_windowed(host, query, Some(window), self.response_format).await?;
        Ok(match (uses_cursor, total) {
            (false, Some(total)) => overfetched_offset_payload(page, total, limit, offset),
            (_, total) => {
                let resume = end.resume.map(|PageResume::Key(key)| key);
                let comparison = self.comparison.clone();
                cursor_payload(page, total, limit, offset, resume, comparison)
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
        let executor = host.db.query_executor();
        let resumable = window.is_some() && !matches!(query.sort_order, Some(SortOrder::Desc));
        let (result, end) = match window {
            Some(window) if resumable => {
                fold_db::db_operations::with_query_page_tally(executor.query_window(query, window))
                    .await
            }
            Some(window) => (
                executor.query_window(query, window).await,
                PageEnd::default(),
            ),
            None => (executor.query(query).await, PageEnd::default()),
        };
        let records = result.map_err(HostError::from)?;
        let mut results = format_rows(records, response_format == QueryResponseFormat::Values);
        let range_comparison = KeyComparisonContext::capture_current_range_comparison();
        rules::sort_rows(&mut results, sort_order.as_ref(), range_comparison.as_ref())?;
        Ok((results, end))
    };
    if let Some(comparison) = comparison {
        comparison.run(future).await
    } else {
        future.await
    }
}
