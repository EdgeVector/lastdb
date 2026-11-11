//! Owned row/page response assembly and `POST /api/queries/batch` execution.

use std::collections::HashMap;

use fold_db::access::AccessContext;
use fold_db::fold_db_core::query::query_executor::AlternatePage;
use fold_db::schema::types::{KeyValue, QueryCursor};
use serde::Deserialize;
use serde_json::Value;

use super::execute_query;
use crate::exec::reject::Reject;
use crate::exec::reply::{error_response, render, HostError};
use crate::exec::wire::{body_object, parse_query_body, QueryRequest};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

const QUERY_BATCH_MAX_QUERIES: usize = 64;
const QUERY_BATCH_CONCURRENCY: usize = 4;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryBatchRequest {
    queries: Vec<Value>,
}

/// `POST /api/queries/batch` — several independent queries in one request.
pub(in crate::exec) async fn execute_query_batch_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    use futures::StreamExt as _;

    let queries = match parse_query_batch(req, ctx) {
        Ok(queries) => queries,
        Err(response) => return response,
    };

    let items = queries
        .into_iter()
        .enumerate()
        .map(|(index, query)| async move {
            let Value::Object(obj) = query else {
                return (index, Reject::malformed_body().response());
            };
            let QueryRequest {
                query,
                limit,
                offset,
                cursor,
                response_format,
            } = match parse_query_body(obj) {
                Ok(query) => query,
                Err(response) => return (index, response),
            };
            let response = render(
                execute_query(host, query, limit, offset, cursor, response_format).await,
                ctx,
            );
            (index, response)
        });
    let scope = match host.db.db_ops().read_scope().await {
        Ok(scope) => scope,
        Err(error) => return error_response(503, &error.to_string(), ctx),
    };
    let future = futures::stream::iter(items)
        .buffer_unordered(QUERY_BATCH_CONCURRENCY)
        .collect();
    let finished: Vec<(usize, UdsResponse)> = scope.run_shared(future).await;

    let results = batch_results(finished);
    render(
        Ok(serde_json::json!({ "count": results.len(), "results": results })),
        ctx,
    )
}

pub(in crate::exec) fn batch_results(mut finished: Vec<(usize, UdsResponse)>) -> Vec<Value> {
    finished.sort_by_key(|(index, _)| *index);
    finished
        .into_iter()
        .map(|(_, response)| {
            let body = serde_json::from_slice::<Value>(&response.body).unwrap_or_else(|_| {
                Value::String(String::from_utf8_lossy(&response.body).into_owned())
            });
            let mut entry = serde_json::json!({ "status": response.status });
            entry["response"] = body;
            entry
        })
        .collect()
}

pub(in crate::exec) fn parse_query_batch(
    req: &UdsRequest,
    ctx: &AccessContext,
) -> Result<Vec<Value>, UdsResponse> {
    let obj = body_object(req)?;
    let queries = serde_json::from_value::<QueryBatchRequest>(Value::Object(obj))
        .map_err(|error| Reject::from_serde_error(&error, None).response())?
        .queries;
    if queries.is_empty() {
        return Err(error_response(
            400,
            "Query batch must contain at least one query",
            ctx,
        ));
    }
    if queries.len() > QUERY_BATCH_MAX_QUERIES {
        return Err(error_response(
            400,
            &format!(
                "Query batch carries {} queries; the limit is {QUERY_BATCH_MAX_QUERIES}",
                queries.len()
            ),
            ctx,
        ));
    }
    Ok(queries)
}

fn page_payload(
    page: Vec<Value>,
    total_count: Option<usize>,
    limit: usize,
    offset: usize,
    has_more: bool,
    key_form: Option<&'static str>,
) -> Value {
    let returned_count = page.len();
    Value::Object(serde_json::Map::from_iter([
        ("results".into(), Value::Array(page)),
        (
            "total_count".into(),
            total_count.map_or(Value::Null, Value::from),
        ),
        ("returned_count".into(), Value::from(returned_count)),
        ("limit".into(), Value::from(limit)),
        ("offset".into(), Value::from(offset)),
        ("has_more".into(), Value::from(has_more)),
        ("next_cursor".into(), Value::Null),
        ("key_form".into(), key_form.map_or(Value::Null, Value::from)),
    ]))
}

pub(super) fn cursor_payload(
    mut page: Vec<Value>,
    total_count: Option<usize>,
    limit: usize,
    offset: usize,
    resume: Option<KeyValue>,
    comparison: Option<String>,
) -> Value {
    let has_more = page.len() > limit || resume.is_some();
    if page.len() > limit {
        page.truncate(limit);
    }
    let next_cursor = resume
        .or_else(|| next_cursor_for(&page, has_more))
        .map(|key| QueryCursor::new(key, comparison));
    let mut payload = page_payload(page, total_count, limit, offset, has_more, None);
    payload["next_cursor"] = serde_json::to_value(next_cursor).unwrap_or(Value::Null);
    payload
}

pub(super) fn overfetched_offset_payload(
    mut page: Vec<Value>,
    total_count: usize,
    limit: usize,
    offset: usize,
) -> Value {
    let has_more = page.len() > limit;
    if page.len() > limit {
        page.truncate(limit);
    }
    page_payload(page, Some(total_count), limit, offset, has_more, None)
}

pub(super) fn materialized_page_payload(results: Vec<Value>, limit: usize, offset: usize) -> Value {
    let total_count = results.len();
    let page: Vec<_> = results.into_iter().skip(offset).take(limit).collect();
    let has_more = offset.saturating_add(page.len()) < total_count;
    page_payload(page, Some(total_count), limit, offset, has_more, None)
}

fn next_cursor_for(page: &[Value], has_more: bool) -> Option<KeyValue> {
    if !has_more {
        return None;
    }
    page.last()
        .and_then(|row| row.get("key"))
        .and_then(|key| serde_json::from_value::<KeyValue>(key.clone()).ok())
}

pub(super) fn format_rows(
    records: fold_db::fold_db_core::query::QueryRecords,
    compact: bool,
) -> Vec<Value> {
    records
        .into_iter()
        .map(|(key, record)| format_row(&key, record, compact))
        .collect()
}

fn format_row(
    key: &KeyValue,
    record: fold_db::fold_db_core::query::Record,
    compact: bool,
) -> Value {
    let mut row = serde_json::Map::new();
    row.insert("key".into(), serde_json::json!(key));
    row.insert("revision".into(), Value::String(record.revision));
    row.insert("alternates".into(), serde_json::json!(record.alternates));
    row.insert(
        "fields".into(),
        Value::Object(record.fields.into_iter().collect()),
    );
    if !compact {
        row.insert("metadata".into(), serde_json::json!(record.metadata));
    }
    Value::Object(row)
}

/// Render an already ordered page read from one verified database view.
pub(in crate::exec) fn verified_query_page(
    records: Vec<(KeyValue, fold_db::fold_db_core::query::Record)>,
    compact: bool,
    total_count: Option<usize>,
    limit: usize,
    offset: usize,
    has_more: bool,
    next_cursor: Option<QueryCursor>,
) -> Value {
    let key_form = (!records.is_empty()).then_some("api");
    let rows = records
        .into_iter()
        .map(|(key, record)| format_row(&key, record, compact))
        .collect();
    let mut payload = page_payload(rows, total_count, limit, offset, has_more, key_form);
    payload["next_cursor"] = serde_json::to_value(next_cursor).unwrap_or(Value::Null);
    payload
}

/// Format a verified alternate page with the standard primary and matched keys.
pub(in crate::exec) fn format_alternate_page(
    page: AlternatePage,
    compact: bool,
    limit: usize,
) -> Result<Value, HostError> {
    let mut rows = HashMap::new();
    for row in format_rows(page.values, compact) {
        let key: KeyValue = serde_json::from_value(row["key"].clone())
            .map_err(|_| HostError::internal("alternate response has an invalid primary key"))?;
        rows.insert(key, row);
    }
    let mut results = Vec::with_capacity(page.entries.len());
    for entry in &page.entries {
        let mut row = rows.remove(&entry.primary).unwrap_or_else(|| {
            let mut row = serde_json::json!({ "key": entry.primary, "fields": {} });
            if !compact {
                row["metadata"] = Value::Null;
            }
            row
        });
        row["matched_key"] = serde_json::to_value(&entry.matched)
            .map_err(|_| HostError::internal("invalid matched alternate key"))?;
        results.push(row);
    }
    let next = if page.has_more {
        page.entries.last().map(|entry| {
            let mut cursor = QueryCursor::new(entry.matched.clone(), Some(page.comparison));
            cursor.alternate_primary = Some(entry.primary.clone());
            cursor
        })
    } else {
        None
    };
    let mut payload = page_payload(results, None, limit, 0, page.has_more, None);
    payload["next_cursor"] =
        serde_json::to_value(next).map_err(|_| HostError::internal("invalid alternate cursor"))?;
    Ok(payload)
}
