//! Owned row/page response assembly, scan rejection, sorting, and `POST /api/queries/batch`.

use std::cmp::Ordering;

use fold_db::access::AccessContext;
use fold_db::fold_db_core::query::query_executor::AlternatePage;
use fold_db::schema::types::key_comparison::RangeKeyComparison;
use fold_db::schema::types::operations::SortOrder;
use fold_db::schema::types::{KeyValue, QueryCursor, Schema, SchemaError};
use serde::Deserialize;
use serde_json::Value;

use super::execute_query;
use crate::exec::reply::{error_response, render, HostError};
use crate::exec::wire::{body_object, parse_query_body, Reject};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

const QUERY_BATCH_MAX_QUERIES: usize = 64;
const QUERY_BATCH_CONCURRENCY: usize = 4;
const KEYED_FILTER_VARIANTS: &[&str] = &[
    "HashKey",
    "HashRangeKey",
    "HashRangePrefix",
    "HashRangeRange",
    "HashRangeKeys",
];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryBatchRequest {
    queries: Vec<Value>,
}

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
            let resp = match parse_query_body(obj) {
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
            };
            (index, resp)
        });
    let Ok(scope) = host.records().db_ops().read_scope().await else {
        return error_response(503, "read scope unavailable", ctx);
    };
    let finished: Vec<(usize, UdsResponse)> = scope
        .run_shared(
            futures::stream::iter(items)
                .buffer_unordered(QUERY_BATCH_CONCURRENCY)
                .collect(),
        )
        .await;
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
            serde_json::json!({ "status": response.status, "response": body })
        })
        .collect()
}

pub(in crate::exec) fn parse_query_batch(
    req: &UdsRequest,
    ctx: &AccessContext,
) -> Result<Vec<Value>, UdsResponse> {
    let queries = serde_json::from_value::<QueryBatchRequest>(Value::Object(body_object(req)?))
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
    next_cursor: Option<QueryCursor>,
    key_form: Option<&'static str>,
) -> Value {
    let returned_count = page.len();
    let next_cursor = serde_json::to_value(next_cursor).unwrap_or(Value::Null);
    serde_json::json!({
        "results": Value::Array(page), "total_count": total_count, "returned_count": returned_count,
        "limit": limit, "offset": offset, "has_more": has_more,
        "next_cursor": next_cursor, "key_form": key_form,
    })
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
    page.truncate(limit);
    let next_cursor = resume
        .or_else(|| {
            has_more
                .then(|| page.last()?.get("key"))
                .flatten()
                .and_then(|k| serde_json::from_value::<KeyValue>(k.clone()).ok())
        })
        .map(|key| QueryCursor::new(key, comparison));
    page_payload(
        page,
        total_count,
        limit,
        offset,
        has_more,
        next_cursor,
        None,
    )
}

pub(super) fn overfetched_offset_payload(
    mut page: Vec<Value>,
    total_count: usize,
    limit: usize,
    offset: usize,
) -> Value {
    let has_more = page.len() > limit;
    page.truncate(limit);
    page_payload(page, Some(total_count), limit, offset, has_more, None, None)
}

pub(super) fn materialized_page_payload(results: Vec<Value>, limit: usize, offset: usize) -> Value {
    let total_count = results.len();
    let page: Vec<_> = results.into_iter().skip(offset).take(limit).collect();
    let has_more = offset.saturating_add(page.len()) < total_count;
    page_payload(page, Some(total_count), limit, offset, has_more, None, None)
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
    let mut row = serde_json::Map::from_iter([
        ("key".into(), serde_json::json!(key)),
        ("revision".into(), Value::String(record.revision)),
        ("alternates".into(), serde_json::json!(record.alternates)),
        (
            "fields".into(),
            Value::Object(record.fields.into_iter().collect()),
        ),
    ]);
    if !compact {
        row.insert("metadata".into(), serde_json::json!(record.metadata));
    }
    Value::Object(row)
}

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
        .map(|(k, r)| format_row(&k, r, compact))
        .collect();
    page_payload(
        rows,
        total_count,
        limit,
        offset,
        has_more,
        next_cursor,
        key_form,
    )
}

pub(in crate::exec) fn format_alternate_page(
    mut page: AlternatePage,
    compact: bool,
    limit: usize,
) -> Result<Value, HostError> {
    let mut results = Vec::with_capacity(page.entries.len());
    for entry in &page.entries {
        let mut row = page.values.remove(&entry.primary).map_or_else(
            || {
                let mut row = serde_json::json!({ "key": entry.primary, "fields": {} });
                if !compact {
                    row["metadata"] = Value::Null;
                }
                row
            },
            |record| format_row(&entry.primary, record, compact),
        );
        row["matched_key"] = serde_json::to_value(&entry.matched)
            .map_err(|_| HostError::internal("invalid matched alternate key"))?;
        results.push(row);
    }
    let next = page
        .has_more
        .then(|| page.entries.last())
        .flatten()
        .map(|entry| {
            let mut cursor = QueryCursor::new(entry.matched.clone(), Some(page.comparison));
            cursor.alternate_primary = Some(entry.primary.clone());
            cursor
        });
    Ok(page_payload(
        results,
        None,
        limit,
        0,
        page.has_more,
        next,
        None,
    ))
}

pub(super) fn full_schema_scan_rejection(requested: &str, schema: Option<&Schema>) -> HostError {
    let is_id = |v: &str| v.len() == 64 && v.bytes().all(|b| b.is_ascii_hexdigit());
    let schema_name = schema
        .and_then(|s| s.descriptive_name.clone())
        .or_else(|| (!is_id(requested)).then(|| requested.to_string()))
        .or_else(|| schema.and_then(|s| (!is_id(&s.name)).then(|| s.name.clone())))
        .or_else(|| {
            schema
                .and_then(|s| s.owner_app_id.as_deref())
                .map(|o| format!("{o}/<unnamed-schema>"))
        })
        .unwrap_or_else(|| "<unnamed-product-schema>".to_string());
    let message = format!(
        "full_schema_scan_not_allowed: unfiltered query on product schema '{schema_name}' is not supported; add a keyed filter using the structured remediation"
    );
    let body = serde_json::json!({
        "ok": false,
        "kind": "full_schema_scan_not_allowed",
        "error": "product apps are keyed-access only; unfiltered schema queries are not supported",
        "message": message,
        "schema_name": schema_name,
        "schema_id": schema.map_or(requested, |s| s.name.as_str()),
        "remediation": {
            "action": "add_key_filter",
            "request_key": "filter",
            "record_keys": schema.and_then(|s| s.record_keys.as_ref()),
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
    let desc = matches!(sort_order, Some(SortOrder::Desc));
    rows.sort_by(|a, b| {
        let (ar, br) = (
            a["key"]["range"].as_str().unwrap_or(""),
            b["key"]["range"].as_str().unwrap_or(""),
        );
        let range_order = match range_comparison {
            Some(cmp) => match cmp.compare(ar, br) {
                Ok(order) => order,
                Err(detail) => {
                    error.get_or_insert(SchemaError::InvalidData(detail));
                    return Ordering::Equal;
                }
            },
            None => ar.cmp(br),
        };
        let primary = if desc {
            range_order.reverse()
        } else {
            range_order
        };
        primary.then(range_order).then_with(|| {
            a["key"]["hash"]
                .as_str()
                .unwrap_or("")
                .cmp(b["key"]["hash"].as_str().unwrap_or(""))
        })
    });
    error.map_or(Ok(()), |e| Err(e.into()))
}
