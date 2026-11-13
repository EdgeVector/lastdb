//! Owned row/page response assembly, scan rejection, sorting, and `POST /api/queries/batch`.

use std::cmp::Ordering;

use fold_db::access::AccessContext;
use fold_db::fold_db_core::query::query_executor::AlternatePage;
use fold_db::fold_db_core::query::Record;
use fold_db::schema::types::key_comparison::RangeKeyComparison;
use fold_db::schema::types::operations::SortOrder;
use fold_db::schema::types::{KeyValue, QueryCursor, Schema, SchemaError};
use serde::Deserialize;
use serde_json::{json, Value};

use super::execute_query;
use crate::exec::reply::{error_response, render, HostError};
use crate::exec::wire::{body_object, parse_query_body, Reject};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

const QUERY_BATCH_MAX_QUERIES: usize = 64;
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

pub(in crate::exec) async fn query_batch_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    use futures::StreamExt as _;
    let queries = match parse_query_batch(req, ctx) {
        Ok(q) => q,
        Err(resp) => return resp,
    };
    let items = queries
        .into_iter()
        .enumerate()
        .map(|(i, query)| async move {
            let Value::Object(obj) = query else {
                return (i, Reject::malformed_body().response());
            };
            let resp = match parse_query_body(obj) {
                Ok(q) => render(execute_query(host, q).await, ctx),
                Err(r) => r,
            };
            (i, resp)
        });
    let Ok(scope) = host.records().db_ops().read_scope().await else {
        return error_response(503, "read scope unavailable", ctx);
    };
    let finished: Vec<(usize, UdsResponse)> = scope
        .run_shared(futures::stream::iter(items).buffer_unordered(4).collect())
        .await;
    let results = batch_results(finished);
    render(
        Ok(json!({ "count": results.len(), "results": results })),
        ctx,
    )
}

pub(in crate::exec) fn batch_results(mut finished: Vec<(usize, UdsResponse)>) -> Vec<Value> {
    finished.sort_by_key(|(i, _)| *i);
    finished
        .into_iter()
        .map(|(_, resp)| {
            let body = serde_json::from_slice::<Value>(&resp.body).unwrap_or_else(|_| {
                Value::String(String::from_utf8_lossy(&resp.body).into_owned())
            });
            json!({ "status": resp.status, "response": body })
        })
        .collect()
}

pub(in crate::exec) fn parse_query_batch(
    req: &UdsRequest,
    ctx: &AccessContext,
) -> Result<Vec<Value>, UdsResponse> {
    let queries = serde_json::from_value::<QueryBatchRequest>(Value::Object(body_object(req)?))
        .map_err(|e| Reject::from_serde_error(&e, None).response())?
        .queries;
    if queries.is_empty() {
        return Err(error_response(
            400,
            "Query batch must contain at least one query",
            ctx,
        ));
    }
    if queries.len() > QUERY_BATCH_MAX_QUERIES {
        let msg = format!(
            "Query batch carries {} queries; the limit is {QUERY_BATCH_MAX_QUERIES}",
            queries.len()
        );
        return Err(error_response(400, &msg, ctx));
    }
    Ok(queries)
}

fn page_payload(
    page: Vec<Value>,
    total_count: Option<usize>,
    (limit, offset): (usize, usize),
    has_more: bool,
    next_cursor: Option<QueryCursor>,
    key_form: Option<&'static str>,
) -> Value {
    let n = page.len();
    json!({
        "results": Value::Array(page), "total_count": total_count, "returned_count": n,
        "limit": limit, "offset": offset, "has_more": has_more,
        "next_cursor": serde_json::to_value(next_cursor).unwrap_or(Value::Null), "key_form": key_form,
    })
}

pub(super) fn cursor_payload(
    mut page: Vec<Value>,
    total_count: Option<usize>,
    (limit, offset): (usize, usize),
    resume: Option<KeyValue>,
    comparison: Option<String>,
    with_cursor: bool,
) -> Value {
    let has_more = page.len() > limit || resume.is_some();
    page.truncate(limit);
    let next = with_cursor
        .then(|| {
            resume.or_else(|| {
                has_more
                    .then(|| serde_json::from_value(page.last()?.get("key")?.clone()).ok())
                    .flatten()
            })
        })
        .flatten()
        .map(|key| QueryCursor::new(key, comparison));
    page_payload(page, total_count, (limit, offset), has_more, next, None)
}

pub(super) fn materialized_page_payload(results: Vec<Value>, limit: usize, offset: usize) -> Value {
    let total_count = results.len();
    let page: Vec<_> = results.into_iter().skip(offset).take(limit).collect();
    let has_more = offset.saturating_add(page.len()) < total_count;
    page_payload(
        page,
        Some(total_count),
        (limit, offset),
        has_more,
        None,
        None,
    )
}

pub(super) fn format_rows(
    records: impl IntoIterator<Item = (KeyValue, Record)>,
    compact: bool,
) -> Vec<Value> {
    records
        .into_iter()
        .map(|(k, r)| format_row(&k, r, compact))
        .collect()
}

fn format_row(key: &KeyValue, record: Record, compact: bool) -> Value {
    let mut row = serde_json::Map::from_iter([
        ("key".into(), json!(key)),
        ("revision".into(), Value::String(record.revision)),
        ("alternates".into(), json!(record.alternates)),
        (
            "fields".into(),
            Value::Object(record.fields.into_iter().collect()),
        ),
    ]);
    if !compact {
        row.insert("metadata".into(), json!(record.metadata));
    }
    Value::Object(row)
}

pub(in crate::exec) fn verified_query_page(
    records: Vec<(KeyValue, Record)>,
    compact: bool,
    total_count: Option<usize>,
    limit: usize,
    offset: usize,
    has_more: bool,
    next_cursor: Option<QueryCursor>,
) -> Value {
    let key_form = (!records.is_empty()).then_some("api");
    page_payload(
        format_rows(records, compact),
        total_count,
        (limit, offset),
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
                let mut r = json!({ "key": entry.primary, "fields": {} });
                if !compact {
                    r["metadata"] = Value::Null;
                }
                r
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
        .map(|e| {
            let mut c = QueryCursor::new(e.matched.clone(), Some(page.comparison));
            c.alternate_primary = Some(e.primary.clone());
            c
        });
    Ok(page_payload(
        results,
        None,
        (limit, 0),
        page.has_more,
        next,
        None,
    ))
}

pub(super) fn full_schema_scan_rejection(requested: &str, schema: Option<&Schema>) -> HostError {
    let is_id = |v: &str| v.len() == 64 && v.bytes().all(|b| b.is_ascii_hexdigit());
    let name = schema
        .and_then(|s| s.descriptive_name.clone())
        .or_else(|| (!is_id(requested)).then(|| requested.to_string()))
        .or_else(|| schema.and_then(|s| (!is_id(&s.name)).then(|| s.name.clone())))
        .or_else(|| {
            schema
                .and_then(|s| s.owner_app_id.as_deref())
                .map(|o| format!("{o}/<unnamed-schema>"))
        })
        .unwrap_or_else(|| "<unnamed-product-schema>".to_string());
    let msg = format!(
        "full_schema_scan_not_allowed: unfiltered query on product schema '{name}' is not supported; add a keyed filter using the structured remediation"
    );
    let body = json!({
        "ok": false, "kind": "full_schema_scan_not_allowed",
        "error": "product apps are keyed-access only; unfiltered schema queries are not supported",
        "message": msg, "schema_name": name,
        "schema_id": schema.map_or(requested, |s| s.name.as_str()),
        "remediation": {
            "action": "add_key_filter", "request_key": "filter",
            "record_keys": schema.and_then(|s| s.record_keys.as_ref()),
            "filter_variants": KEYED_FILTER_VARIANTS,
        }
    });
    HostError::structured(400, msg, body)
}

pub(super) fn sort_rows(
    rows: &mut [Value],
    sort_order: Option<&SortOrder>,
    range_cmp: Option<&RangeKeyComparison>,
) -> Result<(), HostError> {
    if let Some(cmp) = range_cmp {
        for r in &*rows {
            cmp.validate_value(r["key"]["range"].as_str().unwrap_or(""))
                .map_err(SchemaError::InvalidData)?;
        }
    }
    let (mut err, desc) = (None, matches!(sort_order, Some(SortOrder::Desc)));
    rows.sort_by(|a, b| {
        let (ar, br) = (
            a["key"]["range"].as_str().unwrap_or(""),
            b["key"]["range"].as_str().unwrap_or(""),
        );
        let ord = if let Some(cmp) = range_cmp {
            match cmp.compare(ar, br) {
                Ok(o) => o,
                Err(d) => {
                    err.get_or_insert(SchemaError::InvalidData(d));
                    return Ordering::Equal;
                }
            }
        } else {
            ar.cmp(br)
        };
        (if desc { ord.reverse() } else { ord }).then_with(|| {
            a["key"]["hash"]
                .as_str()
                .unwrap_or("")
                .cmp(b["key"]["hash"].as_str().unwrap_or(""))
        })
    });
    err.map_or(Ok(()), |e| Err(e.into()))
}
