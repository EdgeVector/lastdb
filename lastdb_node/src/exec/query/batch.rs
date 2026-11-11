//! `POST /api/queries/batch` parsing and concurrent execution.

use fold_db::access::AccessContext;
use serde::Deserialize;
use serde_json::Value;

use crate::exec::reject::Reject;
use crate::exec::reply::{error_response, render};
use crate::exec::wire::{body_object, parse_query_body, QueryRequest};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

use super::execute_query;

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
