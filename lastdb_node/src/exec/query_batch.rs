//! `POST /api/queries/batch`: several independent reads in one request.

use super::*;
use crate::lastdb_host::HostNode as _;

/// Most queries one `POST /api/queries/batch` request may carry.
const QUERY_BATCH_MAX_QUERIES: usize = 64;

/// Queries of one batch that run at the same time.
const QUERY_BATCH_CONCURRENCY: usize = 4;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryBatchRequest {
    queries: Vec<Value>,
}

/// `POST /api/queries/batch` — several independent queries in one request.
pub(super) async fn execute_query_batch_route(
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
                response_format
                    .run(handlers::execute_query(host, query, limit, offset, cursor))
                    .await,
                ctx,
            );
            (index, response)
        });
    let scope = match host.fold_db().db_ops().read_scope().await {
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

pub(super) fn parse_query_batch(
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
