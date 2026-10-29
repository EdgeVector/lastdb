//! `POST /api/queries/batch`: several independent reads in one request.
//! Design page: `docs/designs/lastdb-query-batch-route.md`.

use super::*;
use lastdb_host::HostNode as _;

/// Most queries one `POST /api/queries/batch` request may carry.
const QUERY_BATCH_MAX_QUERIES: usize = 64;

/// Queries of one batch that run at the same time. Each query takes its own QoS
/// permit inside [`handlers::execute_query`]; the batch route holds none. A small
/// fixed width keeps one request from taking the global budget (64 permits by
/// default) and still overlaps the cold loads of its queries.
const QUERY_BATCH_CONCURRENCY: usize = 4;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryBatchRequest {
    queries: Vec<Value>,
}

/// `POST /api/queries/batch` — several independent queries in one request.
///
/// Body: `{"queries": [<POST /api/query body>, ...]}`. Each item is a complete
/// `/api/query` body with its own `schema_name`, so one request can read from
/// several schemas. Reply: `{"ok": true, "count": N, "results": [{"status": S,
/// "response": <the /api/query reply>}, ...]}` in request order.
///
/// Every item uses the single route's query parser, host handler, and receipt
/// renderer directly. The parsed JSON never becomes synthetic request bytes.
/// One item can fail (its `status` is not 200)
/// while the others succeed; the request itself fails only for a malformed body,
/// an empty batch, or more than [`QUERY_BATCH_MAX_QUERIES`] items.
///
/// Items are independent: none reads another's result, and the route does not
/// match records across schemas. The `x-lastdb-allow-full-scan` opt-in is not
/// passed to items, so an unkeyed item is refused as it is on a product route.
///
/// Items run [`QUERY_BATCH_CONCURRENCY`] at a time inside this task (not
/// spawned), so the task-local request phases still add up for the whole batch.
/// They may finish in any order: a slow item does not hold back the start of the
/// next one. [`batch_results`](super::query_read_receipt::batch_results) uses
/// each item's request index to restore reply order.
pub(super) async fn execute_query_batch_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    use futures::StreamExt as _;

    let parse_started = std::time::Instant::now();
    let obj = match body_object(req) {
        Ok(obj) => obj,
        Err(resp) => return resp,
    };
    let queries = match serde_json::from_value::<QueryBatchRequest>(Value::Object(obj)) {
        Ok(request) => request.queries,
        Err(err) => return Reject::from_serde_error(&err, None).response(),
    };
    if queries.is_empty() {
        return error_response(400, "Query batch must contain at least one query", ctx);
    }
    if queries.len() > QUERY_BATCH_MAX_QUERIES {
        return error_response(
            400,
            &format!(
                "Query batch carries {} queries; the limit is {QUERY_BATCH_MAX_QUERIES}",
                queries.len()
            ),
            ctx,
        );
    }
    fold_db::request_phases::add_phase(
        fold_db::request_phases::RequestPhase::Parse,
        parse_started.elapsed(),
    );

    let items = queries
        .into_iter()
        .enumerate()
        .map(|(index, query)| async move {
            let parse_started = std::time::Instant::now();
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
            fold_db::request_phases::add_phase(
                fold_db::request_phases::RequestPhase::Parse,
                parse_started.elapsed(),
            );
            let response = super::query_read_receipt::render_query(
                req,
                ctx,
                // Batch items always require keys, even if the outer request
                // carries the owner-only unfiltered-read header.
                response_format.run(handlers::execute_query(
                    host, query, limit, offset, cursor, ctx, false,
                )),
            )
            .await;
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

    let results = super::query_read_receipt::batch_results(finished);
    render(
        Ok(serde_json::json!({ "count": results.len(), "results": results })),
        ctx,
    )
}
