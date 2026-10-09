//! `POST /api/queries/batch`: several independent reads in one request.
//! Design page: `docs/designs/lastdb-query-batch-route.md`.

use super::*;

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
/// Every item runs through [`execute_query_route`] as a sub-request, so parse,
/// schema resolve, access checks, limits, and error bodies are those of the
/// single route by construction. One item can fail (its `status` is not 200)
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
/// next one. Each item carries its request index, and [`query_batch_results`]
/// sorts by it, so the reply order never depends on completion order.
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
    let headers: Vec<(String, String)> = req
        .headers
        .iter()
        .filter(|(name, _)| !name.eq_ignore_ascii_case("x-lastdb-allow-full-scan"))
        .cloned()
        .collect();
    fold_db::request_phases::add_phase(
        fold_db::request_phases::RequestPhase::Parse,
        parse_started.elapsed(),
    );

    let items = queries.into_iter().enumerate().map(|(index, query)| {
        let item = UdsRequest {
            method: "POST".to_string(),
            target: "/api/query".to_string(),
            headers: headers.clone(),
            body: serde_json::to_vec(&query).unwrap_or_default(),
        };
        async move { (index, execute_query_route(&item, ctx, host).await) }
    });
    let finished: Vec<(usize, UdsResponse)> = futures::stream::iter(items)
        .buffer_unordered(QUERY_BATCH_CONCURRENCY)
        .collect()
        .await;

    let results = query_batch_results(finished);
    render(
        Ok(serde_json::json!({ "count": results.len(), "results": results })),
        ctx,
    )
}

/// Reply entries of a query batch, in request order, from items that finished in
/// any order. Each entry is `{"status": S, "response": <the /api/query reply>}`.
fn query_batch_results(mut finished: Vec<(usize, UdsResponse)>) -> Vec<Value> {
    finished.sort_by_key(|(index, _)| *index);
    finished
        .iter()
        .map(|(_, response)| {
            let body = serde_json::from_slice::<Value>(&response.body).unwrap_or_else(|_| {
                Value::String(String::from_utf8_lossy(&response.body).into_owned())
            });
            serde_json::json!({ "status": response.status, "response": body })
        })
        .collect()
}

/// Schema label for the telemetry sample of a batch: the first query's schema,
/// like a mutation batch names its first mutation. A batch may span schemas.
pub(super) fn schema_hint(body: &[u8]) -> Option<String> {
    let first = serde_json::from_slice::<Value>(body)
        .ok()?
        .get("queries")?
        .as_array()?
        .first()?
        .clone();
    crate::request_telemetry::schema_from_body(&serde_json::to_vec(&first).ok()?)
}
