//! Query request flow and shared reads. Pure request and page rules live below.

use super::*;
use fold_db::schema::types::key_comparison::KeyComparisonContext;
use std::sync::Arc;

mod alternate;
mod rows;
pub(in crate::handlers) use self::rows::*;
mod conflicts;
pub(in crate::handlers) use self::conflicts::*;
mod response;
pub(in crate::handlers) use self::response::*;
mod schema_guard;
pub(in crate::handlers) use self::schema_guard::*;
mod execute;
mod output;
mod row_rules;
mod rules;
mod walk;
pub use output::QueryResponseFormat;

/// Execute `POST /api/query`: resolve the schema, check request rules, and
/// select the bounded read before rendering the page response. Ordinary
/// ascending keyed reads do not require a count. Administrative requests
/// retain their explicit access check and bounded execution paths.
///
/// `limit`/`offset` are the pagination fields the caller already stripped from
/// the body (the `Query` type is `deny_unknown_fields`).
///
/// # Errors
/// [`HostError`] `400` for invalid query options, `404` when an app schema is
/// unknown, `409` when an expected count differs, `503` when the read gate is
/// saturated, and the core's typed status on a read failure.
pub async fn execute_query<H: HostNode>(
    host: &H,
    query: Query,
    limit: Option<usize>,
    offset: Option<usize>,
    cursor: Option<QueryCursor>,
    ctx: &AccessContext,
    allow_full_scan: bool,
) -> Result<Value, HostError> {
    let store = host.fold_db().db_ops().namespaced_store();
    let scope = fold_db::storage::disk_pack::PackReadScope::for_store(&store)
        .await
        .map_err(|error| HostError::new(503, error.to_string()))?;
    let future = Box::pin(execute_query_scoped(
        host,
        query,
        limit,
        offset,
        cursor,
        ctx,
        allow_full_scan,
    ));
    match scope {
        Some(scope) => scope.run_result(future).await,
        None => future.await,
    }
}

async fn execute_query_scoped<H: HostNode>(
    host: &H,
    mut query: Query,
    limit: Option<usize>,
    offset: Option<usize>,
    cursor: Option<QueryCursor>,
    ctx: &AccessContext,
    allow_full_scan: bool,
) -> Result<Value, HostError> {
    // Resolve descriptive names → canonical. Bare unknown names still fall
    // through so legacy CLI paths get the standard query-engine not-found
    // substring, but app-owned refs (`owner/name`) are never views: returning a
    // typed 404 keeps startup races like `lastsecrets/LastSecret` from surfacing
    // as noisy generic "Bad request: Schema error" events.

    let requested_schema_name = query.schema_name.clone();
    let resolve_started = std::time::Instant::now();
    let resolved = resolve_schema_name_for_read(host, &query.schema_name).await;
    request_phases::add_phase(RequestPhase::SchemaResolve, resolve_started.elapsed());
    match resolved? {
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

    // Product full-schema scans are deprecated (DynamoDB-style access only).
    // Resolve first so an identity-hash request is rejected in terms of the
    // product schema a human/client knows, while retaining the canonical id as
    // a secondary machine field. Key/partition filters (HashKey / HashRange*)
    // are allowed; bare list-all (including Page/PageAfter/SampleN alone)
    // requires an explicit admin/offline opt-in.
    if !allow_full_scan
        && !query
            .filter
            .as_ref()
            .is_some_and(HashRangeFilter::is_key_restricted)
    {
        let schema = host
            .fold_db()
            .schema_manager()
            .get_schema_metadata(&query.schema_name)
            .map_err(|error| HostError::internal(error.to_string()))?;
        return Err(full_schema_scan_rejection(
            &requested_schema_name,
            schema.as_ref(),
        ));
    }

    if query.key_name.is_some() {
        return alternate::execute(host, query, limit, offset, cursor, ctx).await;
    }
    let walk = walk::walks_records(&query);
    let (limit, offset) = walk::page_request(&mut query, walk, limit, offset);
    rules::validate_query_options(&query, cursor.is_some(), walk)?;
    let comparison = capture_key_comparison(host, &query.schema_name).await?;
    let rules::PreparedQuery {
        query,
        cursor,
        record,
        binding,
        sample,
    } = rules::prepare_comparison(query, cursor, ctx, comparison.as_deref(), walk)?;
    let run = execute::QueryRun {
        host,
        ctx,
        limit: clamp_request_limit(limit),
        offset: offset.unwrap_or(0),
        cursor,
        record,
        comparison: binding,
        sample,
        walk,
    };
    let future = run.execute(query);
    match comparison {
        Some(comparison) => comparison.run(future).await,
        None => future.await,
    }
}

/// Fetch one schema's comparison contract at the request's captured read view.
/// Row materialization uses this same boundary for nested reads.
pub(super) async fn capture_key_comparison<H: HostNode>(
    host: &H,
    name: &str,
) -> Result<Option<Arc<KeyComparisonContext>>, HostError> {
    let Some(schema) = host
        .fold_db()
        .schema_manager()
        .get_schema_following_supersession_for_read(name)
        .await
        .map_err(HostError::from)?
    else {
        return Ok(None);
    };
    host.fold_db()
        .db_ops()
        .records()
        .key_comparison_context(&schema)
        .await
        .map(Some)
        .map_err(HostError::from)
}

pub(super) fn is_app_owned_schema_ref(name: &str) -> bool {
    let trimmed = name.trim();
    let Some((owner_app_id, local_name)) = trimmed.split_once('/') else {
        return false;
    };
    !owner_app_id.is_empty() && !local_name.is_empty() && !local_name.contains('/')
}

/// Cheap exact row count for the push-down. `Ok(None)` ⇒ a view (or not found):
/// the caller falls back to materialize-then-slice.
///
/// Timed as [`RequestPhase::Count`] here rather than at the three call sites:
/// every push-down shape funnels through this one function, so instrumenting
/// it cannot drift out of sync with a new branch above.
pub(super) async fn count_rows<H: HostNode>(
    host: &H,
    query: &Query,
    ctx: &AccessContext,
) -> Result<Option<usize>, HostError> {
    let started = std::time::Instant::now();
    let _permit = if query
        .filter
        .as_ref()
        .is_some_and(HashRangeFilter::is_key_restricted)
    {
        None
    } else {
        Some(acquire_read_permit(host, Lane::Bulk).await?)
    };
    let counted = host
        .fold_db()
        .query_executor()
        .count_query_rows(query, ctx)
        .await
        .map_err(HostError::from);
    request_phases::add_phase(RequestPhase::Count, started.elapsed());
    counted
}

/// Acquire a QoS read permit, charging the wait to
/// [`RequestPhase::AdmissionWait`].
///
/// The query path admits on six different lanes depending on which push-down
/// shape ran; without this helper the governor wait is invisible on reads and
/// is charged to whatever phase happens to bracket it. That is the single
/// suspect the old telemetry could neither confirm nor deny for a slow query
/// with `loads=0`.
pub(super) async fn acquire_read_permit<H: HostNode>(
    host: &H,
    lane: Lane,
) -> Result<H::OpPermit, ReadBusy> {
    let started = std::time::Instant::now();
    let permit = host.acquire_op_permit(lane).await;
    request_phases::add_phase(RequestPhase::AdmissionWait, started.elapsed());
    permit
}
