//! Query execution: row formatting, conflict annotation, pagination push-down.

use super::*;

// ---------------------------------------------------------------------------
// Query row formatting (shared)
// ---------------------------------------------------------------------------

mod rows;
pub use self::rows::*;
mod conflicts;
pub(in crate::handlers) use self::conflicts::*;
mod paging;
pub(in crate::handlers) use self::paging::*;
mod schema_guard;
pub(in crate::handlers) use self::schema_guard::*;
mod plan;

/// Execute `POST /api/query`: resolve the schema, apply the paginated push-down
/// (cheap exact count → fetch only the requested page) when the query is an
/// unfiltered "list all", else the bounded materialize-then-slice fallback, and
/// build the `{ results, total_count, returned_count, limit, offset, has_more }`
/// page payload. This is the single copy of the full node's `execute_query` /
/// `execute_query_with_context` pagination policy; the minimal daemon inherits
/// the push-down + read-gate it previously simplified away.
///
/// `limit`/`offset` are the pagination fields the caller already stripped from
/// the body (the `Query` type is `deny_unknown_fields`).
///
/// # Errors
/// [`HostError`] `404` when the schema is unknown, `503` when the read gate is
/// saturated, `500` on a core failure.
pub async fn execute_query<H: HostNode>(
    host: &H,
    mut query: Query,
    limit: Option<usize>,
    offset: Option<usize>,
    cursor: Option<KeyValue>,
    ctx: &AccessContext,
    allow_full_scan: bool,
) -> Result<Value, HostError> {
    let expected_total_count = query.expected_total_count;
    // Resolve descriptive names → canonical. Bare unknown names still fall
    // through so legacy CLI paths get the standard query-engine not-found
    // substring, but app-owned refs (`owner/name`) are never views: returning a
    // typed 404 keeps startup races like `lastsecrets/LastSecret` from surfacing
    // as noisy generic "Bad request: Schema error" events.

    let requested_schema_name = query.schema_name.clone();
    let resolve_started = std::time::Instant::now();
    let resolved = resolve_schema_name(host, &query.schema_name);
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
    if !allow_full_scan && !is_key_restricted_filter(query.filter.as_ref()) {
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

    if expected_total_count.is_some() && !can_push_down_key_restricted(&query) {
        return Err(HostError::new(
            400,
            "expected_total_count requires a keyed query with no post-key filters".to_string(),
        ));
    }

    let limit = clamp_request_limit(limit);
    let offset = offset.unwrap_or(0);
    let cursor = usable_cursor(cursor, query.sort_order.as_ref());

    let run = plan::QueryRun {
        host,
        ctx,
        limit,
        offset,
        cursor,
        expected_total_count,
    };
    if let Some(payload) = run.caller_supplied_page(&query).await? {
        return Ok(payload);
    }
    if can_push_down_key_restricted(&query) {
        return run.key_restricted(query).await;
    }
    if can_push_down(&query) {
        if let Some(payload) = run.unfiltered_pushdown(&query).await? {
            return Ok(payload);
        }
    }
    run.bounded_fallback(query).await
}

/// Pick a QoS lane for the unknown-cardinality query fallback.
///
/// Full-cap `Page` / `PageAfter` fetches (limit ≥ [`INTERNAL_FETCH_CAP`]) are
/// treated as [`Lane::Bulk`]; everything else stays interactive.
pub(super) fn lane_for_unknown_cardinality(query: &Query) -> Lane {
    let limit = match &query.filter {
        Some(HashRangeFilter::Page { limit, .. } | HashRangeFilter::PageAfter { limit, .. }) => {
            *limit
        }
        _ => return Lane::Interactive,
    };
    Lane::for_page_limit(limit)
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
) -> Result<Box<dyn crate::host_node::ReadPermit>, ReadBusy> {
    let started = std::time::Instant::now();
    let permit = host.acquire_op_permit(lane).await;
    request_phases::add_phase(RequestPhase::AdmissionWait, started.elapsed());
    permit
}
