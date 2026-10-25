use super::*;

mod change_rules;
use change_rules::AppChangesRequest;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AppSearchRequest {
    pub(super) query: String,
    #[serde(default)]
    pub(super) k: Option<usize>,
    #[serde(default)]
    pub(super) target: Option<String>,
}

pub(super) fn execute_app_search_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let Ok(request) = serde_json::from_slice::<AppSearchRequest>(&req.body) else {
        return error_response(400, "invalid app search request", ctx);
    };
    let query = request.query.trim().to_string();
    if query.is_empty() {
        return error_response(
            400,
            "Missing required field: provide a non-empty 'query'",
            ctx,
        );
    }
    // Owner UDS (NodeOwner) is allowed — scope is whole-node / target narrow.
    // Verified apps keep app-scoped search. Unverified non-owner still 403s
    // inside handlers::app_search (capability_required).
    if !ctx.is_owner && ctx.verified_app_id().is_none() {
        return app_search_capability_required_response();
    }
    render(
        handlers::app_search(
            host,
            AppSearchParams {
                term: query,
                limit: request.k,
                target: request.target,
            },
            ctx,
        ),
        ctx,
    )
}

pub(super) fn app_search_capability_required_response() -> UdsResponse {
    let body = serde_json::json!({
        "status": 403,
        "reason": "capability_required",
        "error": "POST /api/app/search requires a verified app identity",
    });
    match serde_json::to_vec(&body) {
        Ok(bytes) => UdsResponse::new(403, "Forbidden", bytes)
            .with_header("Content-Type", "application/json"),
        Err(_) => content_free(403, "Forbidden"),
    }
}

pub(super) async fn execute_app_changes_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let AppChangesRequest {
        after,
        limit,
        target,
    } = match AppChangesRequest::parse(&req.body) {
        Ok(request) => request,
        Err(message) => return error_response(400, message, ctx),
    };
    let target = match target.as_deref() {
        Some(name) => match handlers::resolve_schema_name(host, name) {
            Ok(Some(canonical)) => Some(canonical),
            // Unknown and unauthorized targets are deliberately indistinguishable.
            Ok(None) => Some(String::new()),
            Err(error) => return render(Err(error), ctx),
        },
        None => None,
    };

    let page = match host
        .db
        .db_ops()
        .change_feed()
        .list_after(after, limit)
        .await
    {
        Ok(page) => page,
        Err(error) => {
            return error_response(500, &format!("change feed read failed: {error}"), ctx)
        }
    };
    let mut changes = page.events;
    // Advance past every examined event, including those the caller cannot see.
    let examined_cursor = changes.last().map_or(after, |event| event.seq);
    let tip = host.db.db_ops().change_feed().tip().await;
    changes.retain(|event| change_visible_to_caller(host, ctx, event, target.as_deref()));
    render(
        Ok(change_rules::reply(changes, examined_cursor, tip, page.gap)),
        ctx,
    )
}

pub(super) fn change_visible_to_caller(
    host: &Host,
    ctx: &AccessContext,
    event: &fold_db::db_operations::ChangeFeedEvent,
    target: Option<&str>,
) -> bool {
    let canonical = handlers::resolve_schema_name(host, &event.schema)
        .ok()
        .flatten()
        .unwrap_or_else(|| event.schema.clone());
    if target.is_some_and(|wanted| wanted != canonical) {
        return false;
    }
    if ctx.is_owner {
        return true;
    }
    let Some(app_id) = ctx.verified_app_id() else {
        return false;
    };
    host.db
        .schema_manager()
        .get_schema_metadata(&canonical)
        .ok()
        .flatten()
        .and_then(|mut schema| schema.owner_app_id.take())
        .as_deref()
        == Some(app_id)
}

// ---------------------------------------------------------------------------
// Schemas (host-local: catalog listing / fetch / owner declare)
// ---------------------------------------------------------------------------

/// Count live records through the ordinary bounded query route. A failed or
/// unavailable count is unknown, never evidence that the schema is empty.
pub(super) async fn count_schema_records(host: &Host, canonical: &str) -> Option<usize> {
    let mgr = host.db.schema_manager();
    let schema = mgr
        .get_schema_following_supersession(canonical)
        .await
        .ok()??;
    if schema.runtime_fields.is_empty() {
        return Some(0);
    }

    let query = fold_db::schema::types::Query::new(canonical.to_string(), Vec::new());
    let ctx = AccessContext::owner(host.user_hash.clone());
    host.db
        .query_executor()
        .count_query_rows(&query, &ctx)
        .await
        .ok()
        .flatten()
}

pub(super) const LIST_RECORD_KEYS_DEFAULT_LIMIT: usize = 100;
pub(super) const LIST_RECORD_KEYS_MAX_LIMIT: usize = 1000;

/// `GET /api/list?schema=` — paged API keys for live schema records.
pub(super) async fn execute_list_record_keys_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let Some(requested) = query_value(&req.target, "schema").filter(|s| !s.is_empty()) else {
        return error_response(400, "schema query parameter is required", ctx);
    };
    let limit = match query_value(&req.target, "limit") {
        None => LIST_RECORD_KEYS_DEFAULT_LIMIT,
        Some(raw) => match raw.parse::<usize>() {
            Ok(0) => {
                return error_response(400, "limit must be at least 1", ctx);
            }
            Ok(n) => n.min(LIST_RECORD_KEYS_MAX_LIMIT),
            Err(_) => return error_response(400, "limit must be a positive integer", ctx),
        },
    };
    let cursor = query_value(&req.target, "cursor").filter(|s| !s.is_empty());
    let hash_filter = query_value(&req.target, "hash");

    let resolved = match handlers::resolve_schema_name(host, &requested) {
        Ok(v) => v,
        Err(e) => return render(Err(e), ctx),
    };
    let Some(canonical) = resolved else {
        return error_response(404, &format!("schema not found: {requested}"), ctx);
    };

    match host
        .db
        .list_schema_record_keys_with_access(
            fold_db::db_operations::SchemaRecordKeysRequest {
                requested: &requested,
                schema_id: &canonical,
                limit,
                cursor: cursor.as_deref(),
                hash_filter: hash_filter.as_deref(),
            },
            ctx,
        )
        .await
    {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "list": report }),
            ctx.user_id.as_str(),
        )),
        // `list_schema_record_keys` now returns `SchemaError::NotFound` for an
        // unresolvable schema, `InvalidField` for one with no listable key
        // field, and `InvalidCursor` for a cursor outside the key molecule, so
        // the canonical mapping produces the 404/400 this arm used to
        // reconstruct by matching on message text.
        Err(e) => mapped_error_response("list failed", e, ctx),
    }
}
