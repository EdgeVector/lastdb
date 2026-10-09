use super::*;

#[derive(Deserialize)]
pub(super) struct AppChangesRequest {
    #[serde(default)]
    pub(super) since_cursor: Option<String>,
    #[serde(default)]
    pub(super) limit: Option<usize>,
    #[serde(default)]
    pub(super) target: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AppSearchRequest {
    pub(super) query: String,
    #[serde(default)]
    pub(super) k: Option<usize>,
    #[serde(default)]
    pub(super) target: Option<String>,
}

pub(super) const APP_CHANGES_DEFAULT_LIMIT: usize = 100;
pub(super) const APP_CHANGES_MAX_LIMIT: usize = 500;

pub(super) async fn execute_app_search_route(
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
        )
        .await,
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
    let Ok(request) = serde_json::from_slice::<AppChangesRequest>(&req.body) else {
        return error_response(400, "invalid app changes request", ctx);
    };
    let after = match request.since_cursor.as_deref() {
        Some(cursor) => match decode_change_cursor(cursor) {
            Some(seq) => seq,
            None => return error_response(400, "invalid app changes cursor", ctx),
        },
        None => 0,
    };
    let limit = request
        .limit
        .unwrap_or(APP_CHANGES_DEFAULT_LIMIT)
        .clamp(1, APP_CHANGES_MAX_LIMIT);
    let target = match request.target.as_deref() {
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
    let raw = page.events;
    let examined_cursor = raw.last().map_or(after, |event| event.seq);
    let tip = host.db.db_ops().change_feed().tip().await;
    let changes: Vec<Value> = raw
        .into_iter()
        .filter(|event| change_visible_to_caller(host, ctx, event, target.as_deref()))
        .map(|event| {
            serde_json::json!({
                "cursor": encode_change_cursor(event.seq),
                "mutation_id": event.mutation_id,
                "schema_name": event.schema,
                "key": {"hash": event.hash, "range": event.range},
                "operation": event.operation,
                "committed_at_ms": event.committed_at_ms,
                "background_tasks_drained": event.background_tasks_drained,
                "convergence_pending": event.convergence_pending,
            })
        })
        .collect();
    render(
        Ok(serde_json::json!({
            "changes": changes,
            "next_cursor": encode_change_cursor(examined_cursor),
            "has_more": examined_cursor < tip,
            "gap": page.gap,
        })),
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
        .and_then(|schema| schema.owner_app_id)
        .as_deref()
        == Some(app_id)
}

pub(super) fn encode_change_cursor(seq: u64) -> String {
    format!("v1-{seq:016x}")
}

pub(super) fn decode_change_cursor(cursor: &str) -> Option<u64> {
    let raw = cursor.strip_prefix("v1-")?;
    if raw.len() != 16 {
        return None;
    }
    u64::from_str_radix(raw, 16).ok()
}

// ---------------------------------------------------------------------------
// Schemas (host-local: catalog listing / fetch / owner declare)
// ---------------------------------------------------------------------------

/// Count live records in a schema by enumerating its key field — the full
/// node's `count_schema_records` semantics, including the deliberate `0` for
/// a schema with no countable fields yet.
pub(super) async fn count_schema_records(host: &Host, canonical: &str) -> usize {
    let mgr = host.db.schema_manager();
    let Ok(Some(schema)) = mgr.get_schema_following_supersession(canonical).await else {
        return 0;
    };
    if schema.runtime_fields.is_empty() {
        return 0;
    }

    let query = fold_db::schema::types::Query::new(canonical.to_string(), Vec::new());
    let ctx = AccessContext::owner(host.user_hash.clone());
    host.db
        .query_executor()
        .count_query_rows(&query, &ctx)
        .await
        .ok()
        .flatten()
        .unwrap_or(0)
}

pub(super) const LIST_RECORD_KEYS_DEFAULT_LIMIT: usize = 100;
pub(super) const LIST_RECORD_KEYS_MAX_LIMIT: usize = 1000;

/// `GET /api/list?schema=` — paged live record keys, no atom bodies.
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
    let hash_filter = query_value(&req.target, "hash").filter(|s| !s.is_empty());

    let resolved = match handlers::resolve_schema_name(host, &requested) {
        Ok(v) => v,
        Err(e) => return render(Err(e), ctx),
    };
    let Some(canonical) = resolved else {
        return error_response(404, &format!("schema not found: {requested}"), ctx);
    };

    match host
        .db
        .list_schema_record_keys(
            &requested,
            &canonical,
            limit,
            cursor.as_deref(),
            hash_filter.as_deref(),
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
