use super::*;

mod change_rules;
use change_rules::AppChangesRequest;
pub(in crate::exec) mod alternate_list;
pub(in crate::exec) mod list_rules;
use list_rules::ListRecordKeysRequest;

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
    let last_seq = host.db.db_ops().change_feed().last_seq().await;
    changes.retain(|event| change_visible_to_caller(host, ctx, event, target.as_deref()));
    render(
        Ok(change_rules::reply(
            changes,
            examined_cursor,
            last_seq,
            page.gap,
        )),
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
    ctx.is_owner
}

// ---------------------------------------------------------------------------
// Schemas (host-local: catalog listing / fetch / owner declare)
// ---------------------------------------------------------------------------

/// Count live records through the ordinary bounded query route. A failed or
/// unavailable count is unknown, never evidence that the schema is empty.
pub(super) async fn count_schema_records(host: &Host, canonical: &str) -> Option<usize> {
    let mgr = host.db.schema_manager();
    let schema = mgr.resolve_schema(canonical).await.ok()??;
    if schema.runtime_fields.is_empty() {
        return Some(0);
    }

    let query = fold_db::schema::types::Query::new(canonical.to_string());
    host.db
        .query_executor()
        .count_query_rows(&query)
        .await
        .ok()
        .flatten()
}

/// `GET /api/list?schema=` — paged API keys for live schema records.
pub(super) async fn execute_list_record_keys_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let request = match ListRecordKeysRequest::parse(&req.target) {
        Ok(request) => request,
        Err(message) => return error_response(400, message, ctx),
    };

    let resolved = match handlers::resolve_schema_name(host, &request.requested) {
        Ok(v) => v,
        Err(e) => return render(Err(e), ctx),
    };
    let Some(canonical) = resolved else {
        return error_response(
            404,
            &format!("schema not found: {}", request.requested),
            ctx,
        );
    };

    if request.key_name.is_some() {
        return alternate_list::execute(host, ctx, &request, &canonical).await;
    }

    match host
        .db
        .list_schema_record_keys(fold_db::db_operations::SchemaRecordKeysRequest {
            requested: &request.requested,
            schema_id: &canonical,
            limit: request.limit,
            cursor: request.cursor.as_deref(),
            hash_filter: request.hash_filter.as_deref(),
        })
        .await
    {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "list": report }),
            ctx.user_id.as_str(),
        )),
        // Schema and cursor failures use the canonical 404/400 mapping.
        Err(e) => mapped_error_response("list failed", e, ctx),
    }
}
