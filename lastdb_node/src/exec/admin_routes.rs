use super::*;

pub(super) async fn execute_status_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    // Serve the sampler snapshot. Building a status document used to admit
    // groups (242 cold loads per call on the primary). After the first
    // sample this path is atomics + a clone.
    let mut snapshot = if let Some(snapshot) = host.self_metrics.published_status() {
        snapshot
    } else {
        let snapshot = crate::jobs::self_metrics::status_snapshot(host).await;
        host.self_metrics.publish_status(snapshot.clone());
        snapshot
    };
    // Default fleet health must stay small: the 256-sample request_ops ring +
    // ranking tables dominate the status body (~97% of ~188 KB on a busy
    // primary). Forensic consumers opt in with ?recent=1 / ?forensics=1
    // (`lastdb ops` always does). Scalars sample_count / ring_capacity remain.
    let wants_forensics = query_flag(&req.target, "recent") || query_flag(&req.target, "forensics");
    if wants_forensics {
        // Diagnostic callers need the current ring, not the minute-old sampler
        // copy. This bounded in-memory snapshot does not fetch records.
        snapshot.request_ops = host.request_telemetry.snapshot();
    } else {
        snapshot.request_ops = snapshot.request_ops.for_cheap_health();
    }
    // Additive PR-5 contract block: unit + window + availability for every
    // typed gauge. Existing field names/types stay wire-compatible.
    let mut status = match crate::ops::status_gauge_contract::status_value_with_contract(&snapshot)
    {
        Ok(v) => v,
        Err(e) => {
            return error_response(500, &format!("status contract serialize: {e}"), ctx);
        }
    };
    status["search"] = fold_db::db_operations::search_index::queue_policy::status(
        &fold_db::db_operations::search_index::search_inbox_dir_for_home(&host.home),
    );
    json_ok(&envelope(
        &serde_json::json!({ "status": status }),
        ctx.user_id.as_str(),
    ))
}

pub(super) async fn execute_admin_shed_route(ctx: &AccessContext, host: &Host) -> UdsResponse {
    let report = crate::jobs::footprint::shed_memory(host).await;
    json_ok(&envelope(
        &serde_json::json!({ "shed": report }),
        ctx.user_id.as_str(),
    ))
}

/// `GET /api/system/log-filter` — report the tracing directive in force.
///
/// `installed: false` is a 200, not an error: the question "can this process
/// change its log level" has an answer on a node that cannot, and an operator
/// probing the capability should not have to distinguish a missing feature from
/// a failed request.
pub(super) fn execute_log_filter_get_route(ctx: &AccessContext) -> UdsResponse {
    let installed = crate::ops::log_filter::is_installed();
    json_ok(&envelope(
        &serde_json::json!({
            "log_filter": {
                "installed": installed,
                "directive": crate::ops::log_filter::current(),
            }
        }),
        ctx.user_id.as_str(),
    ))
}

/// `POST /api/system/log-filter` — swap the tracing directive at runtime.
///
/// Body: `{"directive": "<EnvFilter directive>"}`, e.g.
/// `fold_db::fold_db_core::query::hash_range_query=debug,info`.
///
/// A malformed directive is a 400 and leaves the active filter untouched — the
/// reload handle parses before it installs. That distinction is the whole point
/// of the route: an operator raising verbosity to chase a live symptom must not
/// be able to silence the daemon with a typo.
pub(super) fn execute_log_filter_set_route(req: &UdsRequest, ctx: &AccessContext) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
        directive: String,
    }
    let body: Body = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => {
            return error_response(
                400,
                &format!("invalid /api/system/log-filter body: {e}"),
                ctx,
            )
        }
    };
    let directive = body.directive.trim();
    if directive.is_empty() {
        return error_response(
            400,
            "log-filter: directive must be non-empty (pass e.g. \"info\" to restore the default)",
            ctx,
        );
    }
    match crate::ops::log_filter::apply(directive) {
        Ok(previous) => {
            // At INFO so the change is in the log the change affects. An
            // operator reading the file later can see when verbosity moved and
            // to what, which is the context every line after this one needs.
            tracing::info!(
                previous = %previous,
                directive = %directive,
                "tracing filter changed at runtime via /api/system/log-filter"
            );
            json_ok(&envelope(
                &serde_json::json!({
                    "log_filter": {
                        "installed": true,
                        "directive": directive,
                        "previous": previous,
                    }
                }),
                ctx.user_id.as_str(),
            ))
        }
        Err(e @ crate::ops::log_filter::LogFilterError::NotInstalled) => {
            error_response(503, &e.to_string(), ctx)
        }
        // Name the rejected directive. `EnvFilter`'s own parse error is a bare
        // phrase that repeats the wrapper's ("invalid filter directive:
        // invalid filter directive"), and it never quotes the input — so
        // without this the operator is told twice that something was invalid
        // and never told what.
        Err(e @ crate::ops::log_filter::LogFilterError::Rejected(_)) => error_response(
            400,
            &format!("log-filter: rejected directive {directive:?}: {e}"),
            ctx,
        ),
    }
}

/// `GET /api/db/inventory` — live main-tree key-class + per-schema record/history
/// byte breakdown (owner socket). Scans the open store; no offline exclusive open.
/// It writes nothing.
pub(super) async fn execute_db_inventory_route(ctx: &AccessContext, host: &Host) -> UdsResponse {
    match host.db.db_inventory().await {
        Ok(inventory) => json_ok(&envelope(
            &serde_json::json!({ "inventory": inventory }),
            ctx.user_id.as_str(),
        )),
        Err(e) => error_response(500, &format!("db inventory failed: {e}"), ctx),
    }
}

/// `GET /api/db/schemas` — record-only per-schema logical storage (owner socket).
pub(super) async fn execute_db_schemas_route(ctx: &AccessContext, host: &Host) -> UdsResponse {
    match host.db.db_schema_storage().await {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "schema_storage": report }),
            ctx.user_id.as_str(),
        )),
        Err(e) => error_response(500, &format!("db schemas failed: {e}"), ctx),
    }
}
