//! Resolution payloads, audit, and the sync/check executors.

use super::*;

// The request holds this lock through the policy write and audit completion.
static DECLARE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub(crate) async fn execute_catalog_schema_sync(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
    route: &str,
    declare: DirectDeclareProposal,
) -> UdsResponse {
    if let Some(response) = super::logical_admission::sync_admission_error(ctx, &declare) {
        return response;
    }

    let _declare_guard = DECLARE_LOCK.lock().await;
    let mut audit = SchemaSyncAuditEvent::new(
        route,
        &crate::request_telemetry::client_from_request(req),
        ctx.user_id.as_str(),
        &declare.namespace,
        &declare.local_schema_id,
    );
    audit.proposal_identity_hash = declare.catalog_schema.identity_hash.clone();

    let resolution = match super::owned::load(host, &declare).await {
        Ok(resolution) => resolution,
        Err(e) => {
            audit.error = Some(e.clone());
            if let Err(audit_error) = schema_sync_audit::append(&host.home, &audit) {
                return error_response(
                    500,
                    &format!(
                        "schema sync failed ({e}); audit persistence also failed: {audit_error}"
                    ),
                    ctx,
                );
            }
            return error_response(409, &e, ctx);
        }
    };

    resolution.stamp_audit(&mut audit);
    if let Err(e) = schema_sync_audit::append(&host.home, &audit) {
        return error_response(
            500,
            &format!(
                "catalog schema synchronized but audit persistence failed: {e}; refusing bind"
            ),
            ctx,
        );
    }

    tracing::info!(
        target: "lastdb_node::schema_sync_audit",
        event_id = %audit.event_id,
        app_id = %audit.app_id,
        schema = %audit.schema,
        outcome = %audit.outcome,
        mapper_count = audit.mapper_count,
        "canonical app schema sync completed"
    );

    let mut payload = declare_resolution_payload(&declare, &resolution);
    payload["audit_event_id"] = serde_json::json!(audit.event_id);
    payload["bind_eligible"] = serde_json::json!(audit.bind_eligible);

    // Install the policy before the declaration permits its first write.
    let retention = finish_declare_retention(host, req, ctx, &declare, &resolution).await;

    match retention {
        Ok(Some(retention)) => payload["retention"] = retention,
        Ok(None) => {}
        Err(e) => {
            return error_response(
                409,
                &format!("schema declared and bound, but its retention step failed: {e}"),
                ctx,
            )
        }
    }

    json_ok(&envelope(&payload, ctx.user_id.as_str()))
}

/// Read-only declaration preview. It checks the exact definition and retention
/// policy without registration, schema writes, or audit writes.
pub(crate) async fn execute_catalog_schema_check(
    ctx: &AccessContext,
    host: &Host,
    declare: DirectDeclareProposal,
) -> UdsResponse {
    if let Some(response) = logical_layout_error(ctx, &declare.catalog_schema) {
        return response;
    }
    let plan = match super::owned::plan(host, &declare).await {
        Ok(plan) => plan,
        Err(e) => return error_response(409, &e, ctx),
    };
    let mut payload = declare_resolution_payload(&declare, &plan.resolution);
    payload["intent"] = serde_json::json!("check");
    payload["dry_run"] = serde_json::json!(true);
    payload["bind_eligible"] = serde_json::json!(false);
    if let Some(reason) = plan.register_reason {
        payload["register_reason"] = serde_json::json!(reason);
    }
    match preview_declare_retention(host, &declare, &plan.resolution).await {
        Ok(Some(retention)) => payload["retention"] = retention,
        Ok(None) => {}
        Err(e) => return error_response(409, &e, ctx),
    }
    json_ok(&envelope(&payload, ctx.user_id.as_str()))
}

pub(crate) fn reject_schema_sync_request(
    req: &UdsRequest,
    facts: &DeclareRequestFacts,
    ctx: &AccessContext,
    host: &Host,
    route: &str,
    status: u16,
    error: &str,
) -> UdsResponse {
    // A refused read-only check writes nothing, not even an audit event.
    if facts.check {
        return error_response(status, error, ctx);
    }
    let mut audit = SchemaSyncAuditEvent::new(
        route,
        &crate::request_telemetry::client_from_request(req),
        ctx.user_id.as_str(),
        &facts.app_id,
        &facts.schema,
    );
    audit.proposal_identity_hash = facts.proposal_identity_hash.clone();
    audit.error = Some(error.to_string());
    if let Err(audit_error) = schema_sync_audit::append(&host.home, &audit) {
        return error_response(
            500,
            &format!("schema sync rejected ({error}); audit persistence failed: {audit_error}"),
            ctx,
        );
    }
    error_response(status, error, ctx)
}
