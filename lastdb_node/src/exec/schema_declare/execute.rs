//! Resolution payloads, audit, and the sync/check executors.
// lint:file-size-ok moved verbatim from schema_declare.rs; cohesive unit, split further in a later pass

use super::*;

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

    let mut audit = SchemaSyncAuditEvent::new(
        route,
        &crate::request_telemetry::client_from_request(req),
        ctx.user_id.as_str(),
        &declare.namespace,
        &declare.local_schema_id,
    );
    audit.proposal_identity_hash = declare.proposal.identity_hash.clone();

    let resolution = match load_catalog_schema_for_direct_declare(host, &declare).await {
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

    let facts = ResolutionFacts::new(&resolution);
    facts.stamp_audit(&mut audit);
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

    let mut payload = sync_response_payload(&declare, &resolution, &facts, &audit);
    retire_superseded_claims_into_payload(host, &declare, &facts, &mut payload).await;

    // The policy must exist before the first write, so it is applied before
    // this declare returns. Every refusal that can be known from the catalog
    // schema already happened before the bind. The named-db membership comes
    // first so a late failure leaves the declare complete: bound, a member,
    // and the retention step to repeat (it is idempotent).
    let retention = finish_declare_retention(host, req, ctx, &declare, &resolution).await;

    if let Err(e) = insert_declare_catalog_membership(host, ctx, &declare.local_schema_id).await {
        return error_response(
            500,
            &format!("schema declared but named-db catalog membership failed: {e}"),
            ctx,
        );
    }

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

/// Response body for a completed catalog sync: the resolution payload plus
/// the audit event identity.
fn sync_response_payload(
    declare: &DirectDeclareProposal,
    resolution: &DirectDeclareResolution,
    facts: &ResolutionFacts<'_>,
    audit: &SchemaSyncAuditEvent,
) -> Value {
    let mut payload = declare_resolution_payload(declare, resolution, facts);
    payload["audit_event_id"] = serde_json::json!(audit.event_id);
    payload["bind_eligible"] = serde_json::json!(audit.bind_eligible);
    payload
}

/// Retire name claims that a rekey superseded and report them in `payload`.
/// A retirement failure is logged, not returned: the declare already
/// succeeded and is audited.
async fn retire_superseded_claims_into_payload(
    host: &Host,
    declare: &DirectDeclareProposal,
    facts: &ResolutionFacts<'_>,
    payload: &mut Value,
) {
    // A rekey declares the same product under the same readable name with a
    // different key layout, which mints a new identity and leaves the
    // predecessor Available under the SAME `descriptive_name`. That is the
    // residue the 2026-09-04 board_cards rekey left behind: the local pin
    // moved, the old claim did not. Retire it here so the rekey path cannot
    // repeat it, instead of leaving an operator a manual step they will not
    // know to run.
    let keeper_candidates = [
        facts.canonical.map(str::to_string),
        facts.canonical.map(str::to_string),
        Some(declare.local_schema_id.clone()),
    ];
    let keeper_candidates: Vec<String> = keeper_candidates.into_iter().flatten().collect();
    match retire_superseded_name_claims(host, &keeper_candidates).await {
        Ok(retired) => {
            if !retired.is_empty() {
                payload["retired_name_claims"] = serde_json::json!(retired);
            }
        }
        Err(e) => {
            // The declare itself succeeded and is already audited. A failed
            // claim retirement leaves a duplicate name, which is a 409 on the
            // next name lookup — visible, and repairable with the explicit
            // route. Do not fail the declare over it.
            tracing::warn!(
                schema = %declare.local_schema_id,
                error = %e,
                "declare succeeded but retiring superseded name claims failed"
            );
        }
    }
}

/// Read-only `check` intent: report what catalog sync would do.
///
/// The response carries the same `resolution` vocabulary as catalog sync
/// (`reuse`, `compose`, `register`, `expand`) with `dry_run: true`,
/// `bind_eligible: false`, and no `audit_event_id`. A `register` / `expand`
/// plan reports the proposal identity; Schema Service can store a different
/// canonical when the sync runs. A plan that catalog sync would refuse (for
/// example an invalid or missing field mapper, or a conflicting alias) is a
/// 409 with the same error text. Nothing is written: no Schema Service
/// registration, no schema load or bind, no alias row, no name-claim
/// retirement, no named-db membership, and no schema-sync audit event.
///
/// A declared `retention` is previewed in the `retention` object: the TTL a
/// sync would apply, the policy in force now, and any layout warning.
pub(crate) async fn execute_catalog_schema_check(
    ctx: &AccessContext,
    host: &Host,
    declare: DirectDeclareProposal,
) -> UdsResponse {
    if let Some(response) = logical_layout_error(ctx, &declare.catalog_schema) {
        return response;
    }
    let plan = match plan_catalog_schema_for_direct_declare(host, &declare).await {
        Ok(plan) => plan,
        Err(e) => return error_response(409, &e, ctx),
    };
    let facts = ResolutionFacts::new(&plan.resolution);
    let mut payload = declare_resolution_payload(&declare, &plan.resolution, &facts);
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

pub(crate) async fn insert_declare_catalog_membership(
    host: &Host,
    ctx: &AccessContext,
    schema_name: &str,
) -> Result<(), String> {
    let Some(locator) = ctx.db_locator.as_deref() else {
        return Ok(());
    };
    if locator.trim().is_empty() || schema_name.trim().is_empty() {
        return Ok(());
    }
    host.db
        .ensure_catalog_membership(locator, schema_name, ctx.storage_prefix.clone())
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Normalize an `owner_app_id` so `None` and `Some("")` compare equal.
pub(crate) fn norm_owner_app_id(owner: Option<&str>) -> Option<&str> {
    owner.map(str::trim).filter(|owner| !owner.is_empty())
}

/// Retire every OTHER installed schema that claims the keeper's
/// `descriptive_name` under the same owner.
///
/// `keeper_candidates` are tried in order; the first one installed on this
/// node is the keeper. Returns the schema names whose claim this call actually
/// retired (already-retired claimants are not reported again, so a re-declare
/// is honestly a no-op).
///
/// Only the claim moves. Every retired schema keeps its identity hash, its
/// `Available` state, and its data, so a reader pinned to its hash — which is
/// exactly what a rekey predecessor pin is — keeps reading it.
pub(crate) async fn retire_superseded_name_claims(
    host: &Host,
    keeper_candidates: &[String],
) -> Result<Vec<String>, String> {
    let mgr = host.db.schema_manager();
    let mut keeper = None;
    for candidate in keeper_candidates {
        match mgr.get_schema_metadata(candidate) {
            Ok(Some(schema)) => {
                keeper = Some(schema);
                break;
            }
            Ok(None) => {}
            Err(e) => return Err(e.to_string()),
        }
    }
    let Some(keeper) = keeper else {
        return Ok(Vec::new());
    };
    let Some(descriptive_name) = keeper.descriptive_name.clone() else {
        return Ok(Vec::new());
    };
    let keeper_owner = norm_owner_app_id(keeper.owner_app_id.as_deref()).map(str::to_string);

    let entries = mgr
        .get_schema_list_entries_with_states()
        .map_err(|e| e.to_string())?;
    let mut retired = Vec::new();
    for entry in entries {
        if entry.name == keeper.name {
            continue;
        }
        if norm_owner_app_id(entry.owner_app_id.as_deref()).map(str::to_string) != keeper_owner {
            continue;
        }
        if entry.descriptive_name.as_deref() != Some(descriptive_name.as_str()) {
            continue;
        }
        if mgr
            .retire_name_claim(&entry.name)
            .await
            .map_err(|e| e.to_string())?
        {
            tracing::info!(
                retired = %entry.name,
                keeper = %keeper.name,
                descriptive_name = %descriptive_name,
                "retired a superseded claim on a descriptive_name"
            );
            retired.push(entry.name);
        }
    }
    retired.sort();
    Ok(retired)
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
