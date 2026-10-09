//! Resolution payloads, audit, and the sync/check executors.
// lint:file-size-ok moved verbatim from schema_declare.rs; cohesive unit, split further in a later pass

use super::*;

pub(crate) fn declare_resolution_payload(
    declare: &DirectDeclareProposal,
    resolution: &DirectDeclareResolution,
) -> Value {
    match resolution {
        DirectDeclareResolution::Register {
            catalog_hash,
            field_mappers,
            replaced_schema,
        } => {
            let adapter = serde_json::json!({
                "kind": "registered_expansion",
                "local_schema": declare.local_schema_id,
                "catalog_schema": catalog_hash,
                "field_mappers": field_mappers,
            });
            serde_json::json!({
                "namespace": declare.namespace,
                "schema_name": declare.local_schema_id.clone(),
                "descriptive_name": declare.proposal.descriptive_name,
                "identity_hash": catalog_hash,
                "canonical": catalog_hash,
                "resolution": if replaced_schema.is_some() { "expand" } else { "register" },
                "replaced_schema": replaced_schema,
                "adapter": adapter,
                "adapters": [adapter],
            })
        }
        DirectDeclareResolution::Reuse {
            catalog_hash,
            field_mappers,
            confidence,
        } => {
            let adapter = serde_json::json!({
                "kind": "edge_only",
                "local_schema": declare.local_schema_id,
                "catalog_schema": catalog_hash,
                "field_mappers": field_mappers,
            });
            let mut payload = serde_json::json!({
                "namespace": declare.namespace,
                "schema_name": declare.local_schema_id.clone(),
                "descriptive_name": declare.proposal.descriptive_name,
                "identity_hash": catalog_hash,
                "resolution": "reuse",
                "adapter": adapter,
                "adapters": [adapter],
            });
            if let Some(c) = confidence {
                payload["confidence"] = serde_json::json!(c);
            }
            payload
        }
        DirectDeclareResolution::Compose {
            components,
            confidence,
        } => {
            let adapters: Vec<Value> = components
                .iter()
                .map(|c| {
                    let mut a = serde_json::json!({
                        "kind": "edge_only",
                        "local_schema": declare.local_schema_id,
                        "catalog_schema": c.catalog_hash,
                        "field_mappers": c.field_mappers,
                    });
                    if let Some(ref n) = c.matched_descriptive_name {
                        a["matched_descriptive_name"] = serde_json::json!(n);
                    }
                    if !c.unmapped_fields.is_empty() {
                        a["unmapped_fields"] = serde_json::json!(c.unmapped_fields);
                    }
                    if let Some(v) = c.is_exact_match {
                        a["is_exact_match"] = serde_json::json!(v);
                    }
                    if let Some(v) = c.is_superset {
                        a["is_superset"] = serde_json::json!(v);
                    }
                    a
                })
                .collect();
            let component_hashes: Vec<String> =
                components.iter().map(|c| c.catalog_hash.clone()).collect();
            let mut payload = serde_json::json!({
                "namespace": declare.namespace,
                "schema_name": declare.local_schema_id.clone(),
                "descriptive_name": declare.proposal.descriptive_name,
                "identity_hash": serde_json::Value::Null,
                "resolution": "compose",
                "component_catalog_hashes": component_hashes,
                "adapters": adapters,
                "adapter": adapters.first().cloned().unwrap_or(serde_json::json!({})),
            });
            if let Some(c) = confidence {
                payload["confidence"] = serde_json::json!(c);
            }
            payload
        }
    }
}

pub(crate) fn audit_resolution(
    event: &mut SchemaSyncAuditEvent,
    resolution: &DirectDeclareResolution,
) {
    let mut predecessors = HashSet::new();
    match resolution {
        DirectDeclareResolution::Register {
            catalog_hash,
            field_mappers,
            replaced_schema,
        } => {
            event.outcome = if replaced_schema.is_some() {
                "expand".into()
            } else {
                "register".into()
            };
            event.catalog_hashes.push(catalog_hash.clone());
            event.mapper_count = field_mappers.len();
            if let Some(hash) = replaced_schema {
                predecessors.insert(hash.clone());
            }
            predecessors.extend(
                field_mappers
                    .values()
                    .filter_map(|mapper| mapper.split_once('.').map(|(hash, _)| hash.to_string())),
            );
        }
        DirectDeclareResolution::Reuse {
            catalog_hash,
            field_mappers,
            ..
        } => {
            event.outcome = "reuse".into();
            event.catalog_hashes.push(catalog_hash.clone());
            event.mapper_count = field_mappers.len();
            predecessors.extend(
                field_mappers
                    .values()
                    .filter_map(|mapper| mapper.split_once('.').map(|(hash, _)| hash.to_string())),
            );
        }
        DirectDeclareResolution::Compose { components, .. } => {
            event.outcome = "compose".into();
            event.catalog_hashes = components
                .iter()
                .map(|component| component.catalog_hash.clone())
                .collect();
            event.mapper_count = components
                .iter()
                .map(|component| component.field_mappers.len())
                .sum();
            for component in components {
                predecessors.extend(
                    component.field_mappers.values().filter_map(|mapper| {
                        mapper.split_once('.').map(|(hash, _)| hash.to_string())
                    }),
                );
            }
        }
    }
    event.predecessor_hashes = predecessors.into_iter().collect();
    event.predecessor_hashes.sort();
    event.bind_eligible = true;
}

// lint:fn-size-ok moved verbatim from schema_declare.rs; splitting this function is separate work
pub(crate) async fn execute_catalog_schema_sync(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
    route: &str,
    declare: DirectDeclareProposal,
) -> UdsResponse {
    if crate::ephemeral::is_ephemeral() {
        return error_response(
            403,
            "schema catalog synchronization is not permitted on ephemeral nodes; \
             use check intent for read-only inspection or run against the primary node",
            ctx,
        );
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

    audit_resolution(&mut audit, &resolution);
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
    payload["app_id"] = serde_json::json!(declare.namespace);
    payload["schema"] = serde_json::json!(declare.local_schema_id);
    payload["audit_event_id"] = serde_json::json!(audit.event_id);
    payload["bind_eligible"] = serde_json::json!(audit.bind_eligible);
    if let Some(hash) = payload.get("identity_hash").cloned() {
        if !hash.is_null() {
            payload["canonical"] = hash;
        }
    }

    // A rekey declares the same product under the same readable name with a
    // different key layout, which mints a new identity and leaves the
    // predecessor Available under the SAME `descriptive_name`. That is the
    // residue the 2026-09-04 board_cards rekey left behind: the local pin
    // moved, the old claim did not. Retire it here so the rekey path cannot
    // repeat it, instead of leaving an operator a manual step they will not
    // know to run.
    let keeper_candidates = [
        payload
            .get("canonical")
            .and_then(Value::as_str)
            .map(str::to_string),
        payload
            .get("identity_hash")
            .and_then(Value::as_str)
            .map(str::to_string),
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

    if let Err(e) = insert_declare_catalog_membership(host, ctx, &declare.local_schema_id).await {
        return error_response(
            500,
            &format!("schema declared but named-db catalog membership failed: {e}"),
            ctx,
        );
    }

    json_ok(&envelope(&payload, ctx.user_id.as_str()))
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
pub(crate) async fn execute_catalog_schema_check(
    ctx: &AccessContext,
    host: &Host,
    declare: DirectDeclareProposal,
) -> UdsResponse {
    let plan = match plan_catalog_schema_for_direct_declare(host, &declare).await {
        Ok(plan) => plan,
        Err(e) => return error_response(409, &e, ctx),
    };
    let mut payload = declare_resolution_payload(&declare, &plan.resolution);
    payload["app_id"] = serde_json::json!(declare.namespace);
    payload["schema"] = serde_json::json!(declare.local_schema_id);
    payload["intent"] = serde_json::json!("check");
    payload["dry_run"] = serde_json::json!(true);
    payload["bind_eligible"] = serde_json::json!(false);
    if let Some(reason) = plan.register_reason {
        payload["register_reason"] = serde_json::json!(reason);
    }
    if let Some(hash) = payload.get("identity_hash").cloned() {
        if !hash.is_null() {
            payload["canonical"] = hash;
        }
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
        .db_ops()
        .db_catalog()
        .ensure_named_membership(locator, schema_name, ctx.storage_prefix.clone())
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
    ctx: &AccessContext,
    host: &Host,
    route: &str,
    status: u16,
    error: &str,
) -> UdsResponse {
    // A refused read-only check writes nothing, not even an audit event.
    if raw_body_is_check_intent(&req.body) {
        return error_response(status, error, ctx);
    }
    let raw = serde_json::from_slice::<Value>(&req.body).unwrap_or_default();
    let app_id = raw
        .get("app_id")
        .or_else(|| raw.get("namespace"))
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let schema = raw
        .get("schema")
        .and_then(|schema| schema.get("name"))
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let mut audit = SchemaSyncAuditEvent::new(
        route,
        &crate::request_telemetry::client_from_request(req),
        ctx.user_id.as_str(),
        app_id,
        schema,
    );
    audit.proposal_identity_hash = raw
        .get("schema")
        .and_then(|schema| schema.get("identity_hash"))
        .and_then(Value::as_str)
        .map(str::to_string);
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
