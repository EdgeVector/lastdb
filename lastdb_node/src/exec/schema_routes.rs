//! Schema catalog listing, lookup, alias resolution, system seed, and schema drop routes.

use std::collections::HashSet;

use fold_db::access::AccessContext;
use fold_db::schema::types::{DeclarativeSchemaDefinition, Schema};
use fold_db::schema::{SchemaState, SchemaWithState};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::reply::{
    content_free, envelope, error_response, json_ok, mapped_error_response, render, HostError,
};
use super::schema_declare::logical_layout_error;
use super::wire::{query_flag, schema_name_from_target};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

fn annotate_schemas<T: Serialize>(schemas: &[T]) -> Option<Vec<Value>> {
    schemas
        .iter()
        .map(|schema| {
            let mut row = serde_json::to_value(schema).ok()?;
            let obj = row.as_object_mut()?;
            obj.insert("record_count".into(), Value::Null);
            obj.insert("has_data".into(), Value::Null);
            Some(row)
        })
        .collect()
}

pub(in crate::exec) fn execute_list_schemas_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if query_flag(&req.target, "include_counts") {
        return error_response(400, "schema listing does not count records", ctx);
    }
    let include_full = query_flag(&req.target, "include_full");
    let mgr = host.records().schema_manager();
    let annotated = if include_full {
        mgr.get_schemas_with_states().ok().and_then(|s| {
            let active: Vec<_> = s
                .into_iter()
                .filter(|e| e.state != SchemaState::Blocked)
                .collect();
            annotate_schemas(&active)
        })
    } else {
        mgr.get_active_schema_list_entries_with_states()
            .ok()
            .and_then(|s| annotate_schemas(&s))
    };
    let Some(annotated) = annotated else {
        return content_free(500, "Internal Server Error");
    };
    let newer = mgr.schemas_with_newer_identity_algo().unwrap_or_default();
    let payload = json!({
        "schemas": annotated, "count": annotated.len(), "ready": true,
        "counts_included": false, "full_included": include_full,
        "identity_algo_version": schema_types::IDENTITY_HASH_ALGO_VERSION,
        "newer_identity_algo_count": newer.len(), "newer_identity_algo_schemas": newer,
    });
    json_ok(&envelope(&payload, ctx.user_id.as_str()))
}

pub(in crate::exec) fn execute_get_schema_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let Some(name) = schema_name_from_target(&req.target) else {
        return content_free(400, "Bad Request");
    };
    let canonical = match resolve_schema_name(host, &name) {
        Ok(Some(c)) => c,
        Ok(None) => return error_response(404, &format!("Schema not found: {name}"), ctx),
        Err(e) => return render(Err(e), ctx),
    };
    let mgr = host.records().schema_manager();
    match mgr.get_schema_metadata(&canonical) {
        Ok(Some(schema)) => {
            let state = mgr
                .get_schema_states()
                .ok()
                .and_then(|s| s.get(&canonical).copied())
                .unwrap_or_default();
            let sws = SchemaWithState::new(schema, state);
            json_ok(&envelope(&json!({ "schema": sws }), ctx.user_id.as_str()))
        }
        Ok(None) => error_response(404, &format!("Schema not found: {name}"), ctx),
        Err(_) => content_free(500, "Internal Server Error"),
    }
}

pub(in crate::exec) async fn execute_seed_system_schema_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Request {
        schema: DeclarativeSchemaDefinition,
        #[serde(default)]
        isolated_copy: bool,
    }
    let request = match serde_json::from_slice::<Request>(&req.body) {
        Ok(r) => r,
        Err(e) => {
            return error_response(400, &format!("invalid system seed setup request: {e}"), ctx)
        }
    };
    if !request.isolated_copy || !fold_db::env_flag::var_truthy("LASTDB_ISOLATED_COPY") {
        return error_response(
            409,
            "system seed setup requires isolated_copy=true and LASTDB_ISOLATED_COPY=1",
            ctx,
        );
    }
    if request
        .schema
        .owner_app_id
        .as_deref()
        .is_some_and(|o| !o.trim().is_empty())
    {
        return error_response(
            400,
            "system seed setup requires a schema without owner_app_id",
            ctx,
        );
    }
    if let Some(resp) = logical_layout_error(ctx, &request.schema) {
        return resp;
    }
    if let Err(e) = request.schema.validate() {
        return mapped_error_response("system seed setup failed", e.into(), ctx);
    }
    let name = request.schema.name.clone();
    match host
        .records()
        .schema_manager()
        .load_schema_internal(request.schema)
        .await
    {
        Ok(()) => json_ok(&envelope(
            &json!({ "system_seed_schema": name, "source": "system_seed" }),
            ctx.user_id.as_str(),
        )),
        Err(e) => mapped_error_response("system seed setup failed", e.into(), ctx),
    }
}

pub(in crate::exec) async fn execute_drop_schema_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
        #[serde(default)]
        schema: String,
        #[serde(default)]
        owner_app: String,
        #[serde(default)]
        must_exist: bool,
    }
    let Ok(body) = serde_json::from_slice::<Body>(&req.body) else {
        return error_response(400, "invalid /api/schemas/drop body", ctx);
    };
    let (schema, owner_app) = (body.schema.trim(), body.owner_app.trim());
    if schema.is_empty() == owner_app.is_empty() {
        return error_response(400, "pass exactly one of schema or owner_app", ctx);
    }
    let mgr = host.records().schema_manager();
    if !owner_app.is_empty() {
        let Ok(names) = mgr.schema_names_owned_by(owner_app) else {
            return error_response(500, "schema drop failed", ctx);
        };
        let mut dropped = Vec::new();
        for name in names {
            match mgr.drop_schema(&name).await {
                Ok(true) => dropped.push(name),
                Ok(false) => {}
                Err(e) => return error_response(500, &format!("schema drop failed: {e}"), ctx),
            }
        }
        dropped.sort();
        if body.must_exist && dropped.is_empty() {
            return error_response(
                404,
                &format!("no installed schema is owned by '{owner_app}'"),
                ctx,
            );
        }
        return json_ok(&envelope(
            &json!({ "dropped": dropped, "existed": !dropped.is_empty(), "owner_app": owner_app }),
            ctx.user_id.as_str(),
        ));
    }
    let target = match resolve_schema_name(host, schema) {
        Ok(t) => t,
        Err(e) => match blocked_drop_target(host, schema, &e) {
            Some(c) => Some(c),
            None => return render(Err(e), ctx),
        },
    };
    let (canonical, existed) = match target {
        Some(c) => match mgr.drop_schema(&c).await {
            Ok(existed) => (c, existed),
            Err(e) => return error_response(500, &format!("schema drop failed: {e}"), ctx),
        },
        None => (schema.to_string(), false),
    };
    if body.must_exist && !existed {
        return error_response(
            404,
            &format!("no schema is installed under '{canonical}'"),
            ctx,
        );
    }
    let dropped: Vec<String> = existed.then(|| canonical.clone()).into_iter().collect();
    json_ok(&envelope(
        &json!({ "dropped": dropped, "existed": existed, "schema": canonical }),
        ctx.user_id.as_str(),
    ))
}

fn blocked_drop_target(host: &Host, requested: &str, error: &HostError) -> Option<String> {
    (error.status == 400).then_some(())?;
    let schemas = host
        .records()
        .schema_manager()
        .get_schemas_with_states()
        .ok()?;
    let mut blocked = schemas.into_iter().filter(|e| {
        e.state == SchemaState::Blocked
            && (e.schema.descriptive_name.as_deref() == Some(requested)
                || e.schema.identity_hash.as_deref() == Some(requested))
    });
    let first = blocked.next()?;
    blocked.next().is_none().then(|| first.schema.name.clone())
}

pub(in crate::exec) fn resolve_schema_name(
    host: &Host,
    name: &str,
) -> Result<Option<String>, HostError> {
    let mgr = host.records().schema_manager();
    let has_meta = |n: &str| {
        mgr.get_schema_metadata(n)
            .map(|s| s.is_some())
            .map_err(|e| HostError::internal(e.to_string()))
    };
    if has_meta(name)? {
        return Ok(Some(name.to_string()));
    }
    let schemas = mgr
        .get_schemas_with_states()
        .map_err(|e| HostError::internal(e.to_string()))?;
    if let Some(c) = resolve_exact_alias(name, &schemas)? {
        return Ok(Some(c));
    }
    let trimmed = name.trim();
    if trimmed != name && !trimmed.is_empty() && has_meta(trimmed)? {
        return Ok(Some(trimmed.to_string()));
    }
    resolve_folded(trimmed, &schemas)
}

pub(in crate::exec) fn resolve_captured_schema_alias(
    name: &str,
    entries: &[SchemaWithState],
) -> Result<Option<String>, HostError> {
    if let Some(c) = resolve_exact_alias(name, entries)? {
        Ok(Some(c))
    } else {
        resolve_folded(name.trim(), entries)
    }
}

fn resolve_folded(t: &str, schemas: &[SchemaWithState]) -> Result<Option<String>, HostError> {
    if t.is_empty() {
        return Ok(None);
    }
    let f = t.to_lowercase();
    resolve_matches(
        t,
        schemas.iter().filter(|e| {
            e.schema
                .descriptive_name
                .as_deref()
                .is_some_and(|v| v.to_lowercase() == f)
        }),
    )
}

fn resolve_exact_alias(
    req: &str,
    schemas: &[SchemaWithState],
) -> Result<Option<String>, HostError> {
    let by_hash = schemas
        .iter()
        .filter(|e| e.schema.identity_hash.as_deref() == Some(req));
    if let Some(c) = resolve_matches(req, by_hash)? {
        return Ok(Some(c));
    }
    if let Some((owner, local)) = req
        .split_once('/')
        .filter(|(o, l)| !o.is_empty() && !l.is_empty())
    {
        let m = schemas.iter().filter(|e| {
            e.schema.owner_app_id.as_deref() == Some(owner)
                && (e.schema.name == local || e.schema.descriptive_name.as_deref() == Some(local))
        });
        if let Some(c) = resolve_matches(req, m)? {
            return Ok(Some(c));
        }
    }
    let by_desc = schemas
        .iter()
        .filter(|e| e.schema.descriptive_name.as_deref() == Some(req));
    resolve_matches(req, by_desc)
}

fn resolve_matches<'a>(
    req: &str,
    matches: impl Iterator<Item = &'a SchemaWithState>,
) -> Result<Option<String>, HostError> {
    let (mut blocked, mut seen, mut candidates) = (false, HashSet::new(), Vec::<&Schema>::new());
    for entry in matches {
        if entry.state == SchemaState::Blocked {
            blocked = true;
        } else if seen.insert(entry.schema.name.as_str()) {
            candidates.push(&entry.schema);
        }
    }
    let names: HashSet<&str> = candidates.iter().map(|s| s.name.as_str()).collect();
    candidates.retain(|s| {
        s.name
            .split_once(':')
            .is_none_or(|(_, base)| !names.contains(base))
    });
    match candidates.len() {
        0 if blocked => Err(HostError::new(
            400,
            format!("Schema '{req}' is blocked and cannot be queried or mutated"),
        )),
        0 => Ok(None),
        1 => Ok(Some(candidates[0].name.clone())),
        _ => {
            candidates.sort_by(|a, b| a.name.cmp(&b.name));
            let details = candidates
                .iter()
                .map(|s| {
                    let desc = s.descriptive_name.as_deref().unwrap_or("<none>");
                    let owner = s.owner_app_id.as_deref().unwrap_or("<none>");
                    format!(
                        "canonical={} descriptive_name={desc} owner_app_id={owner}",
                        s.name
                    )
                })
                .collect::<Vec<_>>()
                .join("; ");
            Err(HostError::new(
                409,
                format!("descriptive_name '{req}' is ambiguous; pin the schema by canonical hash. Candidates: {details}. Example: lastdb query <canonical-hash> --fields <fields>"),
            ))
        }
    }
}
