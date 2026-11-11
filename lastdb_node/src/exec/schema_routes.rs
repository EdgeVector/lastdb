//! Schema catalog listing, single-schema lookup, system seed, and schema drop routes.

use fold_db::access::AccessContext;
use fold_db::schema::schema_types::SchemaWithState;
use fold_db::schema::types::DeclarativeSchemaDefinition;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::reply::{
    content_free, envelope, error_response, json_ok, mapped_error_response, render, HostError,
};
use super::schema_declare::logical_layout_error;
use super::schema_resolve::resolve_schema_name;
use super::wire::{query_flag, schema_name_from_target};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

fn annotated_schema_row<T: Serialize>(schema: &T) -> Option<Value> {
    let Ok(Value::Object(mut row)) = serde_json::to_value(schema) else {
        return None;
    };
    row.insert("record_count".into(), Value::Null);
    row.insert("has_data".into(), Value::Null);
    Some(Value::Object(row))
}

fn annotate_schemas<T: Serialize>(schemas: &[T]) -> Option<Vec<Value>> {
    let mut annotated = Vec::with_capacity(schemas.len());
    for schema in schemas {
        annotated.push(annotated_schema_row(schema)?);
    }
    Some(annotated)
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

    let mgr = host.db.schema_manager();
    if !mgr.catalog_ready() {
        return error_response(
            503,
            "schema catalog not ready: still hydrating from store",
            ctx,
        );
    }

    let annotated = if include_full {
        mgr.get_active_schemas_with_states()
            .ok()
            .and_then(|schemas| annotate_schemas(&schemas))
    } else {
        mgr.get_active_schema_list_entries_with_states()
            .ok()
            .and_then(|schemas| annotate_schemas(&schemas))
    };
    let Some(annotated) = annotated else {
        return content_free(500, "Internal Server Error");
    };

    let count = annotated.len();
    let newer_identity_algo_schemas = mgr.schemas_with_newer_identity_algo().unwrap_or_default();
    let payload = serde_json::json!({
        "schemas": annotated,
        "count": count,
        "ready": true,
        "counts_included": false,
        "full_included": include_full,
        "identity_algo_version": schema_types::IDENTITY_HASH_ALGO_VERSION,
        "newer_identity_algo_count": newer_identity_algo_schemas.len(),
        "newer_identity_algo_schemas": newer_identity_algo_schemas,
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
        Ok(Some(canonical)) => canonical,
        Ok(None) => return error_response(404, &format!("Schema not found: {name}"), ctx),
        Err(e) => return render(Err(e), ctx),
    };

    let mgr = host.db.schema_manager();
    let schema_with_state = match mgr.get_schema_metadata(&canonical) {
        Ok(Some(schema)) => {
            let state = mgr
                .get_schema_states()
                .ok()
                .and_then(|states| states.get(&canonical).copied())
                .unwrap_or_default();
            SchemaWithState::new(schema, state)
        }
        Ok(None) => return error_response(404, &format!("Schema not found: {name}"), ctx),
        Err(_) => return content_free(500, "Internal Server Error"),
    };

    let payload = serde_json::json!({ "schema": schema_with_state });
    json_ok(&envelope(&payload, ctx.user_id.as_str()))
}

/// `POST /api/schemas/seed-system` — install a system-owned schema on an explicitly isolated node.
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
        Ok(request) => request,
        Err(error) => {
            return error_response(
                400,
                &format!("invalid system seed setup request: {error}"),
                ctx,
            )
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
        .is_some_and(|owner| !owner.trim().is_empty())
    {
        return error_response(
            400,
            "system seed setup requires a schema without owner_app_id",
            ctx,
        );
    }

    if let Some(response) = logical_layout_error(ctx, &request.schema) {
        return response;
    }
    let schema_name = request.schema.name.clone();
    if let Err(error) = request.schema.validate() {
        return mapped_error_response("system seed setup failed", error.into(), ctx);
    }
    match host
        .db
        .schema_manager()
        .load_schema_internal(request.schema)
        .await
    {
        Ok(()) => json_ok(&envelope(
            &serde_json::json!({
                "system_seed_schema": schema_name,
                "source": "system_seed",
            }),
            ctx.user_id.as_str(),
        )),
        Err(error) => mapped_error_response("system seed setup failed", error.into(), ctx),
    }
}

/// `POST /api/schemas/drop` — remove one catalog identity, or every identity owned by one app.
pub(in crate::exec) async fn execute_drop_schema_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct DropSchemaRequest {
        #[serde(default)]
        schema: String,
        #[serde(default)]
        owner_app: String,
        #[serde(default)]
        must_exist: bool,
    }

    let body = match serde_json::from_slice::<DropSchemaRequest>(&req.body) {
        Ok(body) => body,
        Err(e) => return error_response(400, &format!("invalid /api/schemas/drop body: {e}"), ctx),
    };
    let schema = body.schema.trim();
    let owner_app = body.owner_app.trim();
    if schema.is_empty() == owner_app.is_empty() {
        return error_response(400, "pass exactly one of schema or owner_app", ctx);
    }

    if !owner_app.is_empty() {
        let dropped = match host.db.drop_schemas_owned_by_with_bindings(owner_app).await {
            Ok(dropped) => dropped,
            Err(e) => return error_response(500, &format!("schema drop failed: {e}"), ctx),
        };
        if body.must_exist && dropped.is_empty() {
            return error_response(
                404,
                &format!("no installed schema is owned by '{owner_app}'"),
                ctx,
            );
        }
        let payload = serde_json::json!({
            "dropped": dropped,
            "existed": !dropped.is_empty(),
            "owner_app": owner_app,
        });
        return json_ok(&envelope(&payload, ctx.user_id.as_str()));
    }

    let canonical = match resolve_schema_name(host, schema) {
        Ok(Some(canonical)) => canonical,
        Ok(None) => {
            if body.must_exist {
                return error_response(
                    404,
                    &format!("no schema is installed under '{schema}'"),
                    ctx,
                );
            }
            let payload = serde_json::json!({
                "dropped": Vec::<String>::new(),
                "existed": false,
                "schema": schema,
            });
            return json_ok(&envelope(&payload, ctx.user_id.as_str()));
        }
        Err(e) => {
            if let Some(canonical) = blocked_drop_target(host, schema, &e) {
                canonical
            } else {
                return render(Err(e), ctx);
            }
        }
    };

    let existed = match host.db.drop_schema_and_bindings(&canonical).await {
        Ok(existed) => existed,
        Err(e) => return error_response(500, &format!("schema drop failed: {e}"), ctx),
    };
    if body.must_exist && !existed {
        return error_response(
            404,
            &format!("no schema is installed under '{canonical}'"),
            ctx,
        );
    }
    let payload = serde_json::json!({
        "dropped": if existed { vec![canonical.clone()] } else { Vec::new() },
        "existed": existed,
        "schema": canonical,
    });
    json_ok(&envelope(&payload, ctx.user_id.as_str()))
}

fn blocked_drop_target(host: &Host, requested: &str, error: &HostError) -> Option<String> {
    if error.status != 400 {
        return None;
    }
    let schemas = host.db.schema_manager().get_schemas_with_states().ok()?;
    let mut blocked = schemas.into_iter().filter(|entry| {
        entry.state == fold_db::schema::SchemaState::Blocked
            && (entry.schema.descriptive_name.as_deref() == Some(requested)
                || entry.schema.identity_hash.as_deref() == Some(requested))
    });
    let first = blocked.next()?;
    blocked.next().is_none().then(|| first.schema.name.clone())
}
