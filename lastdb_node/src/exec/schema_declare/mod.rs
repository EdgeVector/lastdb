//! App schema declarations own local values and reuse canonical definitions.

use fold_db::access::AccessContext;
use fold_db::schema::types::{DeclarativeSchemaDefinition, Schema, SchemaType};
use fold_db::schema::ManifestRetention;
use serde::Deserialize;
use serde_json::Value;

use super::reply::{envelope, error_response, json_ok};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

mod owned;
mod retention;

use retention::{
    finish_declare_retention, precheck_declare_retention, prepare_declare_retention,
    validate_declare_retention,
};

static DECLARE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Debug)]
pub(crate) struct DirectDeclareProposal {
    pub(crate) namespace: String,
    pub(crate) local_schema_id: String,
    pub(crate) catalog_schema: DeclarativeSchemaDefinition,
    pub(crate) retention: Option<ManifestRetention>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SchemaSyncIntent {
    #[default]
    CatalogSync,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AppsDeclareRequest {
    app_id: String,
    schema: DeclarativeSchemaDefinition,
    #[serde(default)]
    intent: SchemaSyncIntent,
    #[serde(default)]
    retention: Option<ManifestRetention>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaDeclareRequest {
    namespace: String,
    schema: DeclarativeSchemaDefinition,
    #[serde(default)]
    intent: SchemaSyncIntent,
}

pub fn normalize_app_manifest_schema(
    app_id: &str,
    schema: DeclarativeSchemaDefinition,
) -> Result<DeclarativeSchemaDefinition, String> {
    normalize_proposal("app_id", app_id, schema).map(|p| p.catalog_schema)
}

fn normalize_proposal(
    namespace_label: &str,
    namespace: &str,
    mut schema: DeclarativeSchemaDefinition,
) -> Result<DirectDeclareProposal, String> {
    let schema_types::OwnedDeclaration {
        namespace,
        local_schema_id,
    } = schema.normalize_owned_declaration(namespace_label, namespace)?;
    schema.validate().map_err(|e| e.to_string())?;
    Ok(DirectDeclareProposal {
        namespace,
        local_schema_id,
        catalog_schema: schema,
        retention: None,
    })
}

pub(in crate::exec) fn logical_layout_error(
    ctx: &AccessContext,
    schema: &DeclarativeSchemaDefinition,
) -> Option<UdsResponse> {
    fold_db::schema::require_logical_layout(&schema.schema_type)
        .err()
        .map(|error| error_response(400, &error.to_string(), ctx))
}

pub(super) struct DirectDeclareResolution {
    pub(super) runtime_schema: String,
    pub(super) reused: bool,
}

fn declare_resolution_payload(
    declare: &DirectDeclareProposal,
    resolution: &DirectDeclareResolution,
) -> Result<Value, String> {
    let catalog_id = declare
        .catalog_schema
        .identity_hash
        .clone()
        .ok_or("declared schema has no catalog identity")?;
    let definition_id = schema_types::PortableSchemaId::from_catalog(
        &catalog_id,
        declare.catalog_schema.to_wire(),
    )?;
    Ok(serde_json::json!({
        "app_id": declare.namespace,
        "namespace": declare.namespace,
        "schema": declare.local_schema_id,
        "schema_name": declare.local_schema_id,
        "descriptive_name": declare.catalog_schema.descriptive_name,
        "canonical": resolution.runtime_schema,
        "identity_hash": declare.catalog_schema.identity_hash,
        "definition_id": definition_id.as_str(),
        "resolution": if resolution.reused { "reuse" } else { "register" },
    }))
}

pub async fn execute_apps_declare_schema_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let embedded_retention = serde_json::from_slice::<Value>(&req.body)
        .ok()
        .and_then(|raw| raw.pointer("/schema/retention").cloned())
        .is_some();
    let body: AppsDeclareRequest = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => {
            return error_response(
                400,
                &format!("invalid /api/apps/declare-schema body: {e}"),
                ctx,
            )
        }
    };
    let retention = match prepare_declare_retention(ctx, embedded_retention, body.retention) {
        Ok(r) => r,
        Err((status, e)) => return error_response(status, &e, ctx),
    };
    let mut declare = match normalize_proposal("app_id", &body.app_id, body.schema) {
        Ok(d) => d,
        Err(e) => return error_response(400, &e, ctx),
    };
    declare.retention = retention;
    dispatch_declare(ctx, host, declare, body.intent).await
}

pub(in crate::exec) async fn execute_declare_schema_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let body = match serde_json::from_slice::<SchemaDeclareRequest>(&req.body) {
        Ok(b) => b,
        Err(e) => {
            return error_response(400, &format!("invalid /api/schemas/declare body: {e}"), ctx)
        }
    };
    let declare = match normalize_proposal("namespace", &body.namespace, body.schema) {
        Ok(d) => d,
        Err(e) => return error_response(400, &e, ctx),
    };
    dispatch_declare(ctx, host, declare, body.intent).await
}

async fn dispatch_declare(
    ctx: &AccessContext,
    host: &Host,
    declare: DirectDeclareProposal,
    intent: SchemaSyncIntent,
) -> UdsResponse {
    let SchemaSyncIntent::CatalogSync = intent;
    if let Some(response) = logical_layout_error(ctx, &declare.catalog_schema) {
        return response;
    }
    if let Err(error) = validate_declare_retention(&declare) {
        return error_response(409, &error, ctx);
    }
    let _guard = DECLARE_LOCK.lock().await;
    let resolution = match owned::load(host, &declare).await {
        Ok(r) => r,
        Err(e) => return error_response(409, &e, ctx),
    };
    let mut payload = match declare_resolution_payload(&declare, &resolution) {
        Ok(p) => p,
        Err(e) => return error_response(409, &e, ctx),
    };
    payload["bind_eligible"] = serde_json::json!(true);
    match finish_declare_retention(host, &declare, &resolution).await {
        Ok(Some(r)) => payload["retention"] = r,
        Ok(None) => {}
        Err(e) => {
            return error_response(
                409,
                &format!("schema declared and bound, but its retention step failed: {e}"),
                ctx,
            );
        }
    }
    json_ok(&envelope(&payload, ctx.user_id.as_str()))
}
