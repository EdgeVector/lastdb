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
mod retention_assess;

use retention::{
    finish_declare_retention, precheck_declare_retention, prepare_declare_retention,
    preview_declare_retention, validate_declare_retention,
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
    Check,
}

fn body_has_embedded_retention(body: &[u8]) -> bool {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|raw| raw.get("schema")?.get("retention").cloned())
        .is_some()
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

/// Normalize one `lastdb-app.json` schema entry identically to the declare route.
pub fn normalize_app_manifest_schema(
    app_id: &str,
    schema: DeclarativeSchemaDefinition,
) -> Result<DeclarativeSchemaDefinition, String> {
    normalize_direct_declare_proposal("app_id", app_id, schema)
        .map(|proposal| proposal.catalog_schema)
}

fn normalize_direct_declare_proposal(
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

fn sync_admission_error(
    ctx: &AccessContext,
    declare: &DirectDeclareProposal,
) -> Option<UdsResponse> {
    logical_layout_error(ctx, &declare.catalog_schema).or_else(|| {
        validate_declare_retention(declare)
            .err()
            .map(|error| error_response(409, &error, ctx))
    })
}

pub(super) struct DirectDeclareResolution {
    pub(super) runtime_schema: String,
    pub(super) reused: bool,
}

impl DirectDeclareResolution {
    fn outcome(&self) -> &'static str {
        if self.reused {
            "reuse"
        } else {
            "register"
        }
    }
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
    let definition = schema_types::PortableSchemaDefinition::from_catalog(
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
        "definition_id": definition.id().as_str(),
        "resolution": resolution.outcome(),
    }))
}

/// `POST /api/apps/declare-schema` — obtain canonical definitions and load one owned schema.
pub async fn execute_apps_declare_schema_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let embedded_retention = body_has_embedded_retention(&req.body);
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
        Ok(retention) => retention,
        Err((status, e)) => return error_response(status, &e, ctx),
    };
    let mut declare = match normalize_direct_declare_proposal("app_id", &body.app_id, body.schema) {
        Ok(declare) => declare,
        Err(e) => return error_response(400, &e, ctx),
    };
    declare.retention = retention;
    match body.intent {
        SchemaSyncIntent::CatalogSync => execute_catalog_schema_sync(ctx, host, declare).await,
        SchemaSyncIntent::Check => execute_catalog_schema_check(ctx, host, declare).await,
    }
}

pub(in crate::exec) async fn execute_declare_schema_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let body = match serde_json::from_slice::<SchemaDeclareRequest>(&req.body) {
        Ok(body) => body,
        Err(e) => {
            return error_response(400, &format!("invalid /api/schemas/declare body: {e}"), ctx)
        }
    };
    let declare = match normalize_direct_declare_proposal("namespace", &body.namespace, body.schema)
    {
        Ok(declare) => declare,
        Err(e) => return error_response(400, &e, ctx),
    };
    match body.intent {
        SchemaSyncIntent::CatalogSync => execute_catalog_schema_sync(ctx, host, declare).await,
        SchemaSyncIntent::Check => execute_catalog_schema_check(ctx, host, declare).await,
    }
}

async fn execute_catalog_schema_sync(
    ctx: &AccessContext,
    host: &Host,
    declare: DirectDeclareProposal,
) -> UdsResponse {
    if let Some(response) = sync_admission_error(ctx, &declare) {
        return response;
    }

    let _declare_guard = DECLARE_LOCK.lock().await;

    let resolution = match owned::load(host, &declare).await {
        Ok(resolution) => resolution,
        Err(e) => return error_response(409, &e, ctx),
    };

    let mut payload = match declare_resolution_payload(&declare, &resolution) {
        Ok(payload) => payload,
        Err(error) => return error_response(409, &error, ctx),
    };
    payload["bind_eligible"] = serde_json::json!(true);

    match finish_declare_retention(host, &declare, &resolution).await {
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

async fn execute_catalog_schema_check(
    ctx: &AccessContext,
    host: &Host,
    declare: DirectDeclareProposal,
) -> UdsResponse {
    if let Some(response) = logical_layout_error(ctx, &declare.catalog_schema) {
        return response;
    }
    let plan = match owned::plan(host, &declare).await {
        Ok(plan) => plan,
        Err(e) => return error_response(409, &e, ctx),
    };
    let mut payload = match declare_resolution_payload(&declare, &plan.resolution) {
        Ok(payload) => payload,
        Err(error) => return error_response(409, &error, ctx),
    };
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
