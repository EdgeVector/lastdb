//! App schema declarations own local values and reuse canonical definitions.

use std::collections::BTreeSet;

use fold_db::access::AccessContext;
use fold_db::schema::types::{DeclarativeSchemaDefinition, Schema};
use fold_db::schema::ManifestRetention;
use schema_types::FieldValueType;
use serde::Deserialize;
use serde_json::Value;

use super::reply::{envelope, error_response, json_ok};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

mod retention;

use retention::{
    finish_declare_retention, precheck_declare_retention, prepare_declare_retention,
    validate_declare_retention,
};

static DECLARE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const BREAKING_CHANGE: &str = "this declaration changes an existing field, the key, or the alternate keys of an installed schema; declare a new schema name and copy the records explicitly";

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
        .as_deref()
        .ok_or("declared schema has no catalog identity")?;
    let definition_id =
        schema_types::PortableSchemaId::from_catalog(catalog_id, declare.catalog_schema.to_wire())?;
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
        "bind_eligible": true,
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
    let SchemaSyncIntent::CatalogSync = body.intent;
    dispatch_declare(ctx, host, declare).await
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
    let SchemaSyncIntent::CatalogSync = body.intent;
    dispatch_declare(ctx, host, declare).await
}

async fn dispatch_declare(
    ctx: &AccessContext,
    host: &Host,
    declare: DirectDeclareProposal,
) -> UdsResponse {
    if let Some(response) = logical_layout_error(ctx, &declare.catalog_schema) {
        return response;
    }
    if let Err(error) = validate_declare_retention(&declare) {
        return error_response(409, &error, ctx);
    }
    let _guard = DECLARE_LOCK.lock().await;
    let resolution = match load_owned(host, &declare).await {
        Ok(r) => r,
        Err(e) => return error_response(409, &e, ctx),
    };
    let mut payload = match declare_resolution_payload(&declare, &resolution) {
        Ok(p) => p,
        Err(e) => return error_response(409, &e, ctx),
    };
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

fn field_change(stored: &Schema, proposed: &Schema) -> Option<bool> {
    let stored_fields: BTreeSet<&str> =
        stored.fields.iter().flatten().map(String::as_str).collect();
    let proposed_fields: BTreeSet<&str> = proposed
        .fields
        .iter()
        .flatten()
        .map(String::as_str)
        .collect();
    let equal_defs = stored_fields.intersection(&proposed_fields).all(|f| {
        let (st, pt) = (stored.field_types.get(*f), proposed.field_types.get(*f));
        let retyped_any = matches!(st, None | Some(FieldValueType::Any))
            && pt.is_some_and(|kind| *kind != FieldValueType::Any);
        (st == pt || retyped_any)
            && stored.field_descriptions.get(*f) == proposed.field_descriptions.get(*f)
    });
    if !equal_defs {
        return None;
    }
    if stored_fields == proposed_fields {
        return (stored.identity_hash == proposed.identity_hash).then_some(true);
    }
    if stored_fields.is_subset(&proposed_fields) {
        return Some(true);
    }
    proposed_fields.is_subset(&stored_fields).then_some(false)
}

fn check_local(
    host: &Host,
    declare: &DirectDeclareProposal,
) -> Result<Option<(Schema, bool)>, String> {
    let proposed = &declare.catalog_schema;
    proposed.validate_owned_schema()?;
    let schemas = host
        .records()
        .schema_manager()
        .get_schemas()
        .map_err(|e| e.to_string())?;
    let mut installed = None;
    for stored in schemas.values() {
        let related = stored.name == declare.local_schema_id
            || stored.identity_hash == proposed.identity_hash
            || (stored.owner_app_id == proposed.owner_app_id
                && stored.descriptive_name == proposed.descriptive_name);
        if !related {
            continue;
        }
        if stored.owner_app_id != proposed.owner_app_id
            || stored.descriptive_name != proposed.descriptive_name
            || stored.schema_type != proposed.schema_type
            || stored.record_keys != proposed.record_keys
            || stored.key != proposed.key
            || stored.alternate_keys != proposed.alternate_keys
            || installed.is_some()
        {
            return Err(BREAKING_CHANGE.into());
        }
        let needs_load = field_change(stored, proposed).ok_or(BREAKING_CHANGE)?;
        installed = Some((stored.clone(), needs_load));
    }
    Ok(installed)
}

fn copy_missing<V: Clone>(
    dst: &mut std::collections::HashMap<String, V>,
    src: &std::collections::HashMap<String, V>,
    field: &str,
) {
    if let Some(val) = src.get(field) {
        dst.entry(field.to_string()).or_insert_with(|| val.clone());
    }
}

fn owned_schema(
    declare: &DirectDeclareProposal,
    installed: Option<&Schema>,
) -> Result<Schema, String> {
    let mut schema = declare.catalog_schema.clone();
    schema.field_hashes.clear();
    if let Some(installed) = installed {
        let fields = schema.fields.clone().unwrap_or_default();
        for field in fields {
            let unchanged = installed.field_types.get(&field) == schema.field_types.get(&field)
                && installed.field_descriptions.get(&field)
                    == schema.field_descriptions.get(&field)
                && schema
                    .field_versions
                    .get(&field)
                    .is_none_or(|v| installed.field_versions.get(&field) == Some(v));
            if unchanged {
                if let Some(hash) = installed.field_hashes.get(&field) {
                    schema.field_hashes.insert(field.clone(), hash.clone());
                }
                if let Some(version) = installed.field_versions.get(&field) {
                    schema.field_versions.insert(field.clone(), *version);
                }
                copy_missing(
                    &mut schema.field_declarations,
                    &installed.field_declarations,
                    &field,
                );
                copy_missing(
                    &mut schema.field_classifications,
                    &installed.field_classifications,
                    &field,
                );
                copy_missing(
                    &mut schema.field_interest_categories,
                    &installed.field_interest_categories,
                    &field,
                );
            }
        }
        schema.name.clone_from(&installed.name);
    }
    schema.ensure_field_hashes();
    schema.validate().map_err(|e| e.to_string())?;
    schema.validate_owned_schema()?;
    Ok(schema)
}

async fn load_owned(
    host: &Host,
    declare: &DirectDeclareProposal,
) -> Result<DirectDeclareResolution, String> {
    let installed = check_local(host, declare)?;
    if let Some((schema, false)) = &installed {
        precheck_declare_retention(host, declare, &schema.name).await?;
        return Ok(DirectDeclareResolution {
            runtime_schema: schema.name.clone(),
            reused: true,
        });
    }
    let installed_schema = installed.as_ref().map(|(s, _)| s);
    let runtime = installed_schema.map_or(declare.local_schema_id.as_str(), |s| s.name.as_str());
    precheck_declare_retention(host, declare, runtime).await?;
    let schema = owned_schema(declare, installed_schema)?;
    host.records()
        .schema_manager()
        .load_schema_internal(schema)
        .await
        .map_err(|e| e.to_string())?;
    Ok(DirectDeclareResolution {
        runtime_schema: runtime.to_string(),
        reused: installed_schema
            .is_some_and(|s| s.identity_hash == declare.catalog_schema.identity_hash),
    })
}
