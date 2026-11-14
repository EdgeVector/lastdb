//! App schema declarations and catalog synchronization.

mod retention;

use std::collections::HashMap;

use fold_db::access::AccessContext;
use fold_db::schema::types::DeclarativeSchemaDefinition;
use fold_db::schema::{ManifestRetention, ManifestRetentionPlan};
use serde::Deserialize;
use serde_json::{json, Value};

use self::retention::{
    check_retention, finish_retention, validate_manifest_retention, ManifestRetentionLayout,
};
use super::reply::{render, HostError};
use super::schema_routes::check_owned_local;
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

static DECLARE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct Proposal {
    namespace: String,
    local_schema_id: String,
    catalog_schema: DeclarativeSchemaDefinition,
    retention: Option<ManifestRetention>,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SchemaSyncIntent {
    #[default]
    CatalogSync,
}

pub fn normalize_app_manifest_schema(
    app_id: &str,
    schema: DeclarativeSchemaDefinition,
) -> Result<DeclarativeSchemaDefinition, String> {
    normalize_proposal("app_id", app_id, schema, None).map(|p| p.catalog_schema)
}

fn normalize_proposal(
    label: &str,
    namespace: &str,
    mut schema: DeclarativeSchemaDefinition,
    retention: Option<ManifestRetention>,
) -> Result<Proposal, String> {
    let owned = schema.normalize_owned_declaration(label, namespace)?;
    schema.validate().map_err(|e| e.to_string())?;
    Ok(Proposal {
        namespace: owned.namespace,
        local_schema_id: owned.local_schema_id,
        catalog_schema: schema,
        retention,
    })
}

pub async fn execute_apps_declare_schema_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Req {
        app_id: String,
        schema: DeclarativeSchemaDefinition,
        #[serde(default)]
        intent: SchemaSyncIntent,
        #[serde(default)]
        retention: Option<ManifestRetention>,
    }
    let res = async {
        if serde_json::from_slice::<Value>(&req.body)
            .ok()
            .is_some_and(|r| r.pointer("/schema/retention").is_some())
        {
            return Err(HostError::new(
                400,
                "retention must sit beside `schema` in the declare body, not inside it",
            ));
        }
        let b: Req = serde_json::from_slice(&req.body).map_err(|e| {
            HostError::new(400, format!("invalid /api/apps/declare-schema body: {e}"))
        })?;
        if let Some(ret) = b.retention {
            if !ctx.is_owner {
                return Err(HostError::new(
                    403,
                    "a schema retention can be declared only on the owner socket",
                ));
            }
            ret.ttl().map_err(|e| HostError::new(400, e))?;
        }
        let SchemaSyncIntent::CatalogSync = b.intent;
        let d = normalize_proposal("app_id", &b.app_id, b.schema, b.retention)
            .map_err(|e| HostError::new(400, e))?;
        dispatch_declare(host, &d).await
    }
    .await;
    render(res, ctx)
}

pub(in crate::exec) async fn declare_schema(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Req {
        namespace: String,
        schema: DeclarativeSchemaDefinition,
        #[serde(default)]
        intent: SchemaSyncIntent,
    }
    let res = async {
        let b: Req = serde_json::from_slice(&req.body)
            .map_err(|e| HostError::new(400, format!("invalid /api/schemas/declare body: {e}")))?;
        let SchemaSyncIntent::CatalogSync = b.intent;
        let d = normalize_proposal("namespace", &b.namespace, b.schema, None)
            .map_err(|e| HostError::new(400, e))?;
        dispatch_declare(host, &d).await
    }
    .await;
    render(res, ctx)
}

async fn dispatch_declare(host: &Host, d: &Proposal) -> Result<Value, HostError> {
    fold_db::schema::require_logical_layout(&d.catalog_schema.schema_type)
        .map_err(|e| HostError::new(400, e.to_string()))?;
    let conflict = |e| HostError::new(409, e);
    let layout = validate_manifest_retention(d.retention, &d.catalog_schema).map_err(conflict)?;
    let _guard = DECLARE_LOCK.lock().await;
    let (runtime, reused, checked) = load_owned(host, d, layout.as_ref())
        .await
        .map_err(conflict)?;
    let cat_id = d
        .catalog_schema
        .identity_hash
        .as_deref()
        .ok_or_else(|| HostError::new(409, "declared schema has no catalog identity"))?;
    let def_id = schema_types::PortableSchemaId::from_catalog(cat_id, d.catalog_schema.to_wire())
        .map_err(conflict)?;
    let mut payload = json!({
        "app_id": d.namespace, "namespace": d.namespace,
        "schema": d.local_schema_id, "schema_name": d.local_schema_id,
        "descriptive_name": d.catalog_schema.descriptive_name,
        "canonical": runtime, "identity_hash": d.catalog_schema.identity_hash,
        "definition_id": def_id.as_str(),
        "resolution": if reused { "reuse" } else { "register" }, "bind_eligible": true,
    });
    if let Some(r) = finish_retention(host, d, &runtime, layout, checked.as_ref())
        .await
        .map_err(|e| {
            HostError::new(
                409,
                format!("schema declared and bound, but its retention step failed: {e}"),
            )
        })?
    {
        payload["retention"] = r;
    }
    Ok(payload)
}

fn copy_missing<V: Clone>(dst: &mut HashMap<String, V>, src: &HashMap<String, V>, f: &str) {
    if let Some(v) = src.get(f) {
        dst.entry(f.to_string()).or_insert_with(|| v.clone());
    }
}

async fn load_owned(
    host: &Host,
    d: &Proposal,
    layout: Option<&ManifestRetentionLayout>,
) -> Result<(String, bool, Option<(ManifestRetentionPlan, bool)>), String> {
    let installed = check_owned_local(host, &d.local_schema_id, &d.catalog_schema)?;
    let inst = installed.as_ref().map(|(s, _)| s);
    let runtime = inst.map_or(d.local_schema_id.as_str(), |s| s.name.as_str());
    let checked = match layout {
        Some(lay) => {
            Some(check_retention(host, d, runtime, lay.ttl_seconds, lay.range_keyed).await?)
        }
        None => None,
    };
    if let Some((schema, false)) = &installed {
        return Ok((schema.name.clone(), true, checked));
    }
    let mut s = d.catalog_schema.clone();
    s.field_hashes.clear();
    if let Some(i) = inst {
        for f in s.fields.clone().unwrap_or_default() {
            if i.field_types.get(&f) == s.field_types.get(&f)
                && i.field_descriptions.get(&f) == s.field_descriptions.get(&f)
                && s.field_versions
                    .get(&f)
                    .is_none_or(|v| i.field_versions.get(&f) == Some(v))
            {
                copy_missing(&mut s.field_versions, &i.field_versions, &f);
                copy_missing(&mut s.field_hashes, &i.field_hashes, &f);
                copy_missing(&mut s.field_declarations, &i.field_declarations, &f);
                copy_missing(&mut s.field_classifications, &i.field_classifications, &f);
                copy_missing(
                    &mut s.field_interest_categories,
                    &i.field_interest_categories,
                    &f,
                );
            }
        }
        s.name.clone_from(&i.name);
    }
    s.ensure_field_hashes();
    s.validate().map_err(|e| e.to_string())?;
    s.validate_owned_schema()?;
    host.records()
        .schema_manager()
        .load_schema_internal(s)
        .await
        .map_err(|e| e.to_string())?;
    Ok((
        runtime.to_string(),
        inst.is_some_and(|i| i.identity_hash == d.catalog_schema.identity_hash),
        checked,
    ))
}
