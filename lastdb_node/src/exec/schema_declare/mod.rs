//! App schema declarations and catalog synchronization.

mod retention;

use std::collections::{BTreeSet, HashMap};

use fold_db::access::AccessContext;
use fold_db::schema::types::{DeclarativeSchemaDefinition, FieldValueType, Schema};
use fold_db::schema::{ManifestRetention, ManifestRetentionPlan};
use serde::Deserialize;
use serde_json::{json, Value};

use super::reply::{render, HostError};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};
use retention::{check_retention, finish_retention, validate_retention, RetentionLayout};

static DECLARE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const BREAKING_CHANGE: &str = "this declaration changes an existing field, the key, or the alternate keys of an installed schema; declare a new schema name and copy the records explicitly";

pub(super) struct Proposal {
    pub(super) namespace: String,
    pub(super) local_schema_id: String,
    pub(super) catalog_schema: DeclarativeSchemaDefinition,
    pub(super) retention: Option<ManifestRetention>,
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
    normalize_proposal("app_id", app_id, schema).map(|p| p.catalog_schema)
}

fn normalize_proposal(
    label: &str,
    namespace: &str,
    mut schema: DeclarativeSchemaDefinition,
) -> Result<Proposal, String> {
    let owned = schema.normalize_owned_declaration(label, namespace)?;
    schema.validate().map_err(|e| e.to_string())?;
    Ok(Proposal {
        namespace: owned.namespace,
        local_schema_id: owned.local_schema_id,
        catalog_schema: schema,
        retention: None,
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
            .is_some_and(|raw| raw.pointer("/schema/retention").is_some())
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
        let mut d = normalize_proposal("app_id", &b.app_id, b.schema)
            .map_err(|e| HostError::new(400, e))?;
        d.retention = b.retention;
        let SchemaSyncIntent::CatalogSync = b.intent;
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
        let d = normalize_proposal("namespace", &b.namespace, b.schema)
            .map_err(|e| HostError::new(400, e))?;
        let SchemaSyncIntent::CatalogSync = b.intent;
        dispatch_declare(host, &d).await
    }
    .await;
    render(res, ctx)
}

async fn dispatch_declare(host: &Host, declare: &Proposal) -> Result<Value, HostError> {
    fold_db::schema::require_logical_layout(&declare.catalog_schema.schema_type)
        .map_err(|e| HostError::new(400, e.to_string()))?;
    let conflict = |e| HostError::new(409, e);
    let layout = validate_retention(declare).map_err(conflict)?;
    let _guard = DECLARE_LOCK.lock().await;
    let (runtime_schema, reused, checked) = load_owned(host, declare, layout.as_ref())
        .await
        .map_err(conflict)?;
    let cat_id = declare
        .catalog_schema
        .identity_hash
        .as_deref()
        .ok_or_else(|| HostError::new(409, "declared schema has no catalog identity"))?;
    let def_id =
        schema_types::PortableSchemaId::from_catalog(cat_id, declare.catalog_schema.to_wire())
            .map_err(conflict)?;
    let mut payload = json!({
        "app_id": declare.namespace, "namespace": declare.namespace,
        "schema": declare.local_schema_id, "schema_name": declare.local_schema_id,
        "descriptive_name": declare.catalog_schema.descriptive_name,
        "canonical": runtime_schema, "identity_hash": declare.catalog_schema.identity_hash,
        "definition_id": def_id.as_str(),
        "resolution": if reused { "reuse" } else { "register" }, "bind_eligible": true,
    });
    if let Some(r) = finish_retention(host, declare, &runtime_schema, layout, checked.as_ref())
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

fn field_set(s: &Schema) -> BTreeSet<&str> {
    s.fields.iter().flatten().map(String::as_str).collect()
}

fn field_change(stored: &Schema, proposed: &Schema) -> Option<bool> {
    let (sf, pf) = (field_set(stored), field_set(proposed));
    let equal_defs = sf.intersection(&pf).all(|f| {
        let (st, pt) = (stored.field_types.get(*f), proposed.field_types.get(*f));
        let retyped_any = matches!(st, None | Some(FieldValueType::Any))
            && pt.is_some_and(|k| *k != FieldValueType::Any);
        (st == pt || retyped_any)
            && stored.field_descriptions.get(*f) == proposed.field_descriptions.get(*f)
    });
    if !equal_defs {
        return None;
    }
    if sf == pf {
        return (stored.identity_hash == proposed.identity_hash).then_some(true);
    }
    sf.is_subset(&pf)
        .then_some(true)
        .or_else(|| pf.is_subset(&sf).then_some(false))
}

fn check_local(host: &Host, declare: &Proposal) -> Result<Option<(Schema, bool)>, String> {
    let p = &declare.catalog_schema;
    p.validate_owned_schema()?;
    let schemas = host
        .records()
        .schema_manager()
        .get_schemas()
        .map_err(|e| e.to_string())?;
    let mut installed = None;
    for s in schemas.values() {
        let related = s.name == declare.local_schema_id
            || s.identity_hash == p.identity_hash
            || (s.owner_app_id == p.owner_app_id && s.descriptive_name == p.descriptive_name);
        if !related {
            continue;
        }
        if s.owner_app_id != p.owner_app_id
            || s.descriptive_name != p.descriptive_name
            || s.schema_type != p.schema_type
            || s.record_keys != p.record_keys
            || s.key != p.key
            || s.alternate_keys != p.alternate_keys
            || installed.is_some()
        {
            return Err(BREAKING_CHANGE.into());
        }
        installed = Some((s.clone(), field_change(s, p).ok_or(BREAKING_CHANGE)?));
    }
    Ok(installed)
}

fn copy_missing<V: Clone>(dst: &mut HashMap<String, V>, src: &HashMap<String, V>, field: &str) {
    if let Some(val) = src.get(field) {
        dst.entry(field.to_string()).or_insert_with(|| val.clone());
    }
}

fn owned_schema(declare: &Proposal, installed: Option<&Schema>) -> Result<Schema, String> {
    let mut s = declare.catalog_schema.clone();
    s.field_hashes.clear();
    if let Some(inst) = installed {
        for f in s.fields.clone().unwrap_or_default() {
            if inst.field_types.get(&f) == s.field_types.get(&f)
                && inst.field_descriptions.get(&f) == s.field_descriptions.get(&f)
                && s.field_versions
                    .get(&f)
                    .is_none_or(|v| inst.field_versions.get(&f) == Some(v))
            {
                copy_missing(&mut s.field_versions, &inst.field_versions, &f);
                copy_missing(&mut s.field_hashes, &inst.field_hashes, &f);
                copy_missing(&mut s.field_declarations, &inst.field_declarations, &f);
                copy_missing(
                    &mut s.field_classifications,
                    &inst.field_classifications,
                    &f,
                );
                copy_missing(
                    &mut s.field_interest_categories,
                    &inst.field_interest_categories,
                    &f,
                );
            }
        }
        s.name.clone_from(&inst.name);
    }
    s.ensure_field_hashes();
    s.validate().map_err(|e| e.to_string())?;
    s.validate_owned_schema()?;
    Ok(s)
}

async fn load_owned(
    host: &Host,
    declare: &Proposal,
    layout: Option<&(u64, RetentionLayout)>,
) -> Result<(String, bool, Option<(ManifestRetentionPlan, bool)>), String> {
    let installed = check_local(host, declare)?;
    let inst = installed.as_ref().map(|(s, _)| s);
    let runtime = inst.map_or(declare.local_schema_id.as_str(), |s| s.name.as_str());
    let checked = if let Some((ttl, lay)) = layout {
        Some(check_retention(host, declare, runtime, *ttl, lay).await?)
    } else {
        None
    };
    if let Some((schema, false)) = &installed {
        return Ok((schema.name.clone(), true, checked));
    }
    let schema = owned_schema(declare, inst)?;
    host.records()
        .schema_manager()
        .load_schema_internal(schema)
        .await
        .map_err(|e| e.to_string())?;
    Ok((
        runtime.to_string(),
        inst.is_some_and(|s| s.identity_hash == declare.catalog_schema.identity_hash),
        checked,
    ))
}
