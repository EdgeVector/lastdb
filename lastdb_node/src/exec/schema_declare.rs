//! App schema declarations, catalog synchronization, and manifest retention.

use std::collections::HashMap;

use fold_db::access::AccessContext;
use fold_db::schema::types::DeclarativeSchemaDefinition;
use fold_db::schema::{
    ManifestRetention, ManifestRetentionPlan, ManifestRetentionStatus, RetentionSource,
    SchemaError, SchemaRetentionPolicy,
};
use serde::Deserialize;
use serde_json::{json, Value};

use super::reply::{render, HostError};
use super::schema_routes::check_owned_local;
use super::wire::{validate_manifest_retention, RetLayout};
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
    layout: Option<&RetLayout>,
) -> Result<(String, bool, Option<(ManifestRetentionPlan, bool)>), String> {
    let installed = check_owned_local(host, &d.local_schema_id, &d.catalog_schema)?;
    let inst = installed.as_ref().map(|(s, _)| s);
    let runtime = inst.map_or(d.local_schema_id.as_str(), |s| s.name.as_str());
    let checked = match layout {
        Some(lay) => Some(check_retention(host, d, runtime, lay.0, lay.3).await?),
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

fn existing_warning(binding: &str, ttl: u64) -> String {
    format!(
        "existing_records_may_expire: {binding} already holds records. Every record whose range key is older than {ttl}s is deleted for good: at the next sweep for a Range schema, and in every HashRange partition that receives a later write"
    )
}

async fn check_retention(
    host: &Host,
    d: &Proposal,
    anchor: &str,
    ttl: u64,
    range_keyed: bool,
) -> Result<(ManifestRetentionPlan, bool), String> {
    let binding = d.local_schema_id.as_str();
    let meta = host
        .records()
        .schema_manager()
        .get_schema_metadata(anchor)
        .map_err(|e| format!("failed to read schema {anchor}: {e}"))?;
    if let Some(s) = meta
        .as_ref()
        .filter(|s| s.owner_app_id.as_deref() != Some(d.namespace.as_str()))
    {
        return Err(format!(
            "manifest retention refused on {binding}: schema is owned by {}, not {}",
            s.owner_app_id.as_deref().unwrap_or("unowned"),
            d.namespace
        ));
    }
    let plan = host
        .records()
        .db_ops()
        .schemas()
        .plan_manifest_retention(anchor, ttl)
        .await
        .map_err(|e| e.to_string())?;
    let may_expire = match meta.as_ref() {
        Some(s) if range_keyed && plan.status == ManifestRetentionStatus::Applied => host
            .records()
            .db_ops()
            .records()
            .schema_may_hold_records(s)
            .await
            .map_err(|e| format!("failed to probe the records of {anchor}: {e}"))?,
        _ => false,
    };
    if !d
        .retention
        .as_ref()
        .is_some_and(ManifestRetention::confirmed)
        && (plan.shortened || may_expire)
    {
        let mut why = Vec::new();
        if may_expire {
            why.push(existing_warning(binding, ttl));
        }
        if plan.shortened {
            why.push(format!("the manifest TTL would be shortened to {ttl}s, so records between the two TTLs expire"));
        }
        return Err(format!(
            "a manifest TTL on {binding} needs confirmation. {}. Review what would expire, then run `lastdb app check --sync --confirm-expire-existing`",
            why.join(". ")
        ));
    }
    Ok((plan, may_expire))
}

async fn finish_retention(
    host: &Host,
    d: &Proposal,
    anchor: &str,
    layout: Option<RetLayout>,
    checked: Option<&(ManifestRetentionPlan, bool)>,
) -> Result<Option<Value>, String> {
    let records = host.records();
    let schemas = records.db_ops().schemas();
    let Some(((ttl, age_rule, mut warnings, _), &(ref plan, may_expire))) = layout.zip(checked)
    else {
        let policy = match schemas.get_schema_retention_policy(anchor).await {
            Ok(p) => p,
            Err(SchemaError::NotFound(_)) => None,
            Err(e) => return Err(format!("reading the retention policy of {anchor}: {e}")),
        };
        let Some(m_ttl) = policy
            .as_ref()
            .and_then(SchemaRetentionPolicy::manifest_ttl_seconds)
        else {
            return Ok(None);
        };
        let warn = format!(
            "this manifest no longer declares a retention for {}, but a manifest TTL of {m_ttl}s is still running. Removing the key does not clear the policy. Run `lastdb admin retention clear --schema {anchor}` to remove it",
            d.local_schema_id
        );
        return Ok(Some(json!({
            "declared": false, "status": "manifest_policy_still_running",
            "ttl_seconds": m_ttl, "anchor": anchor, "notes": [], "warnings": [warn], "blocked": null,
        })));
    };
    let (confirmed, binding) = (
        d.retention
            .as_ref()
            .is_some_and(ManifestRetention::confirmed),
        d.local_schema_id.as_str(),
    );
    let out = schemas
        .apply_manifest_retention(anchor, ttl, &d.namespace, confirmed)
        .await
        .map_err(|e| e.to_string())?;
    tracing::info!(
        target: "lastdb_node::apps_declare_schema",
        schema = %binding, anchor = %out.anchor, ttl_seconds = ttl, status = ?out.status,
        "applied the manifest retention"
    );
    let prev_ttl = plan
        .current
        .as_ref()
        .and_then(SchemaRetentionPolicy::manifest_ttl_seconds);
    let change = match (plan.status, plan.current.as_ref()) {
        (ManifestRetentionStatus::Unchanged, _) => "unchanged",
        (_, Some(c)) if c.source == RetentionSource::Manifest => {
            if ttl < c.ttl_seconds {
                "shortened"
            } else {
                "lengthened"
            }
        }
        _ => "new",
    };
    if may_expire {
        warnings.push(existing_warning(binding, ttl));
    }
    let notes = match out.status {
        ManifestRetentionStatus::Applied => vec![format!("a new manifest TTL of {ttl}s is in force")],
        ManifestRetentionStatus::Updated => vec![format!("the manifest TTL changed from {}s to {ttl}s ({change})", prev_ttl.unwrap_or(0))],
        ManifestRetentionStatus::Unchanged => Vec::new(),
        ManifestRetentionStatus::OperatorOverride => vec![format!(
            "an operator policy of {}s is in force and wins; the manifest TTL of {ttl}s is kept beside it. Clearing the operator policy puts the manifest TTL in force: when it is shorter, that needs `--confirm-expire-existing`",
            out.effective.ttl_seconds
        )],
    };
    Ok(Some(json!({
        "ttl_seconds": ttl, "source": RetentionSource::Manifest.as_str(),
        "anchor": anchor, "change": change, "previous_ttl_seconds": prev_ttl,
        "age_rule": age_rule, "warnings": warnings,
        "existing_records_may_expire": may_expire, "shortened": plan.shortened,
        "confirmed_expire_existing": confirmed, "status": out.status,
        "effective": out.effective, "notes": notes,
    })))
}
