//! Schema catalog listing, lookup, alias resolution, compatibility check, seed, and drop routes.

use std::collections::{BTreeSet, HashSet};

use fold_db::access::AccessContext;
use fold_db::schema::types::{FieldValueType, Schema};
use fold_db::schema::{SchemaState, SchemaWithState};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::reply::{content_free, envelope, error_response, json_ok, render, HostError};
use super::wire::{query_flag, schema_name_from_target};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

const BREAKING_CHANGE: &str = "this declaration changes an existing field, the key, or the alternate keys of an installed schema; declare a new schema name and copy the records explicitly";

fn annotate<T: Serialize>(schemas: impl IntoIterator<Item = T>) -> Option<Vec<Value>> {
    schemas
        .into_iter()
        .map(|s| {
            let mut row = serde_json::to_value(s).ok()?;
            let obj = row.as_object_mut()?;
            obj.insert("record_count".into(), Value::Null);
            obj.insert("has_data".into(), Value::Null);
            Some(row)
        })
        .collect()
}

pub(in crate::exec) fn list_schemas(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if query_flag(&req.target, "include_counts") {
        return error_response(400, "schema listing does not count records", ctx);
    }
    let full = query_flag(&req.target, "include_full");
    let mgr = host.records().schema_manager();
    let rows = if full {
        mgr.get_schemas_with_states()
            .ok()
            .and_then(|s| annotate(s.into_iter().filter(|e| e.state != SchemaState::Blocked)))
    } else {
        mgr.get_active_schema_list_entries_with_states()
            .ok()
            .and_then(annotate)
    };
    let Some(rows) = rows else {
        return content_free(500, "Internal Server Error");
    };
    let newer = mgr.schemas_with_newer_identity_algo().unwrap_or_default();
    json_ok(&envelope(
        &json!({
            "schemas": rows, "count": rows.len(), "ready": true,
            "counts_included": false, "full_included": full,
            "identity_algo_version": schema_types::IDENTITY_HASH_ALGO_VERSION,
            "newer_identity_algo_count": newer.len(), "newer_identity_algo_schemas": newer,
        }),
        ctx.user_id.as_str(),
    ))
}

pub(in crate::exec) fn get_schema(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let Some(name) = schema_name_from_target(&req.target) else {
        return content_free(400, "Bad Request");
    };
    let mgr = host.records().schema_manager();
    render(
        resolve_schema_name(host, &name).and_then(|opt| {
            let nf = || HostError::new(404, format!("Schema not found: {name}"));
            let c = opt.ok_or_else(nf)?;
            let s = mgr
                .get_schema_metadata(&c)
                .map_err(|e| HostError::internal(e.to_string()))?
                .ok_or_else(nf)?;
            let st = mgr
                .get_schema_states()
                .ok()
                .and_then(|m| m.get(&c).copied())
                .unwrap_or_default();
            Ok(json!({ "schema": SchemaWithState::new(s, st) }))
        }),
        ctx,
    )
}

pub(in crate::exec) async fn drop_schema(
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
    let res = async {
        let b: Body = serde_json::from_slice(&req.body)
            .map_err(|_| HostError::new(400, "invalid /api/schemas/drop body"))?;
        let (schema, app) = (b.schema.trim(), b.owner_app.trim());
        if schema.is_empty() == app.is_empty() {
            return Err(HostError::new(
                400,
                "pass exactly one of schema or owner_app",
            ));
        }
        let mgr = host.records().schema_manager();
        let drop_err = |e| HostError::internal(format!("schema drop failed: {e}"));
        if !app.is_empty() {
            let names = mgr
                .schema_names_owned_by(app)
                .map_err(|_| HostError::internal("schema drop failed"))?;
            let mut dropped = Vec::new();
            for n in names {
                if mgr.drop_schema(&n).await.map_err(drop_err)? {
                    dropped.push(n);
                }
            }
            dropped.sort();
            if b.must_exist && dropped.is_empty() {
                return Err(HostError::new(
                    404,
                    format!("no installed schema is owned by '{app}'"),
                ));
            }
            return Ok(
                json!({ "dropped": dropped, "existed": !dropped.is_empty(), "owner_app": app }),
            );
        }
        let target = resolve_schema_name(host, schema)
            .or_else(|e| blocked_drop_target(host, schema, &e).map(Some).ok_or(e))?;
        let (canonical, existed) = match target {
            Some(c) => {
                let ex = mgr.drop_schema(&c).await.map_err(drop_err)?;
                (c, ex)
            }
            None => (schema.to_string(), false),
        };
        if b.must_exist && !existed {
            return Err(HostError::new(
                404,
                format!("no schema is installed under '{canonical}'"),
            ));
        }
        let dropped: Vec<String> = existed.then(|| canonical.clone()).into_iter().collect();
        Ok(json!({ "dropped": dropped, "existed": existed, "schema": canonical }))
    }
    .await;
    render(res, ctx)
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
    let has = |n: &str| {
        mgr.get_schema_metadata(n)
            .map(|s| s.is_some())
            .map_err(|e| HostError::internal(e.to_string()))
    };
    if has(name)? {
        return Ok(Some(name.to_string()));
    }
    let schemas = mgr
        .get_schemas_with_states()
        .map_err(|e| HostError::internal(e.to_string()))?;
    if let Some(c) = resolve_exact_alias(name, &schemas)? {
        return Ok(Some(c));
    }
    let trimmed = name.trim();
    if trimmed != name && !trimmed.is_empty() && has(trimmed)? {
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
    let split = req
        .split_once('/')
        .filter(|(o, l)| !o.is_empty() && !l.is_empty());
    for kind in [0u8, 1, 2] {
        if kind == 1 && split.is_none() {
            continue;
        }
        let iter = schemas.iter().filter(|e| match kind {
            0 => e.schema.identity_hash.as_deref() == Some(req),
            1 => split.is_some_and(|(o, l)| {
                e.schema.owner_app_id.as_deref() == Some(o)
                    && (e.schema.name == l || e.schema.descriptive_name.as_deref() == Some(l))
            }),
            _ => e.schema.descriptive_name.as_deref() == Some(req),
        });
        if let Some(c) = resolve_matches(req, iter)? {
            return Ok(Some(c));
        }
    }
    Ok(None)
}

fn resolve_matches<'a>(
    req: &str,
    matches: impl Iterator<Item = &'a SchemaWithState>,
) -> Result<Option<String>, HostError> {
    let (mut blocked, mut seen, mut cands) = (false, HashSet::new(), Vec::<&Schema>::new());
    for entry in matches {
        if entry.state == SchemaState::Blocked {
            blocked = true;
        } else if seen.insert(entry.schema.name.as_str()) {
            cands.push(&entry.schema);
        }
    }
    let names: HashSet<&str> = cands.iter().map(|s| s.name.as_str()).collect();
    cands.retain(|s| {
        s.name
            .split_once(':')
            .is_none_or(|(_, b)| !names.contains(b))
    });
    match cands.len() {
        0 if blocked => Err(HostError::new(
            400,
            format!("Schema '{req}' is blocked and cannot be queried or mutated"),
        )),
        0 => Ok(None),
        1 => Ok(Some(cands[0].name.clone())),
        _ => {
            cands.sort_by(|a, b| a.name.cmp(&b.name));
            let details = cands
                .iter()
                .map(|s| {
                    format!(
                        "canonical={} descriptive_name={} owner_app_id={}",
                        s.name,
                        s.descriptive_name.as_deref().unwrap_or("<none>"),
                        s.owner_app_id.as_deref().unwrap_or("<none>"),
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

pub(super) fn check_owned_local(
    host: &Host,
    local_id: &str,
    p: &Schema,
) -> Result<Option<(Schema, bool)>, String> {
    p.validate_owned_schema()?;
    fn fset(s: &Schema) -> BTreeSet<&str> {
        s.fields.iter().flatten().map(String::as_str).collect()
    }
    let schemas = host
        .records()
        .schema_manager()
        .get_schemas()
        .map_err(|e| e.to_string())?;
    let mut inst = None;
    for s in schemas.values() {
        if !(s.name == local_id
            || s.identity_hash == p.identity_hash
            || (s.owner_app_id == p.owner_app_id && s.descriptive_name == p.descriptive_name))
        {
            continue;
        }
        if s.owner_app_id != p.owner_app_id
            || s.descriptive_name != p.descriptive_name
            || s.schema_type != p.schema_type
            || s.record_keys != p.record_keys
            || s.key != p.key
            || s.alternate_keys != p.alternate_keys
            || inst.is_some()
        {
            return Err(BREAKING_CHANGE.into());
        }
        let (sf, pf) = (fset(s), fset(p));
        let ok = sf.intersection(&pf).all(|f| {
            let (st, pt) = (s.field_types.get(*f), p.field_types.get(*f));
            let retyped = matches!(st, None | Some(FieldValueType::Any))
                && pt.is_some_and(|k| *k != FieldValueType::Any);
            (st == pt || retyped) && s.field_descriptions.get(*f) == p.field_descriptions.get(*f)
        });
        let changed = ok
            .then(|| {
                if sf == pf {
                    (s.identity_hash == p.identity_hash).then_some(true)
                } else {
                    sf.is_subset(&pf)
                        .then_some(true)
                        .or_else(|| pf.is_subset(&sf).then_some(false))
                }
            })
            .flatten()
            .ok_or(BREAKING_CHANGE)?;
        inst = Some((s.clone(), changed));
    }
    Ok(inst)
}
