//! Schema checks, signed-index resolution, and manifest publishing rules.

use super::*;
use fold_db::schema::types::DeclarativeSchemaDefinition;
use lastdb_node::app_publish::{self, AppManifest, CheckMode, CheckOutcome, SchemaCoverage};
use lastdb_node::exec::normalize_app_manifest_schema;
use serde_json::Value;

const NODE_SCHEMAS_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) struct ListedSchema {
    name: String,
    descriptive_name: Option<String>,
    identity_hash: String,
}

struct HeldSchema {
    name: String,
    descriptive_name: Option<String>,
    identity_hash: Option<String>,
}

pub(crate) fn app_check(
    home: &Path,
    manifest: &AppManifest,
    mode: CheckMode,
    confirm_expire_existing: bool,
) -> Result<Vec<CheckOutcome>, String> {
    let socket = lastdb_node::lastdb_uds::uds::socket_path(&home.join("data"));
    if probe_health(&socket).is_err() {
        return Err(format!(
            "lastdbd not reachable at {} — schema checks run against your local Mini; start the daemon first",
            socket.display()
        ));
    }
    app_publish::check_schemas_with(&socket, manifest, mode, confirm_expire_existing)
}

fn check_outcome_rows(outcomes: &[CheckOutcome]) -> Vec<Value> {
    outcomes
        .iter()
        .map(|o| {
            let (resolution, detail) = match &o.coverage {
                SchemaCoverage::Reuse { identity_hash } => ("reuse", identity_hash.clone()),
                SchemaCoverage::Register { identity_hash, .. } => {
                    ("register", identity_hash.clone())
                }
                SchemaCoverage::WouldRegister { identity_hash, .. } => {
                    ("would-register", identity_hash.clone())
                }
                SchemaCoverage::Novel { reason } => ("novel", reason.clone()),
            };
            let mut row = serde_json::json!({
                "schema": o.schema_name,
                "resolution": resolution,
                "detail": detail,
                "covered": o.coverage.is_covered(),
            });
            if let Some(retention) = &o.retention {
                row["retention"] = retention.clone();
            }
            row
        })
        .collect()
}

pub(crate) fn print_check_outcomes(manifest: &AppManifest, outcomes: &[CheckOutcome], json: bool) {
    if json {
        print_pretty_json(&serde_json::json!({
            "app_id": manifest.app_id,
            "schemas": check_outcome_rows(outcomes),
            "catalog_collisions": app_publish::catalog_collisions(outcomes)
                .into_iter()
                .map(|(catalog, schemas)| serde_json::json!({ "catalog": catalog, "schemas": schemas }))
                .collect::<Vec<_>>(),
        }));
        return;
    }
    println!("app: {}", manifest.app_id);
    for o in outcomes {
        match &o.coverage {
            SchemaCoverage::Reuse { identity_hash } => {
                println!("  {}\treuse\t{identity_hash}", o.schema_name);
            }
            SchemaCoverage::Register { identity_hash } => {
                println!("  {}\tregister\t{identity_hash}", o.schema_name);
            }
            SchemaCoverage::WouldRegister {
                identity_hash,
                reason,
            } => println!(
                "  {}\twould-register\t{} (read-only check; nothing written{})",
                o.schema_name,
                identity_hash,
                reason
                    .as_deref()
                    .map(|r| format!("; reason={r}"))
                    .unwrap_or_default()
            ),
            SchemaCoverage::Novel { reason } => println!("  {}\tNOVEL\t{reason}", o.schema_name),
        }
        if let Some(retention) = &o.retention {
            print_retention_text(retention);
        }
    }
    for (catalog, names) in app_publish::catalog_collisions(outcomes) {
        println!(
            "WARNING: [{}] resolve to one catalog schema ({catalog}). They share one dataset: a record written through one name is a record of the others.",
            names.join(", ")
        );
    }
}

pub(crate) fn warn_trust_override(source: &lastdb_node::app_registry_index::TrustSource) {
    use lastdb_node::app_registry_index::TrustSource;
    match source {
        TrustSource::Pinned => {}
        TrustSource::Flag => eprintln!("note: index trust key overridden by --trust-key"),
        TrustSource::Env => eprintln!(
            "note: index trust key overridden by ${}",
            lastdb_node::app_registry_index::TRUST_KEY_ENV
        ),
    }
}

pub(crate) fn resolve_by_proof(
    socket: &Path,
    app_id: &str,
    channel: Option<&str>,
    index: Option<&str>,
    trust_key: Option<&str>,
) -> Result<lastdb_node::app_registry_index::Resolution, String> {
    use lastdb_node::app_registry_index as registry_index;
    let channel = channel.unwrap_or(registry_index::DEFAULT_CHANNEL);
    let location = registry_index::IndexLocation::resolve(index)?;
    let (trust, trust_source) = registry_index::resolve_list_trust_key(trust_key)?;
    warn_trust_override(&trust_source);
    let (node, node_source, versions) = registry_index::running_node(socket);
    let node_version = registry_index::NodeVersion {
        version: &node,
        source: node_source,
    };
    block_on_app(registry_index::resolve(
        &location,
        channel,
        &trust,
        &trust_source,
        app_id,
        &node_version,
        &versions,
    ))
}

pub(crate) fn app_check_command(
    home: &Path,
    manifest_path: &Path,
    json: bool,
    sync: bool,
    confirm_expire_existing: bool,
    schema_url: Option<String>,
) -> Result<(), String> {
    let manifest = app_publish::load_manifest(manifest_path)?;
    print_schema_advice(
        resolve_schema_service_url(schema_url, None),
        &manifest.app_id,
        &manifest.schemas,
    );
    let mode = if sync {
        CheckMode::Sync
    } else {
        CheckMode::ReadOnly
    };
    let outcomes = app_check(home, &manifest, mode, confirm_expire_existing)?;
    let node_novel = app_publish::novel_schema_names(&outcomes);
    print_check_outcomes(&manifest, &outcomes, json);
    for schema in &manifest.schemas {
        let name = schema
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("<unnamed>");
        if node_novel.iter().any(|n| n == name) {
            let missing = app_publish::missing_field_descriptions(schema);
            if !missing.is_empty() {
                eprintln!(
                    "note: '{name}' is novel and will need field_descriptions to register — missing: {}",
                    missing.join(", ")
                );
            }
        }
    }
    if node_novel.is_empty() {
        collision_error(&manifest, &outcomes).map_or(Ok(()), Err)
    } else if let Some(refused) = retention_refusals(&outcomes) {
        Err(format!(
            "the node did not apply the manifest retention for: {refused}. No retention policy was changed for those schemas. Fix the cause named above, then run `lastdb app check` again: a declare is idempotent."
        ))
    } else if sync {
        Err(format!(
            "Mini did not return bind-eligible catalog identities for: {}. Retry `lastdb app check --sync` after the reported dependency or quota recovers; do not call Schema Service directly.",
            node_novel.join(", ")
        ))
    } else {
        Err(format!(
            "a catalog sync would fail for: {}. Fix the reported error, then re-run `lastdb app check` (read-only) before `lastdb app check --sync`.",
            node_novel.join(", ")
        ))
    }
}

pub(super) fn listed_schemas(manifest: &Value, app_id: &str) -> Result<Vec<ListedSchema>, String> {
    let schemas = manifest
        .get("schemas")
        .ok_or("manifest must list the schemas the app declares in `schemas` (an empty list when it declares none)")?
        .as_array()
        .ok_or("manifest `schemas` must be a list of schema declarations")?;
    let untyped: Vec<String> = schemas
        .iter()
        .flat_map(|schema| {
            let name = schema.get("name").and_then(Value::as_str).unwrap_or("?");
            let typed = schema.get("field_types").and_then(Value::as_object);
            schema
                .get("fields")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .filter(move |f| !typed.is_some_and(|m| m.contains_key(*f)))
                .map(move |f| format!("{name}.{f}"))
        })
        .collect();
    if !untyped.is_empty() {
        return Err(format!(
            "every schema field needs a type in field_types; these fields have none: {}",
            untyped.join(", ")
        ));
    }
    schemas
        .iter()
        .map(|entry| {
            let label = entry
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("<unnamed>");
            let mut entry = entry.clone();
            if let Some(object) = entry.as_object_mut() {
                object.remove("retention");
            }
            let schema: DeclarativeSchemaDefinition =
                serde_json::from_value(entry).map_err(|e| {
                    format!("manifest schema '{label}' is not a schema declaration: {e}")
                })?;
            let schema = normalize_app_manifest_schema(app_id, schema)
                .map_err(|e| format!("manifest schema '{label}' cannot be declared: {e}"))?;
            let identity_hash = schema
                .identity_hash
                .clone()
                .ok_or_else(|| format!("manifest schema '{label}' has no identity hash"))?;
            Ok(ListedSchema {
                name: schema.name.clone(),
                descriptive_name: schema.descriptive_name.clone(),
                identity_hash,
            })
        })
        .collect()
}

fn held_schemas(socket: &Path, app_id: &str) -> Result<Vec<HeldSchema>, String> {
    let req = format!(
        "GET /api/schemas HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        client_headers()
    );
    let response = request_readonly_with_retry(socket, req.as_bytes(), NODE_SCHEMAS_TIMEOUT)?;
    let value = parse_json_response(&response, "GET /api/schemas")?;
    let rows = value
        .get("data")
        .unwrap_or(&value)
        .get("schemas")
        .and_then(Value::as_array)
        .ok_or("the node's schema list has no schemas array")?;
    let text = |row: &Value, key: &str| row.get(key).and_then(Value::as_str).map(str::to_string);
    Ok(rows
        .iter()
        .filter(|row| row.get("owner_app_id").and_then(Value::as_str) == Some(app_id))
        .map(|row| HeldSchema {
            name: text(row, "name").unwrap_or_default(),
            descriptive_name: text(row, "descriptive_name"),
            identity_hash: text(row, "identity_hash"),
        })
        .collect())
}

pub(super) fn require_held_schemas_listed(
    socket: &Path,
    app_id: &str,
    listed: &[ListedSchema],
) -> Result<(), String> {
    let held = held_schemas(socket, app_id).map_err(|e| {
        format!(
            "publish reads the running node at {} to check that the manifest lists every schema app '{app_id}' declares: {e}",
            socket.display()
        )
    })?;
    let unlisted: Vec<String> = held
        .iter()
        .filter_map(|h| {
            let held_id = h.identity_hash.as_deref();
            let related: Vec<&ListedSchema> = listed
                .iter()
                .filter(|s| {
                    s.name == h.name
                        || held_id == Some(s.identity_hash.as_str())
                        || (h.descriptive_name.is_some()
                            && s.descriptive_name == h.descriptive_name)
                })
                .collect();
            if related
                .iter()
                .any(|s| held_id == Some(s.identity_hash.as_str()))
            {
                return None;
            }
            let shown = held_id.unwrap_or("none");
            Some(related.first().map_or_else(
                || format!("{} (identity {shown}) is not listed", h.name),
                |s| {
                    format!(
                        "{} has identity {shown} on the node, but the manifest's {} has identity {}",
                        h.name, s.name, s.identity_hash
                    )
                },
            ))
        })
        .collect();
    if unlisted.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "the manifest must list every schema app '{app_id}' declares, exactly as declared; the running node holds: {}",
            unlisted.join("; ")
        ))
    }
}
