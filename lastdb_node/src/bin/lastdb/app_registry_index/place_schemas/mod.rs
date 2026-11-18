//! Place each schema an app's signed access block names.
//!
//! For one name: the node already holds that definition, or Schema Service
//! returns it, or the manifest entry hashes to the signed id. The node
//! decides whether a later declare is a compatible change. `"*"` names no
//! schema, so it places nothing by itself. When either side is `"*"`, each
//! manifest schema that no side names is placed from that manifest. When
//! neither side is `"*"`, a manifest schema that no side names is not placed.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use app_identity_crypto::{Access, SchemaSet, ALL_SCHEMAS};
use fold_db::schema::types::DeclarativeSchemaDefinition;
use serde_json::Value;

mod definition_id;
mod manifest;
mod service;

struct Pending {
    schema: DeclarativeSchemaDefinition,
    retention: Option<Value>,
    /// Where the definition came from, for the line install prints.
    from: &'static str,
}

pub(super) fn place(
    source: &Path,
    app_id: &str,
    home: &Path,
    access: &Access,
) -> Result<(), String> {
    let wanted = signed_pairs(access)?;
    let wide = side_is_all(&access.read) || side_is_all(&access.write);
    if wanted.is_empty() && !wide {
        return Ok(());
    }
    let socket = lastdb_node::lastdb_uds::socket_path(&home.join("data"));
    let held = node_definitions(&socket)?;
    let mut ctx = Resolve {
        source,
        app_id,
        loaded: None,
        noted: false,
    };
    let mut pending = Vec::new();
    for (name, signed_id) in &wanted {
        if held.get(name).and_then(Option::as_deref) == Some(signed_id.as_str()) {
            continue;
        }
        let node = node_state(&held, name);
        pending.push(resolve(&mut ctx, name, signed_id, &node)?);
    }
    if wide {
        own_schemas(&mut ctx, &held, &wanted, &mut pending)?;
    }
    for item in pending {
        post_one(&socket, item)?;
    }
    Ok(())
}

fn side_is_all(side: &SchemaSet) -> bool {
    matches!(side, SchemaSet::All(marker) if marker == ALL_SCHEMAS)
}

/// Manifest schemas that no access side names.
///
/// The listing signs the commit, so the manifest at that commit is the
/// publisher's statement of these schemas. A `"*"` side cannot name them.
/// They are posted the same way a manifest entry is posted when Schema
/// Service misses: `placed schema <name> from the app's manifest`, with
/// that entry's retention.
///
/// A name the node already holds at this definition stays, and its
/// retention stays. A different definition is posted. The node accepts a
/// compatible change and refuses a breaking one. A refusal fails the
/// install, and the previous version stays active.
fn own_schemas(
    ctx: &mut Resolve<'_>,
    held: &BTreeMap<String, Option<String>>,
    named: &BTreeMap<String, String>,
    pending: &mut Vec<Pending>,
) -> Result<(), String> {
    let catalog = cached_manifest(&mut ctx.loaded, ctx.source, ctx.app_id)?;
    for item in catalog.unnamed(named)? {
        let name = item.schema.name.clone();
        let id = definition_id::portable_id(&item.schema).map_err(|error| {
            format!("schema '{name}': definition id could not be computed: {error}")
        })?;
        if held.get(&name).and_then(Option::as_deref) == Some(id.as_str()) {
            continue;
        }
        pending.push(Pending {
            schema: item.schema,
            retention: item.retention,
            from: "the app's manifest",
        });
    }
    Ok(())
}

fn signed_pairs(access: &Access) -> Result<BTreeMap<String, String>, String> {
    let mut pairs = BTreeMap::new();
    for side in [access.read.named(), access.write.named()]
        .into_iter()
        .flatten()
    {
        for (name, id) in side {
            if let Some(previous) = pairs.insert(name.clone(), id.clone()) {
                if previous.as_str() != id.as_str() {
                    return Err(format!(
                        "schema '{name}' is signed to two definition ids ({previous} and {id})"
                    ));
                }
            }
        }
    }
    Ok(pairs)
}

fn node_state(held: &BTreeMap<String, Option<String>>, name: &str) -> String {
    match held.get(name) {
        Some(Some(id)) => format!("the node holds definition {id}"),
        Some(None) => "the node holds this schema with no definition id".to_owned(),
        None => "not on the node".to_owned(),
    }
}

struct Resolve<'a> {
    source: &'a Path,
    app_id: &'a str,
    loaded: Option<manifest::Catalog>,
    noted: bool,
}

fn resolve(
    ctx: &mut Resolve<'_>,
    name: &str,
    signed_id: &str,
    node: &str,
) -> Result<Pending, String> {
    let service = match service::fetch(signed_id) {
        Ok(schema) => {
            return Ok(Pending {
                schema,
                retention: None,
                from: "Schema Service",
            });
        }
        Err(reason) => {
            note_service_miss(ctx, name, &reason);
            reason
        }
    };
    let catalog = match cached_manifest(&mut ctx.loaded, ctx.source, ctx.app_id) {
        Ok(catalog) => catalog,
        Err(error) => return Err(not_placed(name, signed_id, node, &service, &error)),
    };
    match catalog.definition(name, signed_id) {
        Ok(item) => Ok(Pending {
            schema: item.schema,
            retention: item.retention,
            from: "the app's manifest",
        }),
        Err(manifest) => Err(not_placed(name, signed_id, node, &service, &manifest)),
    }
}

fn note_service_miss(ctx: &mut Resolve<'_>, name: &str, reason: &str) {
    if ctx.noted {
        return;
    }
    ctx.noted = true;
    tracing::debug!(
        target: "lastdb::app",
        "schema service did not supply {name}: {reason}"
    );
}

fn cached_manifest<'a>(
    slot: &'a mut Option<manifest::Catalog>,
    source: &Path,
    app_id: &str,
) -> Result<&'a manifest::Catalog, String> {
    if slot.is_none() {
        *slot = Some(manifest::load(source, app_id)?);
    }
    slot.as_ref()
        .ok_or_else(|| "manifest was not loaded".to_owned())
}

fn not_placed(name: &str, signed_id: &str, node: &str, service: &str, manifest: &str) -> String {
    format!(
        "schema '{name}' at signed definition {signed_id} was not placed: {node}; Schema Service: {service}; manifest: {manifest}"
    )
}

fn node_definitions(socket: &Path) -> Result<BTreeMap<String, Option<String>>, String> {
    let path = "/api/schemas?include_definition_ids=true";
    let raw = crate::uds_client::get_path_retry(socket, path, Duration::from_secs(30))
        .map_err(|error| format!("cannot read schemas from the node: {error}"))?;
    let held = crate::uds_client::parse_json_response(&raw, "GET /api/schemas")
        .map_err(|error| format!("cannot read schemas from the node: {error}"))?;
    let Some(rows) = held
        .get("data")
        .unwrap_or(&held)
        .get("schemas")
        .and_then(Value::as_array)
    else {
        return Err("cannot read schemas from the node: response has no schemas".into());
    };
    let mut out = BTreeMap::new();
    for row in rows {
        let Some(name) = row.get("name").and_then(Value::as_str) else {
            continue;
        };
        let id = row
            .get("definition_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        out.insert(name.to_owned(), id);
    }
    Ok(out)
}

fn post_one(socket: &Path, item: Pending) -> Result<(), String> {
    let name = item.schema.name.clone();
    let from = item.from;
    let namespace = name.split_once('/').map_or("", |(namespace, _)| namespace);
    let mut body = serde_json::json!({ "app_id": namespace, "schema": item.schema });
    if let Some(retention) = item.retention {
        body["retention"] = retention;
    }
    let raw = crate::uds_client::post_json_admin(socket, "/api/apps/declare-schema", &body)
        .map_err(|error| refused(&name, &error))?;
    accept_declare(&name, &raw)?;
    // One line per schema this install placed, on stderr so the JSON on
    // stdout stays whole. It says which source supplied the definition.
    eprintln!("placed schema {name} from {from}");
    Ok(())
}

fn accept_declare(name: &str, raw: &str) -> Result<(), String> {
    let Err(fallback) =
        crate::uds_client::parse_json_response(raw, "POST /api/apps/declare-schema")
    else {
        return Ok(());
    };
    Err(refused(name, &node_reason(raw).unwrap_or(fallback)))
}

fn node_reason(raw: &str) -> Option<String> {
    let body = raw.split_once("\r\n\r\n")?.1.trim();
    if body.is_empty() {
        return None;
    }
    let Ok(value) = serde_json::from_str::<Value>(body) else {
        return Some(body.to_owned());
    };
    Some(
        value
            .pointer("/error/message")
            .and_then(Value::as_str)
            .or_else(|| value.get("error").and_then(Value::as_str))
            .or_else(|| value.get("message").and_then(Value::as_str))
            .unwrap_or(body)
            .to_owned(),
    )
}

fn refused(name: &str, reason: &str) -> String {
    format!("schema '{name}' was refused: {reason}")
}
