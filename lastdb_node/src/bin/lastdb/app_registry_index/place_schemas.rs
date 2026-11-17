//! Place a restricted app's schemas on the running node at activation. The
//! manifest may list any schema, whichever app first defined it.
//!
//! `POST /api/apps/declare-schema` takes one schema. The same definition
//! already in place is a success, so each manifest entry is posted once
//! and the node is not read first.

use std::path::Path;

use fold_db::schema::types::DeclarativeSchemaDefinition;
use lastdb_node::exec::normalize_app_manifest_schema;
use schema_types::expand_manifest_schema;
use serde_json::Value;

use super::str_err;

struct PreparedSchema {
    name: String,
    schema: DeclarativeSchemaDefinition,
    retention: Option<Value>,
}

pub(super) fn place(source: &Path, app_id: &str, home: &Path) -> Result<(), String> {
    let prepared = prepare(source, app_id)?;
    let socket = lastdb_node::lastdb_uds::socket_path(&home.join("data"));
    for item in prepared {
        post_one(&socket, item)?;
    }
    Ok(())
}

fn prepare(source: &Path, app_id: &str) -> Result<Vec<PreparedSchema>, String> {
    let raw = std::fs::read(source.join("lastdb-app.json")).map_err(str_err)?;
    let manifest: Value = serde_json::from_slice(&raw).map_err(str_err)?;
    let Some(schemas) = manifest.get("schemas").and_then(Value::as_array) else {
        return Err("manifest `schemas` must be a list of schema declarations".into());
    };
    schemas
        .iter()
        .map(|entry| prepare_one(app_id, entry))
        .collect()
}

fn prepare_one(app_id: &str, entry: &Value) -> Result<PreparedSchema, String> {
    let label = entry
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("<unnamed>")
        .to_owned();
    let mut raw = entry.clone();
    let retention = {
        let Some(object) = raw.as_object_mut() else {
            return Err(format!("schema '{label}' is not an object"));
        };
        object.remove("retention")
    };
    expand_manifest_schema(&mut raw)?;
    let schema: DeclarativeSchemaDefinition = serde_json::from_value(raw)
        .map_err(|error| format!("schema '{label}' is not a schema declaration: {error}"))?;
    let schema = normalize_app_manifest_schema(app_id, schema)
        .map_err(|error| format!("schema '{label}' cannot be declared: {error}"))?;
    Ok(PreparedSchema {
        name: schema.name.clone(),
        schema,
        retention,
    })
}

fn post_one(socket: &Path, item: PreparedSchema) -> Result<(), String> {
    // The node declares a schema under the namespace in its name, which is
    // not the installing app's id when the app lists another app's schema.
    let namespace = item
        .name
        .split_once('/')
        .map_or("", |(namespace, _)| namespace);
    let mut body = serde_json::json!({ "app_id": namespace, "schema": item.schema });
    if let Some(retention) = item.retention {
        body["retention"] = retention;
    }
    let raw = crate::uds_client::post_json_admin(socket, "/api/apps/declare-schema", &body)
        .map_err(|error| refused(&item.name, &error))?;
    accept_declare(&item.name, &raw)
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
