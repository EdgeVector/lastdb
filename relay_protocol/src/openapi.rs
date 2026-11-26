//! The OpenAPI document of the protocol, built from the types (feature `schema`). It is what
//! `docs/designs/relay-protocol.openapi.json` holds; nothing else writes that file.

use schemars::JsonSchema;
use serde_json::{json, Map, Value};

use crate::{
    headers, BlobDrop, BlobGet, BlobList, BlobPut, Endpoint, ErrorAnswer, LogAppend, LogRead,
    LogStreams, LogTrim, PROTOCOL,
};

/// Every request and answer type, so one schema run collects them all as named definitions.
#[derive(JsonSchema)]
#[allow(dead_code)]
struct Contract {
    log_append: (crate::LogAppendRequest, crate::LogAppendAnswer),
    log_read: (crate::LogReadRequest, crate::LogReadAnswer),
    log_streams: (crate::LogStreamsRequest, crate::LogStreamsAnswer),
    log_trim: (crate::LogTrimRequest, crate::LogTrimAnswer),
    blob_put: (crate::BlobPutRequest, crate::BlobPutAnswer),
    blob_get: (crate::BlobGetRequest, crate::BlobGetAnswer),
    blob_list: (crate::BlobListRequest, crate::BlobListAnswer),
    blob_drop: (crate::BlobDropRequest, crate::BlobDropAnswer),
    error: ErrorAnswer,
}

/// The whole document.
///
/// # Panics
///
/// Never in practice: the schemas are plain JSON values.
#[must_use]
pub fn document() -> Value {
    let root = serde_json::to_value(schemars::schema_for!(Contract))
        .expect("a JSON Schema is a JSON value");
    let mut schemas = root
        .get("$defs")
        .or_else(|| root.get("definitions"))
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    for schema in schemas.values_mut() {
        rewrite_refs(schema);
        if let Some(object) = schema.as_object_mut() {
            object.remove("$schema");
        }
    }
    let mut paths = Map::new();
    for (path, item) in [
        operation::<LogAppend>(
            "logAppend",
            "Store signed entries, or one log segment, on the caller's stream.",
        ),
        operation::<LogRead>("logRead", "Read one writer's entries."),
        operation::<LogStreams>(
            "logStreams",
            "List one writer's device streams and how far each reaches.",
        ),
        operation::<LogTrim>(
            "logTrim",
            "Drop what a snapshot entry covers (personal database, publisher only).",
        ),
        operation::<BlobPut>("blobPut", "Begin or commit the upload of one blob."),
        operation::<BlobGet>("blobGet", "Get a presigned download of one blob."),
        operation::<BlobList>("blobList", "List the committed blobs of a database."),
        operation::<BlobDrop>(
            "blobDrop",
            "Drop named blobs (personal database, publisher only).",
        ),
    ] {
        paths.insert(path, item);
    }
    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "LastDB relay protocol",
            "version": PROTOCOL,
            "description": "Generated from the relay_protocol crate by `cargo run -p relay_protocol --features schema --bin relay-protocol-openapi`. Do not edit: change the types. Behavior: docs/designs/relay-protocol.md."
        },
        "paths": paths,
        "components": { "schemas": schemas },
    })
}

fn operation<E: Endpoint>(id: &str, summary: &str) -> (String, Value)
where
    E::Request: JsonSchema,
    E::Answer: JsonSchema,
{
    let mut responses = Map::new();
    responses.insert(
        "200".to_owned(),
        json!({
            "description": "OK",
            "content": { "application/json": { "schema": reference::<E::Answer>() } }
        }),
    );
    for status in E::ROUTE.error_statuses() {
        responses.insert(
            status.to_string(),
            json!({
                "description": "Refused",
                "content": { "application/json": { "schema": reference::<ErrorAnswer>() } }
            }),
        );
    }
    let parameters: Vec<Value> = [
        headers::PROTOCOL,
        headers::NODE_KEY,
        headers::DATABASE_ID,
        headers::DEVICE_ID,
        headers::ISSUED_AT,
        headers::NONCE,
        headers::SIGNATURE,
    ]
    .into_iter()
    .map(|name| json!({ "name": name, "in": "header", "required": true, "schema": { "type": "string" } }))
    .collect();
    (
        E::ROUTE.path().to_owned(),
        json!({
            "post": {
                "operationId": id,
                "summary": summary,
                "parameters": parameters,
                "requestBody": {
                    "required": true,
                    "content": { "application/json": { "schema": reference::<E::Request>() } }
                },
                "responses": responses,
            }
        }),
    )
}

fn reference<T: JsonSchema>() -> Value {
    json!({ "$ref": format!("#/components/schemas/{}", T::schema_name()) })
}

fn rewrite_refs(value: &mut Value) {
    match value {
        Value::Object(object) => {
            for (key, inner) in object.iter_mut() {
                if key == "$ref" {
                    if let Value::String(target) = inner {
                        for prefix in ["#/$defs/", "#/definitions/"] {
                            if let Some(name) = target.strip_prefix(prefix) {
                                *target = format!("#/components/schemas/{name}");
                                break;
                            }
                        }
                    }
                } else {
                    rewrite_refs(inner);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                rewrite_refs(item);
            }
        }
        _ => {}
    }
}
