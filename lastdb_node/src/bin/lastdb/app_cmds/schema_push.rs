//! After the registry lists an app, send the running node's wire-form schema
//! definitions to Schema Service. A failed push is a warning. The listing stands.

use super::PublishArgs;
use app_identity_crypto::{schema_push_statement, sign, SigningKey};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use lastdb_node::profile::endpoints::{schema_service_url_for, Environment};
use serde_json::{json, Map, Value};
use std::path::Path;
use std::time::Duration;

const PUSH_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_BODY: usize = 1024 * 1024;
const MAX_SCHEMAS: usize = 256;
const NODE_LIST: &str = "/api/schemas?include_definition_ids=true&include_definitions=true";

pub(super) struct Request<'a> {
    pub(super) home: &'a Path,
    pub(super) env: Environment,
    pub(super) input: &'a PublishArgs,
    pub(super) key: &'a SigningKey,
    pub(super) payload: &'a Value,
    pub(super) response: &'a mut Value,
}

enum Attempt {
    Idle,
    Registered(u64),
    Failed(String),
}

enum Choice {
    Skip,
    Use(Value),
    Bad,
}

/// Push schemas when the registry listed the app. Returns the human success line.
pub(super) fn register(push: &mut Request<'_>) -> Option<String> {
    let listed = push.response.get("status").and_then(Value::as_str) == Some("listed");
    let private = push.payload.get("audience").and_then(Value::as_str) == Some("private");
    if !listed || push.input.prepare.is_some() || private {
        return None;
    }
    report(push, attempt(push))
}

fn attempt(push: &Request<'_>) -> Attempt {
    try_attempt(push).unwrap_or_else(Attempt::Failed)
}

fn try_attempt(push: &Request<'_>) -> Result<Attempt, String> {
    let app_id = required_str(push.payload, "app_id")
        .ok_or_else(|| push_failure("publish payload has no app_id"))?;
    let names =
        schema_names(push.input, app_id, push.payload).map_err(|error| push_failure(&error))?;
    if names.is_empty() {
        return Ok(Attempt::Idle);
    }
    let rows = node_rows(push.home).map_err(|error| push_failure(&error))?;
    let schemas = definitions(&names, &rows, push.payload)?;
    if schemas.is_empty() {
        return Ok(Attempt::Registered(0));
    }
    if schemas.len() > MAX_SCHEMAS {
        return Err(push_failure("more than 256 schemas"));
    }
    crate::block_on("schema push", post_signed(push, schemas)).map(Attempt::Registered)
}

fn schema_names(input: &PublishArgs, app_id: &str, payload: &Value) -> Result<Vec<String>, String> {
    let bytes = std::fs::read(&input.manifest).map_err(|_| "cannot read the app manifest")?;
    let manifest: Value = serde_json::from_slice(&bytes).map_err(|_| "app manifest is not JSON")?;
    let mut names = Vec::new();
    for (name, _, _) in super::listed_schemas(&manifest, app_id)? {
        push_unique(&mut names, name);
    }
    for side in ["read", "write"] {
        let Some(map) = access_map(payload, side) else {
            continue;
        };
        for name in map.keys() {
            push_unique(&mut names, name.clone());
        }
    }
    Ok(names)
}

fn push_unique(names: &mut Vec<String>, name: String) {
    if !name.is_empty() && !names.contains(&name) {
        names.push(name);
    }
}

fn access_map<'a>(payload: &'a Value, side: &str) -> Option<&'a Map<String, Value>> {
    payload
        .get("access")
        .and_then(|access| access.get(side))
        .and_then(Value::as_object)
}

fn node_rows(home: &Path) -> Result<Vec<Value>, String> {
    let socket = lastdb_node::lastdb_uds::socket_path(&home.join("data"));
    let raw = crate::get_path_retry(&socket, NODE_LIST, PUSH_TIMEOUT)
        .map_err(|error| format!("cannot read schemas from the running node: {error}"))?;
    let parsed = crate::parse_json_response(&raw, "GET /api/schemas")
        .map_err(|error| format!("cannot read schemas from the running node: {error}"))?;
    let body = parsed.get("data").unwrap_or(&parsed);
    body.get("schemas")
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(|| "the node's schema list has no schemas array".to_owned())
}

fn definitions(names: &[String], rows: &[Value], payload: &Value) -> Result<Vec<Value>, String> {
    let mut schemas = Vec::new();
    let mut bad = Vec::new();
    for name in names {
        match one_definition(name, rows, payload) {
            Choice::Skip => warn(&format!(
                "schema '{name}' is not on this node and was left out of the Schema Service push"
            )),
            Choice::Use(definition) => schemas.push(definition),
            Choice::Bad => bad.push(name.clone()),
        }
    }
    if bad.is_empty() {
        return Ok(schemas);
    }
    Err(mismatch_message(&bad))
}

fn one_definition(name: &str, rows: &[Value], payload: &Value) -> Choice {
    let Some(row) = rows
        .iter()
        .find(|row| row.get("name").and_then(Value::as_str) == Some(name))
    else {
        return Choice::Skip;
    };
    let Some(definition) = row.get("definition").filter(|value| value.is_object()) else {
        return Choice::Bad;
    };
    let Ok(portable) = portable_id(definition) else {
        return Choice::Bad;
    };
    let Some(node_id) = row.get("definition_id").and_then(Value::as_str) else {
        return Choice::Bad;
    };
    if portable != node_id || access_disagrees(payload, name, &portable) {
        return Choice::Bad;
    }
    Choice::Use(definition.clone())
}

fn portable_id(definition: &Value) -> Result<String, String> {
    let typed: schema_types::DeclarativeSchemaDefinition =
        serde_json::from_value(definition.clone()).map_err(|_| "definition is not a schema")?;
    let Some(catalog) = typed.identity_hash.clone().filter(|id| !id.is_empty()) else {
        return Err("definition has no identity hash".to_owned());
    };
    schema_types::PortableSchemaId::from_catalog(&catalog, typed)
        .map(|portable| portable.as_str().to_owned())
}

fn access_disagrees(payload: &Value, name: &str, portable: &str) -> bool {
    ["read", "write"].into_iter().any(|side| {
        let Some(map) = access_map(payload, side) else {
            return false;
        };
        map.get(name)
            .is_some_and(|value| value.as_str() != Some(portable))
    })
}

fn mismatch_message(names: &[String]) -> String {
    let shown: Vec<String> = names
        .iter()
        .take(8)
        .map(|name| format!("'{name}'"))
        .collect();
    let list = if names.len() > shown.len() {
        format!(
            "{}, and {} more",
            shown.join(", "),
            names.len() - shown.len()
        )
    } else {
        shown.join(", ")
    };
    let label = if names.len() == 1 {
        "schema"
    } else {
        "schemas"
    };
    push_failure(&format!(
        "{label} {list} definition id does not match the id Schema Service would store"
    ))
}

fn report(push: &mut Request<'_>, outcome: Attempt) -> Option<String> {
    match outcome {
        Attempt::Idle => None,
        Attempt::Registered(count) => registered_line(push, count),
        Attempt::Failed(message) => {
            warn(&message);
            if push.input.json {
                insert_service(push, json!({ "error": message }));
            }
            None
        }
    }
}

fn registered_line(push: &mut Request<'_>, count: u64) -> Option<String> {
    if push.input.json {
        insert_service(push, json!({ "registered": count }));
        return None;
    }
    (count > 0).then(|| {
        let noun = if count == 1 { "schema" } else { "schemas" };
        format!("Registered {count} {noun} with Schema Service")
    })
}

fn insert_service(push: &mut Request<'_>, value: Value) {
    if let Some(object) = push.response.as_object_mut() {
        object.insert("schema_service".into(), value);
    }
}

async fn post_signed(push: &Request<'_>, schemas: Vec<Value>) -> Result<u64, String> {
    let body = signed_body(push, Value::Array(schemas))?;
    let bytes =
        serde_json::to_vec(&body).map_err(|_| push_failure("cannot encode the schema push"))?;
    if bytes.len() > MAX_BODY {
        return Err(push_failure("request exceeds 1 MiB"));
    }
    post_bytes(push.env, bytes).await
}

fn signed_body(push: &Request<'_>, schemas: Value) -> Result<Value, String> {
    let app_id = required_str(push.payload, "app_id")
        .ok_or_else(|| push_failure("publish payload has no app_id"))?;
    let channel = required_str(push.payload, "channel")
        .ok_or_else(|| push_failure("publish payload has no channel"))?;
    let sha = required_str(push.payload, "sha")
        .ok_or_else(|| push_failure("publish payload has no sha"))?;
    let api_version = push
        .payload
        .get("api_version")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| push_failure("publish payload has no API version"))?;
    let statement = schema_push_statement(api_version, app_id, channel, &schemas, sha)
        .map_err(|_| push_failure("cannot build the schema push statement"))?;
    let publisher_sig = BASE64.encode(sign(push.key, &statement));
    let publisher_key = BASE64.encode(push.key.verifying_key().as_bytes());
    Ok(json!({
        "app_id": app_id,
        "channel": channel,
        "api_version": api_version,
        "sha": sha,
        "schemas": schemas,
        "publisher_key": publisher_key,
        "publisher_sig": publisher_sig,
    }))
}

fn required_str<'a>(payload: &'a Value, key: &str) -> Option<&'a str> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
}

async fn post_bytes(env: Environment, body: Vec<u8>) -> Result<u64, String> {
    let endpoint = service_endpoint(env)?;
    let client = lastdb_node::cloud_backup_client::build_shared_http_client()
        .map_err(|_| push_failure("cannot construct the Schema Service HTTP client"))?;
    let response = client
        .post(endpoint.clone())
        .timeout(PUSH_TIMEOUT)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .map_err(|error| push_failure(&one_line(&error.to_string())))?;
    if response.status().is_redirection() || response.url() != &endpoint {
        return Err(push_failure("request was redirected"));
    }
    let status = response.status().as_u16();
    if status != 200 {
        return Err(push_failure(&format!("HTTP {status}")));
    }
    let bytes = lastdb_node::cloud_backup_client::read_bounded_bytes(response, 65_536)
        .await
        .map_err(|_| push_failure("cannot read the Schema Service response"))?;
    let value: Value = serde_json::from_slice(&bytes)
        .map_err(|_| push_failure("Schema Service response was not JSON"))?;
    value
        .get("registered")
        .and_then(Value::as_u64)
        .ok_or_else(|| push_failure("Schema Service response did not include registered"))
}

fn service_endpoint(env: Environment) -> Result<reqwest::Url, String> {
    let base = schema_service_url_for(env).trim_end_matches('/');
    let url = reqwest::Url::parse(&format!("{base}/v1/schemas"))
        .map_err(|_| push_failure("Schema Service URL is invalid"))?;
    if url.scheme() != "https" || url.host_str().is_none() {
        return Err(push_failure("Schema Service URL must use HTTPS"));
    }
    Ok(url)
}

fn push_failure(detail: &str) -> String {
    let detail = one_line(detail);
    format!(
        "Schema Service schema push failed ({detail}); the app is listed but its schemas were not registered"
    )
}

fn one_line(text: &str) -> String {
    let flat = text.replace(['\n', '\r'], " ");
    let flat = flat.trim();
    if flat.is_empty() {
        return "unknown error".to_owned();
    }
    let mut chars = flat.chars();
    let short: String = chars.by_ref().take(180).collect();
    if chars.next().is_some() {
        format!("{short}...")
    } else {
        short
    }
}

fn warn(message: &str) {
    eprintln!("warning: {message}");
}
