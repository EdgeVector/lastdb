//! The `databases` extension. Each action calls the owner route's handler.
//! A success reply is that handler's JSON. A refusal uses the app API error shape.

use fold_db::access::AccessContext;
use serde_json::{Map, Value};

use super::databases_reply::pass_owner;
use super::{bad, require_extension, require_write};
use crate::exec::calling_app::active_app;
use crate::exec::database_routes;
use crate::exec::reply::HostError;
use crate::exec::with_handler_timeout_budget;
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};
use crate::lastdb_uds::uds_router::valid_selector;
use crate::lastdb_uds::{admin_handler_timeout, handler_timeout, DATABASES_PATH};

const ACCEPT_PATH: &str = "/api/databases/accept";

struct OwnerCall {
    method: &'static str,
    target: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl OwnerCall {
    fn empty(method: &'static str, target: String) -> Self {
        Self {
            method,
            target,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    fn json(method: &'static str, target: String, body: &Value) -> Result<Self, HostError> {
        let bytes = serde_json::to_vec(body)
            .map_err(|_| HostError::internal("could not encode the owner body"))?;
        Ok(Self {
            method,
            target,
            headers: Vec::new(),
            body: bytes,
        })
    }

    fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_owned(), value.to_owned()));
        self
    }

    fn request(self) -> UdsRequest {
        UdsRequest {
            method: self.method.to_owned(),
            target: self.target,
            headers: self.headers,
            body: self.body,
        }
    }
}

pub(in crate::exec) async fn databases_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    match run(req, ctx, host).await {
        Ok(response) => response,
        Err(error) => super::fail(&error),
    }
}

async fn run(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> Result<UdsResponse, HostError> {
    require_extension("databases")?;
    let body = object(&req.body)?;
    let action = required_string(&body, "action")?;
    let call = build(action, &body, req, host)?;
    // declare_schema uses the admin budget the owner route uses. The route
    // wrapper is longer, so this deadline is the one that answers, in the
    // app API error shape.
    let budget = if action == "declare_schema" {
        admin_handler_timeout()
    } else {
        handler_timeout()
    };
    let mut inner = call.request();
    let response = with_handler_timeout_budget(
        ctx,
        budget,
        database_routes::execute_database_request(&mut inner, ctx, host),
    )
    .await;
    pass_owner(response)
}

fn build(
    action: &str,
    body: &Map<String, Value>,
    req: &UdsRequest,
    host: &Host,
) -> Result<OwnerCall, HostError> {
    match action {
        "list" => list_request(body),
        "create" => create_request(body, req),
        "accept" => accept_request(body),
        "schemas" => schemas_request(body, host),
        "declare_schema" => declare_request(body, host),
        "share_config" => share_config_request(body, host),
        "set_share_config" => set_share_config_request(body, host),
        "invite" => invite_request(body, host),
        "submit" => submit_request(body, host),
        "submission_status" => status_request(body, host),
        _ => Err(bad(format!("unknown action {action}"))),
    }
}

fn list_request(body: &Map<String, Value>) -> Result<OwnerCall, HostError> {
    allow(body, &[])?;
    Ok(OwnerCall::empty("GET", DATABASES_PATH.to_owned()))
}

fn create_request(body: &Map<String, Value>, req: &UdsRequest) -> Result<OwnerCall, HostError> {
    allow(body, &["name", "spec", "kind", "sharingMode"])?;
    let mut owner = Map::new();
    copy_string(body, &mut owner, "name")?;
    copy_present(body, &mut owner, &["spec", "kind", "sharingMode"]);
    let call = OwnerCall::json("POST", DATABASES_PATH.to_owned(), &Value::Object(owner))?;
    Ok(match req.header("Idempotency-Key") {
        Some(key) => call.header("Idempotency-Key", key),
        None => call,
    })
}

fn accept_request(body: &Map<String, Value>) -> Result<OwnerCall, HostError> {
    allow(body, &["invitation", "publisher", "name", "database_id"])?;
    let mut owner = Map::new();
    copy_string(body, &mut owner, "invitation")?;
    copy_string(body, &mut owner, "publisher")?;
    copy_present(body, &mut owner, &["name", "database_id"]);
    OwnerCall::json("POST", ACCEPT_PATH.to_owned(), &Value::Object(owner))
}

fn schemas_request(body: &Map<String, Value>, host: &Host) -> Result<OwnerCall, HostError> {
    allow(
        body,
        &["database", "include_full", "include_definition_ids"],
    )?;
    let id = database_id(body, host)?;
    Ok(OwnerCall::empty("GET", schemas_target(&id, body)?))
}

fn declare_request(body: &Map<String, Value>, host: &Host) -> Result<OwnerCall, HostError> {
    allow(
        body,
        &["database", "schema", "intent", "retention", "app_id"],
    )?;
    let schema = required_object(body, "schema")?;
    // A missing or empty name is the owner handler's refusal. The grant check
    // applies only to a name the app could have declared.
    if let Some(name) = schema
        .get("name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
    {
        require_write(name)?;
    }
    let app_id = declared_app_id(body)?;
    let id = database_id(body, host)?;
    let mut owner = Map::new();
    owner.insert("app_id".into(), Value::String(app_id));
    owner.insert("schema".into(), schema.clone());
    copy_present(body, &mut owner, &["intent", "retention"]);
    OwnerCall::json(
        "POST",
        scoped(&id, "apps/declare-schema"),
        &Value::Object(owner),
    )
}

/// On the app socket a present `app_id` is the schema namespace. Omit it, or
/// send JSON null, and the calling app's id is used. The owner socket still
/// requires the field. The owner handler requires the schema namespace to match.
fn declared_app_id(body: &Map<String, Value>) -> Result<String, HostError> {
    if let Some(app) = active_app() {
        return Ok(match optional_string(body, "app_id")? {
            Some(id) => id.to_owned(),
            None => app.app.clone(),
        });
    }
    Ok(required_string(body, "app_id")?.to_owned())
}

fn share_config_request(body: &Map<String, Value>, host: &Host) -> Result<OwnerCall, HostError> {
    allow(body, &["database"])?;
    Ok(OwnerCall::empty(
        "GET",
        scoped(&database_id(body, host)?, "share/config"),
    ))
}

fn set_share_config_request(
    body: &Map<String, Value>,
    host: &Host,
) -> Result<OwnerCall, HostError> {
    allow(body, &["database", "config"])?;
    let config = required_object(body, "config")?;
    OwnerCall::json(
        "PUT",
        scoped(&database_id(body, host)?, "share/config"),
        config,
    )
}

fn invite_request(body: &Map<String, Value>, host: &Host) -> Result<OwnerCall, HostError> {
    allow(
        body,
        &[
            "database",
            "contactCode",
            "expectedNodeKey",
            "expiresInHours",
        ],
    )?;
    let mut owner = Map::new();
    copy_string(body, &mut owner, "contactCode")?;
    copy_required(body, &mut owner, "expectedNodeKey")?;
    copy_present(body, &mut owner, &["expiresInHours"]);
    OwnerCall::json(
        "POST",
        scoped(&database_id(body, host)?, "invitations"),
        &Value::Object(owner),
    )
}

fn submit_request(body: &Map<String, Value>, host: &Host) -> Result<OwnerCall, HostError> {
    allow(body, &["database", "mutation", "idempotency_key"])?;
    let key = required_string(body, "idempotency_key")?;
    let mutation = required_object(body, "mutation")?;
    Ok(OwnerCall::json(
        "POST",
        scoped(&database_id(body, host)?, "submissions"),
        mutation,
    )?
    .header("Idempotency-Key", key))
}

fn status_request(body: &Map<String, Value>, host: &Host) -> Result<OwnerCall, HostError> {
    allow(body, &["database", "mutationId"])?;
    let mut owner = Map::new();
    copy_string(body, &mut owner, "mutationId")?;
    OwnerCall::json(
        "POST",
        scoped(&database_id(body, host)?, "submissions/status"),
        &Value::Object(owner),
    )
}

fn object(bytes: &[u8]) -> Result<Map<String, Value>, HostError> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|error| bad(format!("invalid databases body: {error}")))?;
    match value {
        Value::Object(map) => Ok(map),
        _ => Err(bad("the body must be a JSON object")),
    }
}

fn allow(body: &Map<String, Value>, keys: &[&str]) -> Result<(), HostError> {
    for key in body.keys() {
        if key != "action" && !keys.contains(&key.as_str()) {
            return Err(bad(format!("unknown field {key}")));
        }
    }
    Ok(())
}

fn required_string<'a>(body: &'a Map<String, Value>, key: &str) -> Result<&'a str, HostError> {
    match body.get(key) {
        Some(Value::String(text)) if !text.is_empty() => Ok(text),
        Some(Value::String(_)) | None => Err(bad(format!("{key} is required"))),
        Some(_) => Err(bad(format!("{key} must be a string"))),
    }
}

fn optional_string<'a>(
    body: &'a Map<String, Value>,
    key: &str,
) -> Result<Option<&'a str>, HostError> {
    match body.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text)),
        Some(_) => Err(bad(format!("{key} must be a string"))),
    }
}

fn required_object<'a>(body: &'a Map<String, Value>, key: &str) -> Result<&'a Value, HostError> {
    match body.get(key) {
        Some(value) if value.is_object() => Ok(value),
        Some(_) => Err(bad(format!("{key} must be an object"))),
        None => Err(bad(format!("{key} is required"))),
    }
}

fn copy_string(
    body: &Map<String, Value>,
    owner: &mut Map<String, Value>,
    key: &str,
) -> Result<(), HostError> {
    owner.insert(
        key.to_owned(),
        Value::String(required_string(body, key)?.to_owned()),
    );
    Ok(())
}

fn copy_required(
    body: &Map<String, Value>,
    owner: &mut Map<String, Value>,
    key: &str,
) -> Result<(), HostError> {
    let value = body
        .get(key)
        .ok_or_else(|| bad(format!("{key} is required")))?;
    owner.insert(key.to_owned(), value.clone());
    Ok(())
}

fn copy_present(body: &Map<String, Value>, owner: &mut Map<String, Value>, keys: &[&str]) {
    for key in keys {
        if let Some(value) = body.get(*key).filter(|value| !value.is_null()) {
            owner.insert((*key).to_owned(), value.clone());
        }
    }
}

fn database_id(body: &Map<String, Value>, host: &Host) -> Result<String, HostError> {
    match optional_string(body, "database")? {
        Some(selector) if valid_selector(selector) => Ok(selector.to_owned()),
        Some(_) => Err(bad("invalid database identifier")),
        None => host
            .owned
            .owner()
            .map(|entry| entry.id)
            .map_err(HostError::from),
    }
}

fn schemas_target(id: &str, body: &Map<String, Value>) -> Result<String, HostError> {
    let mut query = String::new();
    for key in ["include_full", "include_definition_ids"] {
        if flag(body, key)? {
            if !query.is_empty() {
                query.push('&');
            }
            query.push_str(key);
            query.push_str("=true");
        }
    }
    let path = scoped(id, "schemas");
    if query.is_empty() {
        Ok(path)
    } else {
        Ok(format!("{path}?{query}"))
    }
}

fn flag(body: &Map<String, Value>, key: &str) -> Result<bool, HostError> {
    match body.get(key) {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Bool(value)) => Ok(*value),
        Some(_) => Err(bad(format!("{key} must be a boolean"))),
    }
}

fn scoped(id: &str, suffix: &str) -> String {
    format!("/api/databases/{id}/{suffix}")
}
