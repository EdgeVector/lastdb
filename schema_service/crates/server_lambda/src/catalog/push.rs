//! `POST /v1/schemas`: one publisher registers wire-form schema definitions
//! for one listing.
//!
//! Each entry is the definition the node holds, with `identity_hash` and
//! explicit record keys. The service computes the definition id with
//! `PortableSchemaId::from_catalog` and does not accept an id from the caller.
//! A schema's namespace does not have to equal the pushing app.

use std::collections::BTreeMap;
use std::time::Duration;

use app_identity_crypto::{
    schema_push_statement, valid_app_id, validate_channel, verify_schema_push,
};
use lambda_http::{Body, Error, Request, Response};
use schema_types::{DeclarativeSchemaDefinition, FieldValueType, PortableSchemaId};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::listing::ListLookup;
use super::storage::WriteOutcome;
use super::{now, AppRecord, Catalog, Image};
use crate::json_response;

const MAX_PUSH_BODY_BYTES: usize = 1024 * 1024;
const MAX_SCHEMAS: usize = 256;
const STORE_ATTEMPTS: u32 = 4;
const STORE_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) const SCHEMAS_PATH: &str = "/v1/schemas";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PushRequest {
    app_id: String,
    channel: String,
    api_version: u32,
    sha: String,
    schemas: Value,
    publisher_key: String,
    publisher_sig: String,
}

struct Prepared {
    app_id: String,
    channel: String,
    api_version: u32,
    sha: String,
    publisher_key: String,
    publisher_sig: String,
    statement: Vec<u8>,
    definitions: Vec<DeclarativeSchemaDefinition>,
    definition_ids: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct StoredDefinition<'a> {
    definition_id: &'a str,
    definition: &'a DeclarativeSchemaDefinition,
}

pub(crate) async fn push(
    catalog: Option<&Catalog>,
    event: &Request,
) -> Result<Response<Body>, Error> {
    let Some(catalog) = catalog else {
        return refused(503, "schema catalog unavailable", "catalog not configured");
    };
    let prepared = match prepare(event) {
        Ok(prepared) => prepared,
        Err(reason) => return refused(400, "invalid schema push", reason),
    };
    let entry = match catalog
        .listed_app(&prepared.channel, prepared.api_version, &prepared.app_id)
        .await
    {
        Ok(entry) => entry,
        Err(ListLookup::NotListed) => {
            return refused(404, "app is not listed", "app not listed");
        }
        Err(ListLookup::Unavailable) => {
            return refused(
                503,
                "schema catalog unavailable",
                "registry list unavailable",
            );
        }
    };
    if entry.sha != prepared.sha {
        return refused(403, "schema push refused", "sha mismatch");
    }
    if entry.publisher_key != prepared.publisher_key {
        return refused(403, "schema push refused", "publisher key mismatch");
    }
    if verify_schema_push(
        &prepared.publisher_key,
        &prepared.publisher_sig,
        &prepared.statement,
    )
    .is_err()
    {
        return refused(403, "schema push refused", "signature refused");
    }
    let Some(count) = catalog.store_schemas(&prepared, &entry.source).await else {
        return refused(503, "schema catalog unavailable", "storage failed");
    };
    tracing::info!(
        app_id = %prepared.app_id,
        channel = %prepared.channel,
        api_version = prepared.api_version,
        count,
        "schema_catalog_push_accepted"
    );
    json_response(
        200,
        &json!({
            "registered": count,
            "definition_ids": prepared.definition_ids,
        }),
    )
}

fn refused(
    status: u16,
    error: &'static str,
    reason: &'static str,
) -> Result<Response<Body>, Error> {
    tracing::warn!(reason, "schema_catalog_push_refused");
    json_response(status, &json!({ "error": error }))
}

fn prepare(event: &Request) -> Result<Prepared, &'static str> {
    let bytes = body_bytes(event)?;
    let request: PushRequest = match serde_json::from_slice(bytes) {
        Ok(request) => request,
        Err(error) if error.is_syntax() => return Err("invalid json"),
        Err(_) => return Err("invalid shape"),
    };
    validate_channel(&request.channel).map_err(|_| "invalid channel")?;
    if !valid_app_id(&request.app_id) {
        return Err("invalid app id");
    }
    if !valid_sha(&request.sha) {
        return Err("invalid sha");
    }
    if request.api_version == 0 {
        return Err("invalid api version");
    }
    let Some(schemas) = request.schemas.as_array() else {
        return Err("invalid shape");
    };
    if schemas.len() > MAX_SCHEMAS {
        return Err("too many schemas");
    }
    let (definitions, definition_ids) = validate_schema_entries(schemas)?;
    let statement = schema_push_statement(
        request.api_version,
        &request.app_id,
        &request.channel,
        &request.schemas,
        &request.sha,
    )
    .map_err(|_| "statement refused")?;
    Ok(Prepared {
        app_id: request.app_id,
        channel: request.channel,
        api_version: request.api_version,
        sha: request.sha,
        publisher_key: request.publisher_key,
        publisher_sig: request.publisher_sig,
        statement,
        definitions,
        definition_ids,
    })
}

fn body_bytes(event: &Request) -> Result<&[u8], &'static str> {
    let bytes = match event.body() {
        Body::Text(text) => text.as_bytes(),
        Body::Binary(bytes) => bytes.as_slice(),
        Body::Empty => return Err("body empty"),
    };
    if bytes.len() > MAX_PUSH_BODY_BYTES {
        return Err("body too large");
    }
    if bytes.is_empty() {
        return Err("body empty");
    }
    Ok(bytes)
}

fn valid_sha(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

/// Check one pushed wire-form definition. `from_catalog` checks the identity
/// hash, record keys, and typed fields. `Any` is refused here.
fn validate_schema_entries(
    schemas: &[Value],
) -> Result<(Vec<DeclarativeSchemaDefinition>, BTreeMap<String, String>), &'static str> {
    let mut definitions = Vec::with_capacity(schemas.len());
    let mut definition_ids = BTreeMap::new();
    for declaration in schemas {
        let (definition, id) = validate_schema_entry(declaration)?;
        if definition_ids.insert(definition.name.clone(), id).is_some() {
            return Err("schema entry refused");
        }
        definitions.push(definition);
    }
    Ok((definitions, definition_ids))
}

fn validate_schema_entry(
    declaration: &Value,
) -> Result<(DeclarativeSchemaDefinition, String), &'static str> {
    let Some(identity_hash) = declared_identity_hash(declaration) else {
        return Err("schema entry refused");
    };
    // `from_value` uses the wire codec, which rejects shapes the node rejects.
    let schema: DeclarativeSchemaDefinition =
        serde_json::from_value(declaration.clone()).map_err(|_| "schema entry refused")?;
    if schema.identity_hash.as_deref() != Some(identity_hash) || contains_any_type(&schema) {
        return Err("schema entry refused");
    }
    let id = PortableSchemaId::from_catalog(identity_hash, schema.clone())
        .map_err(|_| "schema entry refused")?;
    Ok((schema, id.as_str().to_owned()))
}

fn declared_identity_hash(declaration: &Value) -> Option<&str> {
    declaration
        .get("identity_hash")
        .and_then(Value::as_str)
        .filter(|hash| !hash.is_empty())
}

fn contains_any_type(schema: &DeclarativeSchemaDefinition) -> bool {
    schema.field_types.values().any(contains_any)
}

fn contains_any(kind: &FieldValueType) -> bool {
    match kind {
        FieldValueType::Any => true,
        FieldValueType::Array(inner) | FieldValueType::Map(inner) => contains_any(inner),
        FieldValueType::Object(fields) => fields.values().any(contains_any),
        FieldValueType::OneOf(choices) => choices.iter().any(contains_any),
        _ => false,
    }
}

fn encoded_definition(
    definition_id: &str,
    definition: &DeclarativeSchemaDefinition,
) -> Result<Vec<u8>, &'static str> {
    serde_json::to_vec(&StoredDefinition {
        definition_id,
        definition,
    })
    .map_err(|_| "definition object could not be encoded")
}

impl Catalog {
    async fn store_schemas(&self, prepared: &Prepared, source: &str) -> Option<usize> {
        self.write_definitions(prepared).await?;
        self.write_catalog(prepared, source).await
    }

    async fn write_definitions(&self, prepared: &Prepared) -> Option<()> {
        for definition in &prepared.definitions {
            let id = prepared.definition_ids.get(&definition.name)?;
            let body = encoded_definition(id, definition).ok()?;
            if !self.put_definition(id, &body).await {
                return None;
            }
        }
        Some(())
    }

    async fn put_definition(&self, id: &str, body: &[u8]) -> bool {
        for _ in 0..STORE_ATTEMPTS {
            if self
                .store
                .put_definition(&self.client, id, body, STORE_TIMEOUT)
                .await
                .is_ok()
            {
                return true;
            }
        }
        false
    }

    async fn write_catalog(&self, prepared: &Prepared, source: &str) -> Option<usize> {
        let count = prepared.definitions.len();
        let key = format!("{}/{}", prepared.app_id, prepared.channel);
        for _ in 0..STORE_ATTEMPTS {
            let Ok(loaded) = self.store.read(&self.client, STORE_TIMEOUT).await else {
                continue;
            };
            let (mut image, etag) = match loaded {
                Some((image, etag)) => (image, Some(etag)),
                None => (Image::empty(), None),
            };
            image.apps.insert(key.clone(), app_record(prepared, source));
            let stored = self
                .store
                .write(&self.client, &image, etag.as_deref(), STORE_TIMEOUT)
                .await;
            if matches!(stored, Ok(WriteOutcome::Stored)) {
                return Some(count);
            }
        }
        None
    }
}

fn app_record(prepared: &Prepared, source: &str) -> AppRecord {
    AppRecord {
        source: source.to_owned(),
        sha: prepared.sha.clone(),
        api_version: prepared.api_version,
        registered_at: now(),
        definitions: prepared.definitions.clone(),
        definition_ids: prepared.definition_ids.clone(),
    }
}
