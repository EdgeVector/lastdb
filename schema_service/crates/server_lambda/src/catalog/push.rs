//! `POST /v1/schemas`: one publisher registers the schemas for one listing.

use std::time::Duration;

use app_identity_crypto::{
    schema_push_statement, valid_app_id, validate_channel, verify_schema_push,
};
use lambda_http::{Body, Error, Request, Response};
use schema_types::{DeclarativeSchemaDefinition, FieldValueType};
use serde::Deserialize;
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
    json_response(200, &json!({ "registered": count }))
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
    let definitions = validate_schema_entries(&request.app_id, schemas)?;
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

/// Check one pushed schema entry: typed, a definition LastDB accepts, and no `Any`.
fn validate_schema_entries(
    app_id: &str,
    schemas: &[Value],
) -> Result<Vec<DeclarativeSchemaDefinition>, &'static str> {
    schemas
        .iter()
        .map(|declaration| validate_schema_entry(app_id, declaration))
        .collect()
}

fn validate_schema_entry(
    app_id: &str,
    declaration: &Value,
) -> Result<DeclarativeSchemaDefinition, &'static str> {
    let mut value = declaration.clone();
    if let Some(object) = value.as_object_mut() {
        object.remove("retention");
    }
    let mut schema: DeclarativeSchemaDefinition =
        serde_json::from_value(value).map_err(|_| "schema entry refused")?;
    schema
        .normalize_owned_declaration("app_id", app_id)
        .map_err(|_| "schema entry refused")?;
    if schema.field_types.values().any(contains_any) {
        return Err("schema entry refused");
    }
    schema.field_hashes.clear();
    schema.ensure_field_hashes();
    Ok(schema)
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

impl Catalog {
    async fn store_schemas(&self, prepared: &Prepared, source: &str) -> Option<usize> {
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
            image.apps.insert(
                key.clone(),
                AppRecord {
                    source: source.to_owned(),
                    sha: prepared.sha.clone(),
                    api_version: prepared.api_version,
                    registered_at: now(),
                    definitions: prepared.definitions.clone(),
                },
            );
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
