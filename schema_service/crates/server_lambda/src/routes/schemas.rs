//! Schema registry routes: listing, lookup, similarity, mutation challenge, submit, resolve.

use crate::http::*;
use lambda_http::{Body, Error, Request, Response};
use schema_service_core::builtin_schemas::AvailableSource;
use schema_service_core::state::{similarity_threshold_reply, FieldVocabularyReply};
use schema_service_core::{
    log_schema_mutation_gate_result, RegistryIndexReply, SchemaMutationChallengeReply,
    SchemaMutationChallengeRequest,
};
use schema_service_server_shared::state::SchemaServiceState;
use schema_service_server_shared::types::{
    BatchSchemaReuseRequest, SchemaAddOutcome, SchemaEnvelope, SchemaResolveRequest,
};
use serde_json::{json, Value};
use std::collections::HashMap;

mod repost;
use repost::idempotent_repost_response;

pub(crate) fn get_registry_index(
    state: &SchemaServiceState,
    event: &Request,
) -> Result<Response<Body>, Error> {
    let if_none_match = event
        .headers()
        .get("if-none-match")
        .and_then(|v| v.to_str().ok());
    match state.registry_index_reply(if_none_match) {
        RegistryIndexReply::NotModified { etag } => cors_builder(304, "application/json")
            .header("ETag", etag)
            .body(Body::Empty)
            .map_err(|e| Error::from(format!("Failed to build response: {e}"))),
        RegistryIndexReply::Ready { index, etag } => {
            let body = serde_json::to_value(index).map_err(serialization_error)?;
            json_response_cacheable(200, &body, &etag)
        }
        RegistryIndexReply::Failed { body, .. } => json_response(500, &body),
    }
}

pub(crate) fn get_schemas_available(
    state: &SchemaServiceState,
    event: &Request,
) -> Result<Response<Body>, Error> {
    // An unknown source is an empty 200 before the registry read. A lock
    // failure stays a 500 only when the source is absent or known.
    let source = AvailableSource::parse(query_param(event, "source="));
    if source.is_unknown() {
        return json_response(200, &json!({ "schemas": Vec::<SchemaEnvelope>::new() }));
    }
    match state.get_all_schemas_cached() {
        Ok(schemas) => {
            let envelopes: Vec<SchemaEnvelope> = schemas
                .into_iter()
                .filter(|schema| source.keeps(schema))
                .map(|schema| SchemaEnvelope {
                    system: state.is_system_schema(&schema.name),
                    schema,
                })
                .collect();
            json_response(200, &json!({ "schemas": envelopes }))
        }
        Err(e) => json_response(
            500,
            &schema_service_core::error_only_body(&format!("Failed to get schemas: {e}")),
        ),
    }
}

pub(crate) fn get_schemas_similar(
    state: &SchemaServiceState,
    event: &Request,
    p: &str,
) -> Result<Response<Body>, Error> {
    let schema_name = p.trim_start_matches("/v1/schemas/similar/");
    let threshold = match similarity_threshold_reply(Some(parse_threshold(event))).into_http() {
        Ok(threshold) => threshold,
        Err((status, body)) => return json_response(status, &body),
    };

    match state
        .similar_schemas_reply(schema_name, threshold)
        .into_http()
    {
        Ok(response) => {
            let body = serde_json::to_value(response).map_err(serialization_error)?;
            json_response(200, &body)
        }
        Err((status, body)) => json_response(status, &body),
    }
}

// Singular /v1/schema/{name} or plural /v1/schemas/{name}
pub(crate) async fn get_schema(
    state: &SchemaServiceState,
    p: &str,
) -> Result<Response<Body>, Error> {
    let schema_name = p
        .strip_prefix("/v1/schemas/")
        .or_else(|| p.strip_prefix("/v1/schema/"))
        .unwrap_or("");
    crate::http::get_schema_by_name(state, schema_name).await
}

pub(crate) fn post_schemas_mutation_challenge(
    state: &SchemaServiceState,
    event: &Request,
) -> Result<Response<Body>, Error> {
    let request: SchemaMutationChallengeRequest =
        match typed_json_body(event, "Invalid challenge request") {
            Ok(request) => request,
            Err(response) => return response,
        };
    match state.issue_schema_mutation_challenge_reply(&request, request_ip(event).as_deref()) {
        SchemaMutationChallengeReply::Created(response) => {
            let body = serde_json::to_value(response).map_err(serialization_error)?;
            json_response(201, &body)
        }
        SchemaMutationChallengeReply::Failed { status, body } => json_response(status, &body),
    }
}

/// Optional `mutation_mappers` map; a malformed value is logged and ignored.
fn parse_mutation_mappers(request: &Value) -> HashMap<String, String> {
    match request.get("mutation_mappers") {
        Some(m) => match serde_json::from_value(m.clone()) {
            Ok(mappers) => mappers,
            Err(e) => {
                tracing::warn!("Failed to parse mutation_mappers, using empty: {}", e);
                HashMap::new()
            }
        },
        None => HashMap::new(),
    }
}

pub(crate) async fn post_schemas(
    state: &SchemaServiceState,
    event: &Request,
) -> Result<Response<Body>, Error> {
    let request: Value = match typed_json_body(event, "Invalid JSON") {
        Ok(request) => request,
        Err(response) => return response,
    };

    // Keep the raw `schema` sub-object: it is the payload the
    // `schema_claim` envelope signs (app_identity v3.1, Lane B2b).
    let Some(schema_value) = request.get("schema").cloned() else {
        return json_response(
            400,
            &schema_service_core::error_only_body("Missing 'schema' field"),
        );
    };
    let schema: schema_types::Schema = match serde_json::from_value(schema_value.clone()) {
        Ok(schema) => schema,
        Err(e) => {
            return json_response(
                400,
                &schema_service_core::error_only_body(&format!("Invalid schema: {e}")),
            );
        }
    };

    if let FieldVocabularyReply::Refused { body } = state.field_vocabulary_reply(&schema).await {
        return json_response(400, &body);
    }
    let mutation_mappers = parse_mutation_mappers(&request);

    // Absent / non-bool means a local namespace claim. Both a local claim
    // and a shared discovery offer need developer authorization when new.
    let offer_to_shared_discovery = request
        .get("offer_to_shared_discovery")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);

    let observation =
        state.observe_schema_mutation_gate(&schema, &mutation_mappers, offer_to_shared_discovery);

    // Validation precedes the cert-free echo of a public existing schema.
    if let Some(resp) = idempotent_repost_response(state, &schema, &mutation_mappers).await {
        return resp;
    }

    // Every new registration requires an Exemem developer account and the
    // registered app owner, including local claims and un-namespaced proposals.
    // A DEV deployment with SCHEMA_OPEN_REGISTRATION skips the account check.
    // Exact public reposts above remain reads and create no definitions.
    if let Err(e) = state
        .authorize_schema_claim(
            schema.owner_app_id.as_deref(),
            header_str(event, "x-exemem-dev-cert"),
            header_str(event, "x-signature"),
            &schema_value,
            offer_to_shared_discovery,
        )
        .await
    {
        let (status, body) = e.to_http();
        return json_response(status, &body);
    }

    let gate_result = state.enforce_schema_mutation_gate(
        &observation,
        &schema,
        &schema_value,
        &schema_mutation_gate_headers(event),
        request_ip(event).as_deref(),
    );
    log_schema_mutation_gate_result(&gate_result);
    if let Err(e) = gate_result {
        let (status, body) = e.to_http();
        return json_response(status, &body);
    }

    add_schema_response(state, schema, mutation_mappers).await
}

/// Success body for a schema add and for an idempotent repost.
///
/// `replaced_schema` is present only for an expansion. `composed` stays off
/// this body, so a composed outcome still matches a plain add.
pub(super) fn schema_add_body(
    schema: &schema_types::Schema,
    mutation_mappers: &HashMap<String, String>,
    replaced_schema: Option<&str>,
    system: bool,
) -> Value {
    let mut body = serde_json::Map::new();
    body.insert("schema".to_owned(), json!(schema));
    body.insert("mutation_mappers".to_owned(), json!(mutation_mappers));
    if let Some(old_name) = replaced_schema {
        body.insert("replaced_schema".to_owned(), json!(old_name));
    }
    body.insert("system".to_owned(), json!(system));
    Value::Object(body)
}

/// Register `schema` and map the outcome to its HTTP response.
async fn add_schema_response(
    state: &SchemaServiceState,
    schema: schema_types::Schema,
    mutation_mappers: HashMap<String, String>,
) -> Result<Response<Body>, Error> {
    match state
        .schema_add_reply(schema, mutation_mappers)
        .await
        .into_http()
    {
        Err((status, body, _)) => json_response(status, &body),
        Ok(outcome) => match *outcome {
            // `Composed` (newly-registered canonical referencing existing
            // sub-schemas via SchemaRef) serializes like `Added`: 201, no
            // replaced schema. Only produced once the compositional
            // `apply` step is enabled (follow-on to the advisory shadow
            // pass; off by default), so the wire shape is unchanged today.
            SchemaAddOutcome::Added(schema, mutation_mappers)
            | SchemaAddOutcome::Composed(schema, mutation_mappers, _) => {
                let system = state.is_system_schema(&schema.name);
                json_response(
                    201,
                    &schema_add_body(&schema, &mutation_mappers, None, system),
                )
            }
            SchemaAddOutcome::AlreadyExists(schema, mutation_mappers) => {
                let system = state.is_system_schema(&schema.name);
                json_response(
                    200,
                    &schema_add_body(&schema, &mutation_mappers, None, system),
                )
            }
            SchemaAddOutcome::Expanded(old_name, schema, mutation_mappers) => {
                let system = state.is_system_schema(&schema.name);
                json_response(
                    201,
                    &schema_add_body(&schema, &mutation_mappers, Some(old_name.as_str()), system),
                )
            }
            SchemaAddOutcome::DescriptiveNameConflict(conflict) => {
                tracing::warn!(
                    descriptive_name = %conflict.descriptive_name,
                    existing_canonical = %conflict.existing_canonical,
                    "Refused duplicate descriptive_name registration (409)",
                );
                json_response(
                    409,
                    &serde_json::to_value(&conflict).unwrap_or_else(|_| {
                        json!({
                            "existing_canonical": conflict.existing_canonical,
                            "descriptive_name": conflict.descriptive_name,
                            "reason": conflict.reason,
                        })
                    }),
                )
            }
        },
    }
}

pub(crate) fn post_schemas_batch_check_reuse(
    state: &SchemaServiceState,
    event: &Request,
) -> Result<Response<Body>, Error> {
    let request: BatchSchemaReuseRequest =
        match typed_json_body(event, "Invalid batch-check-reuse request") {
            Ok(request) => request,
            Err(response) => return response,
        };

    match state.batch_reuse_reply(&request.schemas).into_http() {
        Ok(response) => json_response(200, &json!({ "matches": response.matches })),
        Err((status, body, _)) => json_response(status, &body),
    }
}

pub(crate) fn post_schemas_resolve(
    state: &SchemaServiceState,
    event: &Request,
) -> Result<Response<Body>, Error> {
    let request: SchemaResolveRequest =
        match typed_json_body(event, "Invalid schema resolve request") {
            Ok(request) => request,
            Err(response) => return response,
        };

    match state.schema_resolve_reply(&request).into_http() {
        Ok(response) => {
            let body = serde_json::to_value(response).map_err(serialization_error)?;
            json_response(200, &body)
        }
        Err((status, body)) => json_response(status, &body),
    }
}
