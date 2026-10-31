//! Schema registry routes: listing, lookup, similarity, mutation challenge, submit, resolve.

use crate::http::*;
use lambda_http::{Body, Error, Request, Response};
use schema_service_core::builtin_schemas::AvailableSource;
use schema_service_core::state::{
    accepted_similarity_threshold, SchemaResolveReply, SimilarSchemasReply,
};
use schema_service_core::{
    log_schema_mutation_gate_result, RegistryIndexReply, SchemaMutationChallengeRequest,
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
        RegistryIndexReply::Failed { detail } => json_response(
            500,
            &json!({"error": format!("Failed to export registry index: {detail}")}),
        ),
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
            &json!({"error": format!("Failed to get schemas: {e}")}),
        ),
    }
}

pub(crate) fn get_schemas_similar(
    state: &SchemaServiceState,
    event: &Request,
    p: &str,
) -> Result<Response<Body>, Error> {
    let schema_name = p.trim_start_matches("/v1/schemas/similar/");
    let Some(threshold) = accepted_similarity_threshold(Some(parse_threshold(event))) else {
        return json_response(
            400,
            &json!({"error": "Threshold must be between 0.0 and 1.0"}),
        );
    };

    match state.similar_schemas_reply(schema_name, threshold) {
        SimilarSchemasReply::Ready(response) => {
            let body = serde_json::to_value(response).map_err(serialization_error)?;
            json_response(200, &body)
        }
        SimilarSchemasReply::Missing => json_response(
            404,
            &json!({"error": format!("Schema '{schema_name}' not found")}),
        ),
        SimilarSchemasReply::Failed { detail } => json_response(
            500,
            &json!({"error": format!("Failed to find similar schemas: {detail}")}),
        ),
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
    let body = match parse_body(event) {
        Ok(b) => b,
        Err(r) => return Ok(r),
    };
    let request: SchemaMutationChallengeRequest = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => {
            return json_response(
                400,
                &json!({"error": format!("Invalid challenge request: {e}")}),
            );
        }
    };
    match state.issue_schema_mutation_challenge(&request, request_ip(event).as_deref()) {
        Ok(response) => {
            let body = serde_json::to_value(response).map_err(serialization_error)?;
            json_response(201, &body)
        }
        Err(error) => {
            let (status, body) = error.to_http();
            json_response(status, &body)
        }
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
    let body = match parse_body(event) {
        Ok(b) => b,
        Err(r) => return Ok(r),
    };

    let request: Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => {
            return json_response(400, &json!({"error": format!("Invalid JSON: {e}")}));
        }
    };

    // Keep the raw `schema` sub-object: it is the payload the
    // `schema_claim` envelope signs (app_identity v3.1, Lane B2b).
    let Some(schema_value) = request.get("schema").cloned() else {
        return json_response(400, &json!({"error": "Missing 'schema' field"}));
    };
    let schema: schema_types::Schema = match serde_json::from_value(schema_value.clone()) {
        Ok(schema) => schema,
        Err(e) => {
            return json_response(400, &json!({"error": format!("Invalid schema: {e}")}));
        }
    };

    if let Err(error) = state.validate_schema_field_vocabulary(&schema).await {
        return json_response(400, &json!({"error": error.to_string()}));
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

/// Register `schema` and map the outcome to its HTTP response.
async fn add_schema_response(
    state: &SchemaServiceState,
    schema: schema_types::Schema,
    mutation_mappers: HashMap<String, String>,
) -> Result<Response<Body>, Error> {
    match state.add_schema(schema, mutation_mappers).await {
        // `Composed` (newly-registered canonical referencing existing
        // sub-schemas via SchemaRef) serializes like `Added`: 201, no
        // replaced schema. Only produced once the compositional
        // `apply` step is enabled (follow-on to the advisory shadow
        // pass; off by default), so the wire shape is unchanged today.
        Ok(
            SchemaAddOutcome::Added(schema, mutation_mappers)
            | SchemaAddOutcome::Composed(schema, mutation_mappers, _),
        ) => {
            let system = state.is_system_schema(&schema.name);
            json_response(
                201,
                &json!({
                    "schema": schema,
                    "mutation_mappers": mutation_mappers,
                    "system": system,
                }),
            )
        }
        Ok(SchemaAddOutcome::AlreadyExists(schema, mutation_mappers)) => {
            let system = state.is_system_schema(&schema.name);
            json_response(
                200,
                &json!({
                    "schema": schema,
                    "mutation_mappers": mutation_mappers,
                    "system": system,
                }),
            )
        }
        Ok(SchemaAddOutcome::Expanded(old_name, schema, mutation_mappers)) => {
            let system = state.is_system_schema(&schema.name);
            json_response(
                201,
                &json!({
                    "schema": schema,
                    "mutation_mappers": mutation_mappers,
                    "replaced_schema": old_name,
                    "system": system,
                }),
            )
        }
        Ok(SchemaAddOutcome::DescriptiveNameConflict(conflict)) => {
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
        Err(e) => json_response(400, &json!({"error": format!("Failed to add schema: {e}")})),
    }
}

pub(crate) fn post_schemas_batch_check_reuse(
    state: &SchemaServiceState,
    event: &Request,
) -> Result<Response<Body>, Error> {
    let body = match parse_body(event) {
        Ok(b) => b,
        Err(r) => return Ok(r),
    };

    let request: BatchSchemaReuseRequest = match serde_json::from_str(&body) {
        Ok(r) => r,
        Err(e) => {
            return json_response(
                400,
                &json!({"error": format!("Invalid batch-check-reuse request: {e}")}),
            );
        }
    };

    match state.batch_check_schema_reuse(&request.schemas) {
        Ok(matches) => json_response(200, &json!({ "matches": matches })),
        Err(e) => json_response(
            500,
            &json!({"error": format!("Batch schema reuse check failed: {e}")}),
        ),
    }
}

pub(crate) fn post_schemas_resolve(
    state: &SchemaServiceState,
    event: &Request,
) -> Result<Response<Body>, Error> {
    let body = match parse_body(event) {
        Ok(b) => b,
        Err(r) => return Ok(r),
    };

    let request: SchemaResolveRequest = match serde_json::from_str(&body) {
        Ok(r) => r,
        Err(e) => {
            return json_response(
                400,
                &json!({"error": format!("Invalid schema resolve request: {e}")}),
            );
        }
    };

    match state.schema_resolve_reply(&request) {
        SchemaResolveReply::Ready(response) => {
            let body = serde_json::to_value(response).map_err(serialization_error)?;
            json_response(200, &body)
        }
        SchemaResolveReply::Refused { detail } => json_response(
            400,
            &json!({"error": format!("Schema resolve failed: {detail}")}),
        ),
    }
}
