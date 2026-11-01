//! Reuse checks, schema resolution, field-match probes and health.

use super::*;

#[allow(
    clippy::unused_async,
    reason = "actix-web route handler — signature must be async to satisfy the Handler trait"
)]
pub async fn batch_check_reuse(
    payload: web::Json<BatchSchemaReuseRequest>,
    state: web::Data<SchemaServiceState>,
) -> impl Responder {
    let request = payload.into_inner();

    match state.batch_reuse_reply(&request.schemas).into_http() {
        Ok(response) => {
            for entry in &request.schemas {
                match response.matches.get(&entry.descriptive_name) {
                    Some(m) if m.is_superset => {
                        state.record_schema_match_outcome("matched_existing", "batch_reuse", None);
                    }
                    Some(_) => state.record_schema_match_outcome(
                        "low_confidence_fallback",
                        "batch_reuse",
                        Some("insufficient_field_coverage"),
                    ),
                    None => state.record_schema_match_outcome(
                        "no_match",
                        "batch_reuse",
                        Some("no_candidate_schema"),
                    ),
                }
            }
            HttpResponse::Ok().json(response)
        }
        Err((status, body, message)) => {
            tracing::error!(target: "schema_service::schema", "{message}");
            json_status(status, body)
        }
    }
}

#[allow(
    clippy::unused_async,
    reason = "actix-web route handler — signature must be async to satisfy the Handler trait"
)]
pub async fn resolve_schemas(
    payload: web::Json<SchemaResolveRequest>,
    state: web::Data<SchemaServiceState>,
) -> impl Responder {
    let request = payload.into_inner();

    match state.schema_resolve_reply(&request).into_http() {
        Ok(response) => HttpResponse::Ok().json(response),
        Err((status, body)) => json_status(status, body),
    }
}

/// Eval helper: cosine field-context matches for component_cover embedding-beam.
#[allow(
    clippy::unused_async,
    reason = "actix-web route handler — signature must be async to satisfy the Handler trait"
)]
pub async fn field_match_probe(
    payload: web::Json<schema_service_core::FieldMatchProbeRequest>,
    state: web::Data<SchemaServiceState>,
) -> impl Responder {
    match state
        .field_match_probe_reply(&payload.into_inner())
        .into_http()
    {
        Ok(response) => HttpResponse::Ok().json(response),
        Err((status, body)) => json_status(status, body),
    }
}

#[allow(
    clippy::unused_async,
    reason = "actix-web route handler — signature must be async to satisfy the Handler trait"
)]
pub async fn health_check(state: web::Data<SchemaServiceState>) -> impl Responder {
    HttpResponse::Ok().json(HealthResponse {
        status: "healthy".to_string(),
        // The catalog write counter rides on health so a proof run can
        // observe "no schema registration during release or install"
        // without a route the design does not contain.
        schema_writes: state.current_schema_writes(),
    })
}
