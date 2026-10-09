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

    match state.batch_check_schema_reuse(&request.schemas) {
        Ok(matches) => {
            for entry in &request.schemas {
                match matches.get(&entry.descriptive_name) {
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
            HttpResponse::Ok().json(BatchSchemaReuseResponse { matches })
        }
        Err(e) => internal_error("Batch schema reuse check failed", e),
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

    match state.resolve_schema_proposals(&request) {
        Ok(response) => HttpResponse::Ok().json(response),
        Err(e) => HttpResponse::BadRequest().json(ErrorResponse {
            error: format!("Schema resolve failed: {e}"),
        }),
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
    match state.field_match_probe(&payload.into_inner()) {
        Ok(response) => HttpResponse::Ok().json(response),
        Err(e) => HttpResponse::BadRequest().json(ErrorResponse {
            error: format!("Field match probe failed: {e}"),
        }),
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
