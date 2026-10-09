//! Schema read routes: registry index, listing, lookup, similarity and reload.

use super::*;

#[allow(
    clippy::unused_async,
    reason = "actix-web route handler — signature must be async to satisfy the Handler trait"
)]
pub async fn registry_index(
    req: HttpRequest,
    state: web::Data<SchemaServiceState>,
) -> impl Responder {
    let current_etag = format!("\"{}\"", state.current_state_version());
    if req
        .headers()
        .get("if-none-match")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|etag| etag.trim() == current_etag)
    {
        return HttpResponse::NotModified()
            .insert_header(("ETag", current_etag))
            .finish();
    }

    match state.export_registry_index() {
        Ok(index) => HttpResponse::Ok()
            .insert_header(("Cache-Control", "public, max-age=60"))
            .insert_header(("ETag", format!("\"{}\"", index.registry_version)))
            .json(index),
        Err(e) => internal_error("Failed to export registry index", e),
    }
}

#[allow(
    clippy::unused_async,
    reason = "actix-web route handler — signature must be async to satisfy the Handler trait"
)]
pub async fn list_schemas(state: web::Data<SchemaServiceState>) -> impl Responder {
    let schemas = match read_schemas(&state) {
        Ok(s) => s,
        Err(r) => return r,
    };

    HttpResponse::Ok().json(SchemasListResponse {
        schemas: schemas.keys().cloned().collect(),
    })
}

#[derive(serde::Deserialize)]
pub struct AvailableSchemasQuery {
    /// Optional filter by schema source. Accepts `system_seed`, `starter_seed`,
    /// `user`. When absent, all schemas are returned.
    pub source: Option<String>,
}

#[allow(
    clippy::unused_async,
    reason = "actix-web route handler — signature must be async to satisfy the Handler trait"
)]
pub async fn get_available_schemas(
    state: web::Data<SchemaServiceState>,
    query: web::Query<AvailableSchemasQuery>,
) -> impl Responder {
    use schema_types::SchemaSource;

    // Parse the optional source filter. Unknown values fall through to "no
    // match" (empty response) rather than 400 — the filter is best-effort
    // and unknown sources simply select nothing.
    let source_filter: Option<SchemaSource> = query.source.as_deref().and_then(|s| match s {
        "system_seed" => Some(SchemaSource::SystemSeed),
        "starter_seed" => Some(SchemaSource::StarterSeed),
        "user" => Some(SchemaSource::User),
        _ => None,
    });

    let schemas = match read_schemas(&state) {
        Ok(s) => s,
        Err(r) => return r,
    };

    let envelopes: Vec<SchemaEnvelope> = schemas
        .iter()
        .filter(|(_, schema)| match source_filter {
            Some(wanted) => schema.source == wanted,
            // Default available is live language only: hide Schema.org
            // leftovers; keep templates (`owner_app_id=templates`).
            None => !schema_service_core::builtin_schemas::is_schema_org_leftover(schema),
        })
        .map(|(name, schema)| SchemaEnvelope {
            schema: schema.clone(),
            system: state.is_system_schema(name),
        })
        .collect();

    // If the user passed a source value that didn't parse to a known variant,
    // return an empty list — don't silently return everything.
    if query.source.is_some() && source_filter.is_none() {
        return HttpResponse::Ok().json(AvailableSchemasResponse { schemas: vec![] });
    }

    HttpResponse::Ok().json(AvailableSchemasResponse { schemas: envelopes })
}

/// `GET /v1/canonicalization-near-misses` — Phase C shadow-mode audit
/// log. Reads from the in-memory cache populated at startup and
/// appended to whenever the dual-signal algorithm would disagree with
/// the single-signal one (only happens when `SCHEMA_SHADOW_MODE=true`
/// at registration time, so an empty response on a server that has
/// shadow mode disabled is expected).
///
/// Records are returned newest-first. Filtering happens before
/// pagination: `total` is the count after `since`/`until` are applied,
/// `next_offset` carries the cursor when more records remain.
#[allow(
    clippy::unused_async,
    reason = "actix-web route handler — signature must be async to satisfy the Handler trait"
)]
pub async fn list_near_misses(
    state: web::Data<SchemaServiceState>,
    query: web::Query<NearMissesQuery>,
) -> impl Responder {
    use schema_service_core::state::DEFAULT_NEAR_MISSES_LIMIT;
    let offset = query.offset.unwrap_or(0);
    let limit = query.limit.unwrap_or(DEFAULT_NEAR_MISSES_LIMIT);
    let (near_misses, total, next_offset) = state.query_near_misses(
        query.since.as_deref(),
        query.until.as_deref(),
        offset,
        limit,
    );
    HttpResponse::Ok().json(NearMissesResponse {
        near_misses,
        total,
        next_offset,
    })
}

#[allow(
    clippy::unused_async,
    reason = "actix-web route handler — signature must be async to satisfy the Handler trait"
)]
pub async fn get_schema(
    path: web::Path<String>,
    state: web::Data<SchemaServiceState>,
) -> impl Responder {
    let schema_name = path.into_inner();
    tracing::info!(
            target: "schema_service::schema",
        "Schema service: getting schema '{}'",
        schema_name
    );

    let schemas = match read_schemas(&state) {
        Ok(s) => s,
        Err(r) => return r,
    };

    match schemas.get(&schema_name) {
        Some(schema) => {
            let system = state.is_system_schema(&schema.name);
            HttpResponse::Ok().json(SchemaEnvelope {
                schema: schema.clone(),
                system,
            })
        }
        None => not_found("Schema", &schema_name),
    }
}

/// Query string for the two similarity endpoints (`find_similar`,
/// `find_similar_transforms`). Both accept an optional cosine-similarity
/// `threshold`; see [`resolve_threshold`] for defaulting and validation.
#[derive(Debug, Deserialize)]
pub struct SimilarQuery {
    pub(super) threshold: Option<f64>,
}

/// Resolve and range-check the optional `threshold` query parameter shared
/// by [`find_similar`] and [`find_similar_transforms`]. Defaults to `0.5`
/// when absent; returns a `400` response when it falls outside `[0.0, 1.0]`.
pub(super) fn resolve_threshold(query: &SimilarQuery) -> Result<f64, HttpResponse> {
    let threshold = query.threshold.unwrap_or(0.5);
    if !(0.0..=1.0).contains(&threshold) {
        return Err(HttpResponse::BadRequest().json(ErrorResponse {
            error: "Threshold must be between 0.0 and 1.0".to_string(),
        }));
    }
    Ok(threshold)
}

#[allow(
    clippy::unused_async,
    reason = "actix-web route handler — signature must be async to satisfy the Handler trait"
)]
pub async fn find_similar(
    path: web::Path<String>,
    query: web::Query<SimilarQuery>,
    state: web::Data<SchemaServiceState>,
) -> impl Responder {
    let schema_name = path.into_inner();
    let threshold = match resolve_threshold(&query) {
        Ok(t) => t,
        Err(resp) => return resp,
    };

    tracing::info!(
            target: "schema_service::schema",
        "Schema service: finding schemas similar to '{}' with threshold {}",
        schema_name,
        threshold
    );

    match state.find_similar_schemas(&schema_name, threshold) {
        Ok(response) => HttpResponse::Ok().json(response),
        Err(e) => {
            let error_msg = format!("{e}");
            if error_msg.contains("not found") {
                HttpResponse::NotFound().json(ErrorResponse {
                    error: format!("Schema '{schema_name}' not found"),
                })
            } else {
                HttpResponse::InternalServerError().json(ErrorResponse {
                    error: format!("Failed to find similar schemas: {e}"),
                })
            }
        }
    }
}

pub async fn reload_schemas(state: web::Data<SchemaServiceState>) -> impl Responder {
    tracing::info!(
            target: "schema_service::schema",
        "Schema service: reloading schemas"
    );

    match state.load_schemas().await {
        Ok(_) => {
            let schemas = match read_schemas(&state) {
                Ok(s) => s,
                Err(r) => return r,
            };

            HttpResponse::Ok().json(ReloadResponse {
                success: true,
                schemas_loaded: schemas.len(),
            })
        }
        Err(e) => internal_error("Failed to reload schemas", e),
    }
}
