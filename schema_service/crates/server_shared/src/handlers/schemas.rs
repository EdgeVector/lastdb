//! Schema read routes: registry index, listing, lookup, similarity and reload.

use schema_service_core::builtin_schemas::AvailableSource;
use schema_service_core::state::{
    accepted_similarity_threshold, SchemaByNameReply, SimilarSchemasReply,
};
use schema_service_core::RegistryIndexReply;

use super::*;

#[allow(
    clippy::unused_async,
    reason = "actix-web route handler — signature must be async to satisfy the Handler trait"
)]
pub async fn registry_index(
    req: HttpRequest,
    state: web::Data<SchemaServiceState>,
) -> impl Responder {
    let if_none_match = req
        .headers()
        .get("if-none-match")
        .and_then(|v| v.to_str().ok());
    match state.registry_index_reply(if_none_match) {
        RegistryIndexReply::NotModified { etag } => HttpResponse::NotModified()
            .insert_header(("ETag", etag))
            .finish(),
        RegistryIndexReply::Ready { index, etag } => HttpResponse::Ok()
            .insert_header(("Cache-Control", "public, max-age=60"))
            .insert_header(("ETag", etag))
            .json(index),
        RegistryIndexReply::Failed { detail } => {
            internal_error("Failed to export registry index", detail)
        }
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
    /// `user`. When absent, the list hides Schema.org leftovers and keeps templates.
    /// An unknown value selects nothing.
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
    // Read first. A lock failure stays 500, including when `source` is unknown.
    // `AvailableSource::keeps` then drops every schema for an unknown name.
    let source = AvailableSource::parse(query.source.as_deref());
    let schemas = match read_schemas(&state) {
        Ok(s) => s,
        Err(r) => return r,
    };

    let envelopes: Vec<SchemaEnvelope> = schemas
        .iter()
        .filter(|(_, schema)| source.keeps(schema))
        .map(|(name, schema)| SchemaEnvelope {
            schema: schema.clone(),
            system: state.is_system_schema(name),
        })
        .collect();

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
    HttpResponse::Ok().json(state.near_misses_reply(
        query.since.as_deref(),
        query.until.as_deref(),
        offset,
        limit,
    ))
}

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

    match state.schema_by_name_reply(&schema_name).await {
        SchemaByNameReply::Ready(envelope) => HttpResponse::Ok().json(envelope),
        SchemaByNameReply::Missing => not_found("Schema", &schema_name),
        SchemaByNameReply::Failed { detail } => internal_error("Failed to get schema", detail),
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
    accepted_similarity_threshold(query.threshold).ok_or_else(|| {
        HttpResponse::BadRequest().json(ErrorResponse {
            error: "Threshold must be between 0.0 and 1.0".to_string(),
        })
    })
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

    match state.similar_schemas_reply(&schema_name, threshold) {
        SimilarSchemasReply::Ready(response) => HttpResponse::Ok().json(response),
        SimilarSchemasReply::Missing { body } => HttpResponse::NotFound().json(body),
        SimilarSchemasReply::Failed { body } => HttpResponse::InternalServerError().json(body),
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
