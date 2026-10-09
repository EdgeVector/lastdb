//! HTTP route handlers for the schema service.
//!
//! Ported from `fold_db_node/src/schema_service/routes.rs` so this
//! repo owns the wire surface. The handlers are framework-shared:
//! the actix dev binary mounts them directly, and the schema-infra
//! Lambda will mount the same functions in Phase 1.

use actix_web::http::StatusCode;
use actix_web::{web, HttpRequest, HttpResponse, Responder};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::RwLockReadGuard;

use schema_types::Schema;

use schema_service_core::snapshot::{SnapshotEmbeddings, SnapshotEnvelope};
use schema_service_core::state::SchemaServiceState;
use schema_service_core::types::{
    AddSchemaRequest, AddSchemaResponse, AvailableSchemasResponse, BatchSchemaReuseRequest,
    BatchSchemaReuseResponse, DeprecateSchemasRequest, ErrorResponse, HealthResponse,
    NearMissesQuery, NearMissesResponse, ReloadResponse, ResetRequest, ResetResponse,
    SchemaAddOutcome, SchemaEnvelope, SchemaResolveRequest, SchemasListResponse,
};
use schema_service_core::{
    log_schema_mutation_gate_result, observe_legacy_schema_caller, validate_surface_metadata,
    SchemaMutationChallengeRequest, SchemaMutationGateHeaders, HEADER_DEV_PUBKEY,
    HEADER_NODE_PUBLIC_KEY, HEADER_NODE_SIGNATURE, HEADER_POW_CHALLENGE, HEADER_POW_CHALLENGE_MAC,
    HEADER_POW_COUNTER, HEADER_POW_DIFFICULTY_BITS, HEADER_POW_EXPIRES_AT, HEADER_POW_NONCE,
};

/// Log a missing-resource lookup and build the matching 404 response.
///
/// `entity` is the human-readable resource label (e.g. `"Transform source"`);
/// it appears both in the warn log (`<entity> '<id>' not found`) and the JSON
/// body (`<entity> not found`). `id` is the lookup key that missed.
fn not_found(entity: &str, id: &str) -> HttpResponse {
    tracing::warn!(target: "schema_service::schema", "{} '{}' not found", entity, id);
    HttpResponse::NotFound().json(ErrorResponse {
        error: format!("{entity} not found"),
    })
}

/// Log an internal failure against `schema_service::schema` and return a
/// `500` carrying the same `"{context}: {error}"` message.
///
/// Collapses the `match` `Err` arms whose log line and error body are
/// identical (`list_views`, `reload_schemas`, `batch_check_reuse`,
/// `list_transforms`).
fn internal_error(context: &str, error: impl std::fmt::Display) -> HttpResponse {
    let message = format!("{context}: {error}");
    tracing::error!(target: "schema_service::schema", "{message}");
    HttpResponse::InternalServerError().json(ErrorResponse { error: message })
}

fn read_schemas(
    state: &SchemaServiceState,
) -> Result<RwLockReadGuard<'_, std::collections::HashMap<String, Schema>>, HttpResponse> {
    state.schemas.read().map_err(|e| {
        tracing::error!(
            target: "schema_service::schema",
            "Failed to acquire schemas read lock: {}",
            e
        );
        HttpResponse::InternalServerError().json(ErrorResponse {
            error: "Failed to acquire schemas read lock".to_string(),
        })
    })
}

/// Read a request header as a trimmed, non-empty string.
fn header_value(req: &HttpRequest, name: &str) -> Option<String> {
    req.headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn request_ip(req: &HttpRequest) -> Option<String> {
    header_value(req, "X-Forwarded-For")
        .or_else(|| req.peer_addr().map(|addr| addr.ip().to_string()))
}

fn schema_mutation_gate_headers(req: &HttpRequest) -> SchemaMutationGateHeaders {
    SchemaMutationGateHeaders {
        node_public_key: header_value(req, HEADER_NODE_PUBLIC_KEY),
        node_signature: header_value(req, HEADER_NODE_SIGNATURE),
        challenge_id: header_value(req, HEADER_POW_CHALLENGE),
        nonce: header_value(req, HEADER_POW_NONCE),
        challenge_mac: header_value(req, HEADER_POW_CHALLENGE_MAC),
        difficulty_bits: header_value(req, HEADER_POW_DIFFICULTY_BITS),
        expires_at_unix_secs: header_value(req, HEADER_POW_EXPIRES_AT),
        counter: header_value(req, HEADER_POW_COUNTER),
        dev_pubkey: header_value(req, HEADER_DEV_PUBKEY),
    }
}

/// Build an `HttpResponse` from a `(status, body)` pair produced by the
/// app-identity error/outcome mappers.
fn json_status(status: u16, body: Value) -> HttpResponse {
    let code = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    HttpResponse::build(code).json(body)
}

mod add_schema;
mod admin;
mod apps;
mod fields;
mod resolve;
mod schemas;
pub use add_schema::*;
pub use admin::*;
pub use apps::*;
pub use fields::*;
pub use resolve::*;
pub use schemas::*;
