//! Admin, debug, snapshot and telemetry routes.

use crate::http::*;
use crate::snapshot::handle_snapshot_export_shared_only;
use lambda_http::{Body, Error, Request, Response};
use schema_service_core::state::SchemaMatchTelemetryReply;
use schema_service_server_shared::state::SchemaServiceState;
use schema_service_server_shared::types::DeprecateSchemasRequest;
use serde_json::json;

pub(crate) fn post_debug_field_match_probe(
    state: &SchemaServiceState,
    event: &Request,
) -> Result<Response<Body>, Error> {
    let request: schema_service_core::FieldMatchProbeRequest =
        match typed_json_body(event, "Invalid field-match-probe request") {
            Ok(request) => request,
            Err(response) => return response,
        };
    match state.field_match_probe_reply(&request).into_http() {
        Ok(response) => {
            let body = serde_json::to_value(response).map_err(serialization_error)?;
            json_response(200, &body)
        }
        Err((status, body)) => json_response(status, &body),
    }
}

// ============== Admin ==============
//
// One-shot embedding backfill. Run once after deploying the
// persisted-embeddings feature against an S3 bucket whose
// blobs pre-date the change — computes embeddings for every
// schema + canonical field that doesn't already have one and
// writes them to the sibling embedding blobs. Subsequent
// writes maintain the cache incrementally; subsequent cold
// starts load from the blob, skipping fastembed.
//
// Idempotent — already-persisted entries are skipped. Safe to
// call twice. Response body returns counts per cache.
pub(crate) async fn post_admin_warm_embeddings(
    state: &SchemaServiceState,
) -> Result<Response<Body>, Error> {
    let (canonical_computed, canonical_skipped) = state.warm_canonical_field_embeddings().await;
    let (descriptive_computed, descriptive_skipped) =
        state.warm_descriptive_name_embeddings().await;
    tracing::info!(
        canonical_computed,
        canonical_skipped,
        descriptive_computed,
        descriptive_skipped,
        "warm-embeddings complete"
    );
    json_response(
        200,
        &json!({
            "canonical_fields": {
                "computed": canonical_computed,
                "skipped": canonical_skipped,
            },
            "descriptive_names": {
                "computed": descriptive_computed,
                "skipped": descriptive_skipped,
            },
        }),
    )
}

// One-shot dev cleanup. Walks the registry, picks the active schema
// with the largest field set per `descriptive_name` as the survivor,
// marks the rest `superseded_by` the survivor. Idempotent — a clean
// registry returns an empty array. See state.rs:dedupe_descriptive_names
// for the rationale and the historical cross-schema_type fall-through.
//
// Intentionally NOT auth-gated for now — the action is reversible
// (data isn't deleted, only superseded) and the dev API gateway is
// already loopback/internal. If the same endpoint ever ships to prod,
// require X-API-Key like /v1/snapshot.
pub(crate) async fn post_admin_dedupe_descriptive_names(
    state: &SchemaServiceState,
) -> Result<Response<Body>, Error> {
    let (status, body, _) = state.dedupe_names_reply().await.into_http();
    json_response(status, &body)
}

// Surgical cleanup for polluted registry entries. Marks schemas
// inactive (`superseded_by`) and removes their descriptive-name index
// entry without deleting immutable records.
pub(crate) async fn post_admin_deprecate_schemas(
    state: &SchemaServiceState,
    event: &Request,
) -> Result<Response<Body>, Error> {
    let request: DeprecateSchemasRequest =
        match typed_json_body(event, "Invalid deprecate-schemas request") {
            Ok(request) => request,
            Err(response) => return response,
        };
    let (status, body, _) = state.deprecate_schemas_reply(request).await.into_http();
    json_response(status, &body)
}

pub(crate) fn get_admin_schema_match_telemetry(
    state: &SchemaServiceState,
) -> Result<Response<Body>, Error> {
    let SchemaMatchTelemetryReply { status, body } = state.schema_match_telemetry_reply();
    json_response(status, &body)
}

pub(crate) async fn get_snapshot_shared_only(
    state: &SchemaServiceState,
    event: &Request,
) -> Result<Response<Body>, Error> {
    handle_snapshot_export_shared_only(event, state).await
}

// ============== Phase C: Canonicalization near-misses ==============
//
// Shadow-mode audit log. Populated when `SCHEMA_SHADOW_MODE=true`
// on the writer and the dual-signal algorithm would have
// disagreed with the single-signal one. Filter + paginate via
// the shared core helper so behavior matches the actix wrapper
// bit-for-bit.
pub(crate) fn get_canonicalization_near_misses(
    state: &SchemaServiceState,
    event: &Request,
) -> Result<Response<Body>, Error> {
    let since = query_param(event, "since=").map(str::to_owned);
    let until = query_param(event, "until=").map(str::to_owned);
    let offset: usize = query_param(event, "offset=")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let limit: usize = query_param(event, "limit=")
        .and_then(|v| v.parse().ok())
        .unwrap_or(schema_service_core::state::DEFAULT_NEAR_MISSES_LIMIT);
    let resp = state.near_misses_reply(since.as_deref(), until.as_deref(), offset, limit);
    let body = serde_json::to_value(&resp).map_err(serialization_error)?;
    json_response(200, &body)
}
