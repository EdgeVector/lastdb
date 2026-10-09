//! Admin, debug, snapshot and telemetry routes.

use super::super::*;

pub(crate) fn post_debug_field_match_probe(
    state: &SchemaServiceState,
    event: &Request,
) -> Result<Response<Body>, Error> {
    let body = match parse_body(event) {
        Ok(b) => b,
        Err(r) => return Ok(r),
    };
    let request: schema_service_core::FieldMatchProbeRequest = match serde_json::from_str(&body) {
        Ok(r) => r,
        Err(e) => {
            return json_response(
                400,
                &json!({"error": format!("Invalid field-match-probe request: {e}")}),
            );
        }
    };
    match state.field_match_probe(&request) {
        Ok(response) => {
            let body = serde_json::to_value(response).map_err(serialization_error)?;
            json_response(200, &body)
        }
        Err(e) => json_response(
            400,
            &json!({"error": format!("Field match probe failed: {e}")}),
        ),
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
// for the rationale and `state_expansion::is_cross_schema_type_expansion`
// for the historical fall-through bug this cleans up after.
//
// Intentionally NOT auth-gated for now — the action is reversible
// (data isn't deleted, only superseded) and the dev API gateway is
// already loopback/internal. If the same endpoint ever ships to prod,
// require X-API-Key like /v1/snapshot.
pub(crate) async fn post_admin_dedupe_descriptive_names(
    state: &SchemaServiceState,
) -> Result<Response<Body>, Error> {
    match state.dedupe_descriptive_names().await {
        Ok(report) => json_response(200, &json!({ "groups": report })),
        Err(e) => json_response(
            500,
            &json!({"error": format!("dedupe-descriptive-names failed: {e}")}),
        ),
    }
}

// Surgical cleanup for polluted registry entries. Marks schemas
// inactive (`superseded_by`) and removes their descriptive-name index
// entry without deleting immutable records.
pub(crate) async fn post_admin_deprecate_schemas(
    state: &SchemaServiceState,
    event: &Request,
) -> Result<Response<Body>, Error> {
    let body = match parse_body(event) {
        Ok(b) => b,
        Err(r) => return Ok(r),
    };
    let request: DeprecateSchemasRequest = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => {
            return json_response(
                400,
                &json!({"error": format!("Invalid deprecate-schemas request: {e}")}),
            );
        }
    };
    match state.deprecate_schemas(request).await {
        Ok(report) => json_response(200, &json!(report)),
        Err(e) => json_response(
            500,
            &json!({"error": format!("deprecate-schemas failed: {e}")}),
        ),
    }
}

pub(crate) fn get_admin_schema_match_telemetry(
    state: &SchemaServiceState,
) -> Result<Response<Body>, Error> {
    json_response(200, &json!(state.schema_match_telemetry_snapshot()))
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
    let (near_misses, total, next_offset) =
        state.query_near_misses(since.as_deref(), until.as_deref(), offset, limit);
    let resp = NearMissesResponse {
        near_misses,
        total,
        next_offset,
    };
    let body = serde_json::to_value(&resp).map_err(serialization_error)?;
    json_response(200, &body)
}
