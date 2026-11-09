//! Admin, debug, and snapshot routes.

use crate::http::*;
use crate::snapshot::handle_snapshot_export_shared_only;
use lambda_http::{Body, Error, Request, Response};
use schema_service_server_shared::state::SchemaServiceState;
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

pub(crate) async fn get_snapshot_shared_only(
    state: &SchemaServiceState,
    event: &Request,
) -> Result<Response<Body>, Error> {
    handle_snapshot_export_shared_only(event, state).await
}
