//! Auth-gated registry snapshot export (`GET /v1/snapshot` and
//! `GET /v1/snapshot/shared-only`).
//!
//! Both routes validate the `X-API-Key` header against the deployment's key
//! DynamoDB table (via `api_key::validate_api_key`), then return a
//! `SnapshotEnvelope` for the requesting dev's hydration. The auth gate is
//! enforced on the Lambda only — the actix dev binary leaves the routes open
//! for localhost.

use crate::api_key;
use crate::http::{extract_api_key, json_response, serialization_error, unauthorized};
use lambda_http::{Body, Error, Request, Response};
use schema_service_core::snapshot::{SnapshotEmbeddings, SnapshotEnvelope};
use schema_service_server_shared::state::SchemaServiceState;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::OnceCell;

/// AWS clients used by the `X-API-Key` validation on the snapshot routes.
/// Initialized lazily on the first authenticated request so cold-start cost
/// stays off health probes and read-only schema endpoints. `API_KEYS_TABLE`
/// is supplied by the schema-infra stack as a cross-stack import from
/// `exemem-infra`.
struct ApiKeysClient {
    ddb: aws_sdk_dynamodb::Client,
    table_name: String,
}

static API_KEYS_CLIENT: OnceCell<Arc<ApiKeysClient>> = OnceCell::const_new();

async fn get_or_init_api_keys_client() -> Result<Arc<ApiKeysClient>, Error> {
    API_KEYS_CLIENT
        .get_or_try_init(|| async {
            let table_name = std::env::var("API_KEYS_TABLE").map_err(|_| {
                Error::from(
                    "API_KEYS_TABLE env var is not set. The schema service Lambda's \
                     `GET /v1/snapshot` route validates X-API-Key against the exemem \
                     ApiKeys DynamoDB table; the table name must be supplied by the \
                     deploy stack via cross-stack import from `exemem-infra` (see \
                     schema-infra: SchemaServiceStack).",
                )
            })?;
            let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
            Ok(Arc::new(ApiKeysClient {
                ddb: aws_sdk_dynamodb::Client::new(&aws_config),
                table_name,
            }))
        })
        .await
        .cloned()
}

/// Validate the request's `X-API-Key`. `Ok(Some(resp))` is the 401 to return;
/// `Ok(None)` means the key is valid. `rejection_log` names the route in the
/// warning emitted for a rejected key.
async fn reject_unless_authorized(
    event: &Request,
    rejection_log: &str,
) -> Result<Option<Response<Body>>, Error> {
    let Some(api_key) = extract_api_key(event) else {
        return unauthorized(
            "Missing or empty X-API-Key header. \
             Get a key at https://www.exemem.com/developer.",
        )
        .map(Some);
    };

    let client = get_or_init_api_keys_client().await?;
    if let Err(reason) = api_key::validate_api_key(&client.ddb, &client.table_name, api_key).await {
        tracing::warn!(
            target: "schema_service::auth",
            reason = %reason,
            "{rejection_log}"
        );
        return unauthorized(&reason).map(Some);
    }
    // The validated key is intentionally not logged: it's a credential.
    Ok(None)
}

/// Serialize a snapshot with its embeddings stripped.
///
/// Embeddings blow Lambda's 6 MB sync-invoke response cap. Verified
/// empirically on 2026-05-05: prod has 1700 canonical-field embeddings of
/// 384-dim f32 vectors. Binary is ~2.6 MB, but JSON-encoded (each f32
/// ~10-20 chars) lands at 15-25 MB. The Lambda runtime drops oversize
/// responses silently — Errors metric stays at 0, but API Gateway returns the
/// generic {"message":"Internal Server Error"} body instead of the handler's
/// structured response.
///
/// Dev nodes hydrating from a snapshot don't need the embeddings:
/// bootstrap-at-boot only registers schemas / views / transforms into
/// fold_db, and semantic-similarity matching is a schema_service-side concern
/// that doesn't run on the dev path. If a consumer ever needs the embeddings,
/// expose them via a dedicated endpoint (e.g. GET /v1/snapshot/embeddings
/// backed by an S3 presigned URL or paginated chunks) rather than inflating
/// this response.
fn snapshot_response<E: std::fmt::Display>(
    exported: Result<SnapshotEnvelope, E>,
    failure_prefix: &str,
) -> Result<Response<Body>, Error> {
    match exported {
        Ok(mut envelope) => {
            envelope.embeddings = SnapshotEmbeddings::default();
            let body = serde_json::to_value(envelope).map_err(serialization_error)?;
            json_response(200, &body)
        }
        Err(e) => json_response(500, &json!({"error": format!("{failure_prefix}: {e}")})),
    }
}

/// `GET /v1/snapshot` — auth-gated full registry export.
pub(crate) async fn handle_snapshot_export(
    event: &Request,
    state: &SchemaServiceState,
) -> Result<Response<Body>, Error> {
    if let Some(resp) = reject_unless_authorized(event, "Snapshot fetch rejected").await? {
        return Ok(resp);
    }
    snapshot_response(state.export_snapshot(), "Failed to export snapshot")
}

/// `GET /v1/snapshot/shared-only` — auth-gated shared-surface projection.
///
/// Same X-API-Key gate and embedding strip as [`handle_snapshot_export`], but
/// only system-owned / explicit shared schemas are included. Used by
/// resolver-pack publishing so private legacy bootstrap rows never leave the
/// control plane as pack inputs.
pub(crate) async fn handle_snapshot_export_shared_only(
    event: &Request,
    state: &SchemaServiceState,
) -> Result<Response<Body>, Error> {
    if let Some(resp) =
        reject_unless_authorized(event, "Shared-only snapshot fetch rejected").await?
    {
        return Ok(resp);
    }
    snapshot_response(
        state.export_shared_only_snapshot(),
        "Failed to export shared-only snapshot",
    )
}
