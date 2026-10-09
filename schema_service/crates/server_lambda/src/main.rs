//! Schema Service Lambda handler.
//!
//! Ported from `schema-infra/lambdas/schema_service/` into the new
//! `schema_service` repo as part of Phase 1 (see
//! gbrain slug `projects/phase-1-absorb-lambda`). The handler wraps
//! `SchemaServiceState` as an HTTP API via API Gateway.
//!
//! Storage: S3 blobs. The schema service state is backed by the
//! `S3BlobPersistence` implementation of `ExternalSchemaPersistence`,
//! which stores schemas, canonical_fields, and views in a single bucket.
//!
//! Routes: all endpoints are mounted under `/v1/*` to match the actix
//! wrapper scaffolded in Phase 0. `/health` is also served unversioned
//! as a belt-and-braces alias during deploy transitions.
//!
//! `POST /v1/system/reset` remains actix-only (dev reset; see comment in
//! the main dispatch).

// The `lambda_http::Error` type wraps a boxed error that Clippy's
// result_large_err lint flags. The whole Lambda uses this error
// consistently; rewriting every handler to box-on-return would be
// substantial churn without behavior benefit.
#![allow(clippy::result_large_err)]

use schema_service_s3 as s3_persistence;

// State types are re-exported by `schema_service_server_shared` so the
// Lambda and actix wrapper agree on the canonical import path. Today
// both still ultimately resolve to `fold_db::schema_service::*`; when
// Phase 2 moves the brain into `schema_service_core`, only the
// re-export flips and this file does not change.
use schema_service_core::{
    log_schema_mutation_gate_result, SchemaMutationChallengeRequest, SchemaMutationGateHeaders,
    HEADER_DEV_PUBKEY, HEADER_NODE_PUBLIC_KEY, HEADER_NODE_SIGNATURE, HEADER_POW_CHALLENGE,
    HEADER_POW_CHALLENGE_MAC, HEADER_POW_COUNTER, HEADER_POW_DIFFICULTY_BITS,
    HEADER_POW_EXPIRES_AT, HEADER_POW_NONCE,
};
use schema_service_server_shared::state::SchemaServiceState;
use schema_service_server_shared::types::{
    BatchSchemaReuseRequest, DeprecateSchemasRequest, NearMissesResponse, SchemaAddOutcome,
    SchemaEnvelope, SchemaResolveRequest,
};

use lambda_http::{run, service_fn};
use lambda_http::{Body, Error, Request, Response};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use tokio::sync::OnceCell;

const SCHEMA_MUTATION_GATE_QUOTA_TABLE_ENV: &str = "SCHEMA_MUTATION_GATE_QUOTA_TABLE";
const SCHEMA_STORE_ENDPOINT_URL_ENV: &str = "SCHEMA_STORE_ENDPOINT_URL";
const SCHEMA_STORE_R2_ENDPOINT_ENV: &str = "SCHEMA_STORE_R2_ENDPOINT";
const SCHEMA_STORE_ACCESS_KEY_ID_ENV: &str = "SCHEMA_STORE_ACCESS_KEY_ID";
const SCHEMA_STORE_SECRET_ACCESS_KEY_ENV: &str = "SCHEMA_STORE_SECRET_ACCESS_KEY";
const SCHEMA_STORE_REGION_ENV: &str = "SCHEMA_STORE_REGION";
const SCHEMA_STORE_DEFAULT_REGION: &str = "auto";

// Global singleton for Lambda warm starts
static SCHEMA_STATE: OnceCell<Arc<SchemaServiceState>> = OnceCell::const_new();

/// AWS clients used by the `X-API-Key` validation on
/// `GET /v1/snapshot`. Initialized lazily on the first authenticated
/// request so cold-start cost stays off health probes and read-only
/// schema endpoints. `API_KEYS_TABLE` is supplied by the schema-infra
/// stack as a cross-stack import from `exemem-infra`.
struct ApiKeysClient {
    ddb: aws_sdk_dynamodb::Client,
    table_name: String,
}

static API_KEYS_CLIENT: OnceCell<Arc<ApiKeysClient>> = OnceCell::const_new();

async fn get_or_init_api_keys_client() -> Result<Arc<ApiKeysClient>, Error> {
    API_KEYS_CLIENT
        .get_or_try_init(|| async {
            let table_name = env::var("API_KEYS_TABLE").map_err(|_| {
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

/// Pull the `X-API-Key` header from a request. Header lookup is
/// case-insensitive in `http::HeaderMap`, so requests that send
/// `x-api-key` resolve identically.
fn extract_api_key(event: &Request) -> Option<&str> {
    event
        .headers()
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

/// Pull an arbitrary header as a trimmed, non-empty string. Header lookup
/// is case-insensitive. Used for the app-identity `X-Exemem-Dev-Cert` and
/// `X-Signature` envelopes.
fn header_str<'a>(event: &'a Request, name: &str) -> Option<&'a str> {
    event
        .headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

fn request_ip(event: &Request) -> Option<String> {
    header_str(event, "x-forwarded-for")
        .map(str::to_string)
        .or_else(|| {
            event
                .extensions()
                .get::<lambda_http::request::RequestContext>()
                .and_then(|ctx| match ctx {
                    lambda_http::request::RequestContext::ApiGatewayV2(ctx) => {
                        ctx.http.source_ip.clone()
                    }
                    _ => None,
                })
        })
}

fn schema_mutation_gate_headers(event: &Request) -> SchemaMutationGateHeaders {
    SchemaMutationGateHeaders {
        node_public_key: header_str(event, HEADER_NODE_PUBLIC_KEY).map(str::to_string),
        node_signature: header_str(event, HEADER_NODE_SIGNATURE).map(str::to_string),
        challenge_id: header_str(event, HEADER_POW_CHALLENGE).map(str::to_string),
        nonce: header_str(event, HEADER_POW_NONCE).map(str::to_string),
        challenge_mac: header_str(event, HEADER_POW_CHALLENGE_MAC).map(str::to_string),
        difficulty_bits: header_str(event, HEADER_POW_DIFFICULTY_BITS).map(str::to_string),
        expires_at_unix_secs: header_str(event, HEADER_POW_EXPIRES_AT).map(str::to_string),
        counter: header_str(event, HEADER_POW_COUNTER).map(str::to_string),
        dev_pubkey: header_str(event, HEADER_DEV_PUBKEY).map(str::to_string),
    }
}

/// Standard 401 for missing/invalid keys.
fn unauthorized(reason: &str) -> Result<Response<Body>, Error> {
    json_response(
        401,
        &json!({
            "error": "Unauthorized",
            "detail": reason,
        }),
    )
}

/// `GET /v1/snapshot` — auth-gated registry export.
///
/// Validates the `X-API-Key` header against the deployment's key
/// DynamoDB table (via `api_key::validate_api_key`)
/// and, on success, returns a `SnapshotEnvelope` for the requesting
/// dev's hydration. The auth gate is enforced here on the Lambda only
/// — the actix dev binary leaves the route open for localhost.
///
/// In test builds the DynamoDB call is skipped (no AWS reach), and
/// any non-empty key is treated as valid; tests assert the header
/// extraction + 401 path explicitly.
#[allow(
    clippy::unused_async,
    reason = "Lambda handler — awaits are cfg-gated to `not(test)`, leaving the test build with no awaits in the body"
)]
async fn handle_snapshot_export(
    event: &Request,
    state: &SchemaServiceState,
) -> Result<Response<Body>, Error> {
    let api_key = match extract_api_key(event) {
        Some(k) => k.to_string(),
        None => {
            return unauthorized(
                "Missing or empty X-API-Key header. \
                 Get a key at https://www.exemem.com/developer.",
            );
        }
    };

    {
        let client = get_or_init_api_keys_client().await?;
        if let Err(reason) =
            api_key::validate_api_key(&client.ddb, &client.table_name, &api_key).await
        {
            tracing::warn!(
                target: "schema_service::auth",
                reason = %reason,
                "Snapshot fetch rejected"
            );
            return unauthorized(&reason);
        }
    }
    // The validated key is intentionally not logged: it's a credential.
    // Only the user_hash returned by validate_api_key would be safe to
    // log, but the snapshot export doesn't need it for any decision so
    // we drop it.
    let _ = api_key;

    match state.export_snapshot() {
        Ok(mut envelope) => {
            // Drop embeddings before serializing — they blow Lambda's
            // 6 MB sync-invoke response cap. Verified empirically on
            // 2026-05-05: prod has 1700 canonical-field embeddings of
            // 384-dim f32 vectors. Binary is ~2.6 MB, but JSON-encoded
            // (each f32 ~10-20 chars) lands at 15-25 MB. The Lambda
            // runtime drops oversize responses silently — Errors metric
            // stays at 0, but API Gateway returns the generic
            // {"message":"Internal Server Error"} body instead of the
            // handler's structured response.
            //
            // Dev nodes hydrating from this snapshot don't need the
            // embeddings: bootstrap-at-boot only registers schemas /
            // views / transforms into fold_db, and semantic-similarity
            // matching is a schema_service-side concern that doesn't
            // run on the dev path. If a consumer ever needs the
            // embeddings, expose them via a dedicated endpoint (e.g.
            // GET /v1/snapshot/embeddings backed by an S3 presigned
            // URL or paginated chunks) rather than inflating this
            // response.
            envelope.embeddings = schema_service_core::snapshot::SnapshotEmbeddings::default();
            let body = serde_json::to_value(envelope).map_err(serialization_error)?;
            json_response(200, &body)
        }
        Err(e) => json_response(
            500,
            &json!({"error": format!("Failed to export snapshot: {e}")}),
        ),
    }
}

/// `GET /v1/snapshot/shared-only` — auth-gated shared-surface projection.
///
/// Same X-API-Key gate and embedding strip as [`handle_snapshot_export`],
/// but only system-owned / explicit shared schemas are included. Used by
/// resolver-pack publishing so private legacy bootstrap rows never leave
/// the control plane as pack inputs.
#[allow(
    clippy::unused_async,
    reason = "Lambda handler — awaits are cfg-gated to `not(test)`, leaving the test build with no awaits in the body"
)]
async fn handle_snapshot_export_shared_only(
    event: &Request,
    state: &SchemaServiceState,
) -> Result<Response<Body>, Error> {
    let api_key = match extract_api_key(event) {
        Some(k) => k.to_string(),
        None => {
            return unauthorized(
                "Missing or empty X-API-Key header. \
                 Get a key at https://www.exemem.com/developer.",
            );
        }
    };

    {
        let client = get_or_init_api_keys_client().await?;
        if let Err(reason) =
            api_key::validate_api_key(&client.ddb, &client.table_name, &api_key).await
        {
            tracing::warn!(
                target: "schema_service::auth",
                reason = %reason,
                "Shared-only snapshot fetch rejected"
            );
            return unauthorized(&reason);
        }
    }
    let _ = api_key;

    match state.export_shared_only_snapshot() {
        Ok(mut envelope) => {
            envelope.embeddings = schema_service_core::snapshot::SnapshotEmbeddings::default();
            let body = serde_json::to_value(envelope).map_err(serialization_error)?;
            json_response(200, &body)
        }
        Err(e) => json_response(
            500,
            &json!({"error": format!("Failed to export shared-only snapshot: {e}")}),
        ),
    }
}

/// Returns an `http::response::Builder` pre-loaded with the response status,
/// Content-Type, and CORS preflight headers shared by every endpoint. Callers
/// chain `.body(...)` (and any extra headers like `Cache-Control`) on top.
///
/// The deployed HTTP API Gateway owns Access-Control-Allow-Origin from its
/// explicit allowlist; Lambda responses avoid duplicating that policy.
fn cors_builder(status: u16, content_type: &str) -> http::response::Builder {
    Response::builder()
        .status(status)
        .header("Content-Type", content_type)
        .header(
            "Access-Control-Allow-Methods",
            cors::ACCESS_CONTROL_ALLOW_METHODS,
        )
        .header(
            "Access-Control-Allow-Headers",
            cors::ACCESS_CONTROL_ALLOW_HEADERS,
        )
}

/// Map a `serde_json` serialization failure onto the `Error` the Lambda
/// runtime surfaces (a 500). Collapses the
/// `|e| Error::from(format!("Serialization error: {}", e))` closure
/// repeated at every handler that serializes a response body.
#[allow(
    clippy::needless_pass_by_value,
    reason = "used as `.map_err(serialization_error)` callback — map_err passes the error by value"
)]
fn serialization_error(e: serde_json::Error) -> Error {
    Error::from(format!("Serialization error: {e}"))
}

fn json_response(status: u16, body: &Value) -> Result<Response<Body>, Error> {
    cors_builder(status, "application/json")
        .body(Body::from(body.to_string()))
        .map_err(|e| Error::from(format!("Failed to build response: {e}")))
}

fn json_response_cacheable(status: u16, body: &Value, etag: &str) -> Result<Response<Body>, Error> {
    cors_builder(status, "application/json")
        .header("Cache-Control", "public, max-age=60")
        .header("ETag", etag)
        .body(Body::from(body.to_string()))
        .map_err(|e| Error::from(format!("Failed to build response: {e}")))
}

/// Extract a single query-string parameter by key. Pass the key with its
/// trailing `=` (e.g. `"source="`) to strip the prefix in one step.
/// Returns `None` when the parameter is absent.
fn query_param<'a>(event: &'a Request, key_eq: &str) -> Option<&'a str> {
    event
        .uri()
        .query()
        .and_then(|q| q.split('&').find_map(|p| p.strip_prefix(key_eq)))
}

/// Parse the optional `source=` query-string parameter for the
/// `/schemas/available` endpoint. Returns the matched `SchemaSource` variant,
/// or `None` when the param is absent. Unknown values are reported back so
/// callers can distinguish "no filter" from "filter that matched nothing".
fn parse_source_filter(event: &Request) -> (Option<schema_types::SchemaSource>, bool) {
    use schema_types::SchemaSource;
    match query_param(event, "source=") {
        None => (None, false),
        Some("system_seed") => (Some(SchemaSource::SystemSeed), false),
        Some("starter_seed") => (Some(SchemaSource::StarterSeed), false),
        Some("user") => (Some(SchemaSource::User), false),
        Some(_) => (None, true), // present but unknown
    }
}

/// Parse the `threshold` query-string parameter, defaulting to 0.5 when
/// absent or unparseable. Shared by `/schemas/similar/{name}` and
/// `/transforms/similar/{name}`.
fn parse_threshold(event: &Request) -> f64 {
    query_param(event, "threshold=")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.5)
}

/// Initialize the schema service state (once per cold start)
async fn get_or_init_state() -> Result<Arc<SchemaServiceState>, Error> {
    // S3 bucket for the four domain blobs + wasm prefix. Set by the
    // CDK stack; the Lambda refuses to start without it. We build the
    // S3 client and the persistence backend outside OnceCell so any
    // failure surfaces as a cold-start error rather than a swallowed
    // panic from inside the init closure.
    let bucket = env::var("SCHEMA_STORE_BUCKET").map_err(|_| {
        Error::from(
            "SCHEMA_STORE_BUCKET env var is not set. The schema service Lambda requires \
             an S3 bucket to persist schemas, canonical fields, apps, and near-misses. \
             See CDK: schema-stack.ts",
        )
    })?;
    let embeddings_table = env::var("SCHEMA_EMBEDDINGS_TABLE").map_err(|_| {
        Error::from(
            "SCHEMA_EMBEDDINGS_TABLE env var is not set. The schema service Lambda \
             persists fastembed vectors in DynamoDB so cold start doesn't recompute \
             them on every invocation. See CDK: schema-stack.ts (SchemaEmbeddingsTable).",
        )
    })?;
    let schema_store_endpoint_url = schema_store_endpoint_url_from_env();
    let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let schema_store_credentials = if schema_store_endpoint_url.is_some() {
        schema_store_s3_credentials_from_env().map_err(Error::from)?
    } else {
        None
    };
    let s3_config = schema_store_s3_config_builder(
        aws_sdk_s3::config::Builder::from(&aws_config),
        schema_store_endpoint_url.as_deref(),
        schema_store_credentials.as_ref(),
    );
    let s3_client = aws_sdk_s3::Client::from_conf(s3_config.build());
    let ddb_client = aws_sdk_dynamodb::Client::new(&aws_config);
    let schema_mutation_gate_store = schema_mutation_gate_quota_store_from_env(&ddb_client);
    let backend: Arc<dyn schema_service_core::ExternalSchemaPersistence> =
        Arc::new(s3_persistence::S3BlobPersistence::new(
            s3_client,
            bucket.clone(),
            ddb_client,
            embeddings_table,
        ));
    let embedder = default_embedder();

    SCHEMA_STATE
        .get_or_try_init(|| async move {
            tracing::info!(
                bucket = %bucket,
                endpoint_url = schema_store_endpoint_url.as_deref().unwrap_or("aws-default"),
                "Initializing schema service with S3-compatible blob storage",
            );

            let state = SchemaServiceState::new_with_external_and_schema_mutation_gate_store(
                backend,
                embedder,
                schema_mutation_gate_store,
            )
            .await
            .map_err(|e| {
                Error::from(format!(
                    "Failed to initialize schema service against S3-compatible bucket '{bucket}': {e}. \
                         Check that the bucket exists, the Lambda's IAM role has \
                         s3:GetObject and s3:PutObject on it (or the configured \
                         R2 token can read/write the bucket), and the domain blobs \
                         (schemas.json, canonical_fields.json, apps.json, near_misses.json) \
                         are either absent or valid JSON."
                ))
            })?;

            // App-identity verification config (app_identity v3.1, Lane
            // B2b): trusted exemem root pubkeys (APP_IDENTITY_ROOT_PUBKEYS,
            // wired from the KMS GetPublicKey output of the
            // exemem-app-identity-root key by exemem-infra), deployment env,
            // and the offline dev-pubkey revocation denylist. With no roots
            // configured, /v1/apps rejects every cert (401) and the
            // /v1/schemas owner_app_id gate is a passthrough.
            state.configure_app_identity(
                schema_service_core::app_identity::AppIdentityConfig::from_env(),
            );
            state.configure_schema_mutation_gate_from_env();

            // Seed the curated canonical field registry BEFORE seeding
            // schemas, so that schema field classification hits the
            // pre-populated entries on the first pass and skips the
            // Anthropic LLM round-trip per field.
            schema_service_core::builtin_canonical_fields::seed(&state)
                .await
                .map_err(|e| {
                    Error::from(format!(
                        "Failed to seed pre-populated canonical fields: {e}"
                    ))
                })?;

            // Schema.org types and property rows are not live language
            // (preference-schema-org-not-live-language). Do not call
            // schema_org_seeds::seed here.

            // Seed service-owned built-in schemas. Idempotent:
            // already-present schemas are skipped by identity_hash match.
            schema_service_server_shared::builtin_schemas::seed(&state)
                .await
                .map_err(|e| Error::from(format!("Failed to seed built-in schemas: {e}.")))?;

            tracing::info!(
                "Schema service initialized and built-ins seeded ({} descriptive names)",
                schema_service_server_shared::builtin_schemas::PHASE_1_DESCRIPTIVE_NAMES.len()
            );
            Ok(Arc::new(state))
        })
        .await
        .cloned()
}

fn schema_mutation_gate_quota_table_from_env() -> Option<String> {
    env_var_trimmed(SCHEMA_MUTATION_GATE_QUOTA_TABLE_ENV)
}

fn schema_store_endpoint_url_from_env() -> Option<String> {
    env_var_trimmed(SCHEMA_STORE_ENDPOINT_URL_ENV)
        .or_else(|| env_var_trimmed(SCHEMA_STORE_R2_ENDPOINT_ENV))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SchemaStoreS3Credentials {
    access_key_id: String,
    secret_access_key: String,
    region: String,
}

fn schema_store_s3_credentials_from_env() -> Result<Option<SchemaStoreS3Credentials>, String> {
    let access_key_id = env_var_trimmed(SCHEMA_STORE_ACCESS_KEY_ID_ENV);
    let secret_access_key = env_var_trimmed(SCHEMA_STORE_SECRET_ACCESS_KEY_ENV);
    match (access_key_id, secret_access_key) {
        (None, None) => Ok(None),
        (Some(access_key_id), Some(secret_access_key)) => Ok(Some(SchemaStoreS3Credentials {
            access_key_id,
            secret_access_key,
            region: env_var_trimmed(SCHEMA_STORE_REGION_ENV)
                .unwrap_or_else(|| SCHEMA_STORE_DEFAULT_REGION.to_string()),
        })),
        _ => Err(format!(
            "{SCHEMA_STORE_ACCESS_KEY_ID_ENV} and {SCHEMA_STORE_SECRET_ACCESS_KEY_ENV} must both be set for S3-compatible schema-store credentials"
        )),
    }
}

fn schema_store_s3_config_builder(
    mut builder: aws_sdk_s3::config::Builder,
    endpoint_url: Option<&str>,
    credentials: Option<&SchemaStoreS3Credentials>,
) -> aws_sdk_s3::config::Builder {
    if let Some(endpoint_url) = endpoint_url {
        builder = builder.endpoint_url(endpoint_url).force_path_style(true);
        if let Some(credentials) = credentials {
            let s3_credentials = aws_sdk_s3::config::Credentials::new(
                &credentials.access_key_id,
                &credentials.secret_access_key,
                None,
                None,
                "schema-store-s3-compatible",
            );
            builder = builder
                .region(aws_sdk_s3::config::Region::new(credentials.region.clone()))
                .credentials_provider(s3_credentials);
        }
    }
    builder
}

fn env_var_trimmed(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn schema_mutation_gate_quota_store_from_env(
    ddb_client: &aws_sdk_dynamodb::Client,
) -> schema_service_core::SchemaMutationGateStore {
    schema_mutation_gate_quota_table_from_env().map_or_else(
        schema_service_core::SchemaMutationGateStore::default,
        |table| {
            s3_persistence::DynamoDbSchemaMutationGateStore::new(ddb_client.clone(), table)
                .into_schema_mutation_gate_store()
        },
    )
}

#[cfg(feature = "fastembed")]
fn default_embedder() -> Arc<dyn schema_service_server_shared::Embedder> {
    Arc::new(schema_service_server_shared::FoldDbFastEmbedder::from_lambda_layer())
}

#[cfg(not(feature = "fastembed"))]
fn default_embedder() -> Arc<dyn schema_service_server_shared::Embedder> {
    Arc::new(schema_service_core::DisabledEmbeddingModel)
}

fn get_schema_by_name(
    state: &SchemaServiceState,
    schema_name: &str,
) -> Result<Response<Body>, Error> {
    match state.get_schema_by_name(schema_name) {
        Ok(Some(schema)) => {
            let system = state.is_system_schema(&schema.name);
            let envelope = SchemaEnvelope { schema, system };
            let body = serde_json::to_value(envelope).map_err(serialization_error)?;
            json_response(200, &body)
        }
        Ok(None) => json_response(404, &json!({"error": "Schema not found"})),
        Err(e) => json_response(500, &json!({"error": format!("Failed to get schema: {e}")})),
    }
}

fn parse_body(event: &Request) -> Result<String, Response<Body>> {
    match event.body() {
        Body::Text(s) => Ok(s.clone()),
        Body::Binary(b) => Ok(String::from_utf8_lossy(b).into_owned()),
        Body::Empty => Err(json_response(400, &json!({"error": "Request body is empty"})).unwrap()),
    }
}

// ─── `/v2` route helpers ──────────────────────────────────────────────────
//
// The `/v2` arms repeat the same three preludes — parse the body, require
// the DevCert header pair, split an app id or an (app id, channel) pair out
// of the path. Naming them keeps each arm to its own logic.

/// The parsed JSON body, or the 400 to return.
fn v2_body(event: &Request) -> Result<Value, Result<Response<Body>, Error>> {
    let raw = match parse_body(event) {
        Ok(b) => b,
        Err(r) => return Err(Ok(r)),
    };
    serde_json::from_str(&raw).map_err(|e| {
        json_response(
            400,
            &json!({"reason": "invalid_manifest", "detail": format!("invalid JSON: {e}")}),
        )
    })
}

/// The `X-Exemem-Dev-Cert` + `X-Signature` pair, or the 401 to return.
/// Every `/v2` write needs both; a read needs neither.
fn v2_cert_headers(event: &Request) -> Result<(&str, &str), Result<Response<Body>, Error>> {
    match (
        header_str(event, "x-exemem-dev-cert"),
        header_str(event, "x-signature"),
    ) {
        (Some(cert), Some(sig)) => Ok((cert, sig)),
        _ => Err(json_response(
            401,
            &json!({"reason": "cert_invalid", "detail": "missing X-Exemem-Dev-Cert or X-Signature header"}),
        )),
    }
}

/// A path segment that must be exactly one app id — non-empty and with no
/// embedded slash, so a crafted path cannot smuggle a second segment in.
fn v2_app_id(segment: &str) -> Option<&str> {
    let app_id = segment.trim_end_matches('/');
    (!app_id.is_empty() && !app_id.contains('/')).then_some(app_id)
}

/// Split `/v2/apps/{app_id}/channels/{channel}` into its two ids.
fn v2_channel_path(path: &str) -> Option<(&str, &str)> {
    let rest = path.strip_prefix("/v2/apps/")?;
    let (app_id, channel) = rest.split_once("/channels/")?;
    let channel = channel.trim_end_matches('/');
    if app_id.is_empty() || app_id.contains('/') || channel.is_empty() || channel.contains('/') {
        return None;
    }
    Some((app_id, channel))
}

/// Dispatch a request without touching the cold-start state. Extracted
/// so the smoke test can exercise the routing table directly.
fn dispatch_stateless(method: &str, path: &str) -> Option<Result<Response<Body>, Error>> {
    match (method, path) {
        // Health check — versioned and unversioned. The unversioned
        // /health path is retained intentionally: API Gateway health
        // probes and the schema-infra CDK cutover (PR 4/5) both still
        // address `/health` during the deploy-transition window.
        ("GET", "/health" | "/v1/health") => Some(json_response(
            200,
            &json!({
                "status": "healthy",
                "service": "schema-service"
            }),
        )),

        // Root endpoint — lists the supported routes. Kept here (rather
        // than under /v1/) so operators probing the Lambda's API
        // Gateway root see the service identity without needing to
        // know the version prefix.
        ("GET" | "POST", "/") => Some(json_response(
            200,
            &json!({
                "service": "FoldDB Schema & View Registry",
                "version": "2.0.0",
                "endpoints": {
                    "GET /health": "Health check (unversioned alias)",
                    "GET /v1/health": "Health check",
                    "GET /v1/schemas": "List schema names",
                    "GET /v1/registry/index": "Signed compact registry index for local dedup caches",
                    "GET /v1/schemas/available": "Get all schemas with definitions",
                    "GET /v1/schemas/similar/{name}?threshold=0.5": "Find similar schemas",
                    "GET /v1/schema/{name}": "Get specific schema",
                    "POST /v1/schemas": "Add new schema",
                    "POST /v1/schemas/mutation-challenge": "Issue a short-lived node-key proof-of-work challenge for shared schema mutation",
                    "POST /v1/apps": "Register an app namespace as sandbox (dev cert + signature)",
                    "GET /v1/apps": "Browse promoted, non-revoked apps (public shelf)",
                    "GET /v1/apps/{app_id}": "Read a registered app incl. tier (public; lookup-by-known-id)",
                    "PUT /v1/apps/{app_id}": "Update app metadata (owner-only; display_name immutable)",
                    "POST /v1/apps/{app_id}/promote": "Promote a sandbox app to live (owner-only; authorized_publisher required)",
                    "POST /v1/schemas/batch-check-reuse": "Batch lookup of reusable schemas by descriptive name + field list",
                    "POST /v1/schemas/resolve": "Stateless read-only schema dedup for cache misses",
                    "POST /v1/schemas/reload": "Reload schemas from storage",
                    "GET /v1/canonicalization-near-misses": "Shadow-mode audit log of single-signal vs dual-signal canonicalization disagreements",
                    "GET /v1/snapshot": "Export the registry as a single JSON envelope (requires X-API-Key)",
                    "GET /v1/snapshot/shared-only": "Export shared-only projection for resolver packs (requires X-API-Key)"
                }
            }),
        )),
        _ => None,
    }
}

async fn function_handler(event: Request) -> Result<Response<Body>, Error> {
    let method = event.method().as_str();
    let path = event.uri().path();

    tracing::info!("Request: {} {}", method, path);

    // Stateless routes (health, root) bypass cold-start init so health
    // probes stay fast and don't force full state hydration.
    if let Some(resp) = dispatch_stateless(method, path) {
        return resp;
    }

    let state = get_or_init_state().await?;
    dispatch_with_state(state.as_ref(), event).await
}

/// Dispatch a request against an already-initialized state. Factored out of
/// `function_handler` so tests can exercise every stateful route with an
/// in-memory `SchemaServiceState` (no AWS SDK, no S3, no Secrets Manager).
async fn dispatch_with_state(
    state: &SchemaServiceState,
    event: Request,
) -> Result<Response<Body>, Error> {
    // Copy the method/path before we borrow the event for body + query, so
    // the match can still move through the rest of `event` without fighting
    // the borrow checker.
    let method = event.method().as_str().to_owned();
    let path = event.uri().path().to_owned();

    match (method.as_str(), path.as_str()) {
        // ============== Schema Endpoints ==============
        ("GET", "/v1/schemas") => match state.get_schema_names() {
            Ok(schema_names) => json_response(200, &json!({ "schemas": schema_names })),
            Err(e) => json_response(
                500,
                &json!({"error": format!("Failed to get schema names: {e}")}),
            ),
        },

        ("GET", "/v1/registry/index") => routes::schemas::get_registry_index(state, &event),

        ("GET", "/v1/schemas/available") => routes::schemas::get_schemas_available(state, &event),

        (m, p) if m == "GET" && p.starts_with("/v1/schemas/similar/") => {
            routes::schemas::get_schemas_similar(state, &event, p)
        }

        (m, p) if m == "GET" && (p.starts_with("/v1/schema/") || p.starts_with("/v1/schemas/")) => {
            routes::schemas::get_schema(state, p)
        }

        ("POST", "/v1/schemas/mutation-challenge") => {
            routes::schemas::post_schemas_mutation_challenge(state, &event)
        }

        ("POST", "/v1/schemas") => routes::schemas::post_schemas(state, &event).await,

        ("POST", "/v1/apps") => routes::apps::post_app_v1(state, &event).await,

        // Public promoted-app shelf. This mirrors the actix route's
        // shared handler behavior: anonymous browsing is allowed, but
        // only Live, non-revoked apps appear.
        ("GET", "/v1/apps") => json_response(200, &json!(state.list_live_apps())),

        (m, p) if m == "GET" && p.starts_with("/v1/apps/") => routes::apps::get_app_v1(state, p),

        (m, p) if m == "PUT" && p.starts_with("/v1/apps/") => {
            routes::apps::put_app_v1(state, &event, p).await
        }

        (m, p) if m == "POST" && p.starts_with("/v1/apps/") && p.ends_with("/promote") => {
            routes::apps::promote_app_v1(state, &event, p).await
        }

        ("POST", "/v2/apps") => routes::apps::post_apps_v2(state, &event).await,

        (m, p) if m == "POST" && p.starts_with("/v2/apps/") && p.ends_with("/releases") => {
            routes::apps::post_apps_releases(state, &event, p).await
        }

        (m, p) if m == "POST" && p.starts_with("/v2/apps/") && p.ends_with("/revocations") => {
            routes::apps::post_apps_revocations(state, &event, p).await
        }

        (m, p) if m == "PUT" && p.starts_with("/v2/apps/") && p.contains("/channels/") => {
            routes::apps::put_apps_channels(state, &event, p).await
        }

        (m, p) if m == "GET" && p.starts_with("/v2/apps/") && p.contains("/channels/") => {
            routes::apps::get_apps_channels(state, p)
        }

        (m, p) if m == "GET" && p.starts_with("/v2/releases/") => {
            routes::apps::get_releases(state, p)
        }

        (m, p) if m == "GET" && p.starts_with("/v2/apps/") => routes::apps::get_app_v2(state, p),

        ("POST", "/v1/fields/declare") => routes::fields::post_fields_declare(state, &event).await,

        (m, p) if m == "GET" && p.starts_with("/v1/fields/") => {
            routes::fields::get_fields(state, p)
        }

        ("POST", "/v1/schemas/batch-check-reuse") => {
            routes::schemas::post_schemas_batch_check_reuse(state, &event)
        }

        ("POST", "/v1/debug/field-match-probe") => {
            routes::admin::post_debug_field_match_probe(state, &event)
        }

        ("POST", "/v1/schemas/resolve") => routes::schemas::post_schemas_resolve(state, &event),

        ("POST", "/v1/schemas/reload") => match state.load_schemas().await {
            Ok(_) => {
                let count = state.get_schema_count();
                json_response(
                    200,
                    &json!({
                        "ok": true,
                        "count": count,
                        "message": format!("Reloaded {count} schemas")
                    }),
                )
            }
            Err(e) => json_response(500, &json!({"error": format!("Failed to reload: {e}")})),
        },

        // ============== View Endpoints ==============
        ("POST", "/v1/admin/warm-embeddings") => {
            routes::admin::post_admin_warm_embeddings(state).await
        }

        ("POST", "/v1/admin/dedupe-descriptive-names") => {
            routes::admin::post_admin_dedupe_descriptive_names(state).await
        }

        ("POST", "/v1/admin/deprecate-schemas") => {
            routes::admin::post_admin_deprecate_schemas(state, &event).await
        }

        ("GET", "/v1/admin/schema-match-telemetry") => {
            routes::admin::get_admin_schema_match_telemetry(state)
        }

        // ============== Snapshot ==============
        //
        // `GET /v1/snapshot` is auth-gated by X-API-Key (validated
        // against the exemem ApiKeys DynamoDB table). The dev-only
        // `POST /v1/snapshot/import` route is intentionally NOT
        // mounted here — Lambda storage is `External` (S3-backed) and
        // the importer is Sled-only. See
        // `projects/schema-service-dev-hydration` for the design.
        ("GET", "/v1/snapshot") => handle_snapshot_export(&event, state).await,
        ("GET", "/v1/snapshot/shared-only") => {
            routes::admin::get_snapshot_shared_only(state, &event).await
        }

        ("GET", "/v1/canonicalization-near-misses") => {
            routes::admin::get_canonicalization_near_misses(state, &event)
        }

        _ => json_response(
            404,
            &json!({"error": format!("Not found: {} {}", method, path)}),
        ),
    }
}

mod api_key;
mod cors;
mod routes;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Install the shared observability stack: redacting JSON FMT to stdout
    // (captured into CloudWatch by the Lambda runtime) plus the env-gated
    // Sentry ERROR layer. Sentry is additive and a no-op when OBS_SENTRY_DSN
    // is unset, so CloudWatch JSON logging is unchanged where it isn't
    // configured. The guard is held for the whole `main` body — including
    // the long cold-start init below and the `run(...)` await — so the
    // Sentry flush + FMT worker survive until the process exits.
    let _obs = observability::init_lambda("schema_service", env!("CARGO_PKG_VERSION"))
        .map_err(|e| Error::from(format!("failed to install observability: {e}")))?;

    tracing::info!("Schema service Lambda starting...");

    #[cfg(feature = "fastembed")]
    {
        // Point fastembed at the bundled model cache and refuse to hit
        // HuggingFace. AWS Lambda mounts the fastembed Layer at /opt/.
        if std::env::var_os("FASTEMBED_CACHE_DIR").is_none() {
            std::env::set_var("FASTEMBED_CACHE_DIR", "/opt/fastembed_cache");
        }
        if std::env::var_os("HF_HUB_OFFLINE").is_none() {
            std::env::set_var("HF_HUB_OFFLINE", "1");
        }
    }

    // Do the expensive state initialization (S3 hydration + built-in
    // seeding) during Lambda init, not on the first handler invocation.
    // Lambda's init phase is NOT bounded by API Gateway's 29s timeout.
    if let Err(e) = get_or_init_state().await {
        tracing::error!("Schema service init failed during Lambda init phase: {e}");
        return Err(e);
    }
    tracing::info!("Schema service init complete during Lambda init phase");

    run(service_fn(function_handler)).await
}
