//! schema_service_client
//!
//! Typed HTTP client for the schema service `/v1/*` API. Ported during
//! Phase 3 of `projects/extract-schema-service-repo` from
//! `fold_db_node/src/fold_node/schema_client.rs` so consumers (fold_db_node,
//! future tools) depend on this crate instead of hand-rolling wire types.
//!
//! The client talks to the actix dev binary (`schema_service_server_http`)
//! during local development and to the deployed Lambda
//! (`schema_service_server_lambda` behind API Gateway) in dev/prod — both
//! mount the same shared handlers under `/v1/*`.
//!
//! Retries: connect errors, timeouts, and 5xx responses retry up to 3 times
//! with exponential backoff (250ms, 1s, 4s). 4xx responses and JSON
//! deserialization failures fail fast on the first attempt.
//!
//! Resolver-pack fetch: [`resolver_pack_http::HttpResolverPackStore`] implements
//! the core [`schema_service_core::ResolverPackObjectStore`] over HTTPS (or
//! localhost HTTP for tests). Product call-path activation is intentionally
//! not wired here (see local schema resolver engineering plan PR 3/4).

pub mod local_first_resolver;
pub mod resolver_pack_http;

pub use local_first_resolver::{
    classify_disagreement, evaluate_local_proposal, field_context_text,
    map_resolver_output_to_resolve_result, match_live_result, pack_to_registry_metadata,
    proposal_field_id, DisagreementClass, FacadePath, FacadeProposal, FacadeResolveItem,
    LiveSchemaGateway, LocalEvaluateError, LocalEvaluateOutcome, LocalFirstMode,
    LocalFirstSchemaResolver, LocalShadowSummary, NoopPackStore, SharedSurfaceAttachmentRecord,
    SharedSurfacePublishOutcome, SharedSurfacePublishRequest, SharedSurfacePublishResult,
};
pub use resolver_pack_http::{content_length_exceeds_cap, join_object_url, HttpResolverPackStore};

use app_identity_crypto::{
    compute_payload_hash, key_id, sign_envelope, Env, Purpose, SignatureEnvelope, SigningKey,
    ALG_ED25519, ENVELOPE_VERSION,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chrono::Utc;
use reqwest::StatusCode;
use schema_types::{FoldDbError, FoldDbResult, Schema};
use serde::Deserialize;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// Connection-establishment budget shared by schema-service HTTP clients.
///
/// Keep this below the caller-visible request budgets (20 seconds for Mini's
/// owner-socket schema path and the live probe) so a black-holed endpoint
/// returns control before the whole operation expires.
pub const SCHEMA_SERVICE_HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

use schema_service_core::registry_index::RegistryIndexEnvelope;
use schema_service_core::snapshot::{
    AppArtifact, AppCodeSignature, AppMetadata, AppRecord, AppTier, SnapshotEnvelope,
    SnapshotImportReport,
};
use schema_service_core::types::{
    AddSchemaRequest, AddSchemaResponse, BatchSchemaReuseRequest, BatchSchemaReuseResponse,
    DescriptiveNameConflict, SchemaEnvelope, SchemaLookupEntry, SchemaResolveProposal,
    SchemaResolveRequest, SchemaResolveResponse,
};
use schema_service_core::{
    node_signature_payload, pow_satisfies, schema_payload_hash, SchemaMutationChallengeRequest,
    SchemaMutationChallengeResponse, SharedSurfaceMetadata, HEADER_NODE_PUBLIC_KEY,
    HEADER_NODE_SIGNATURE, HEADER_POW_CHALLENGE, HEADER_POW_CHALLENGE_MAC, HEADER_POW_COUNTER,
    HEADER_POW_DIFFICULTY_BITS, HEADER_POW_EXPIRES_AT, HEADER_POW_NONCE,
};

pub use schema_service_core::types;
pub use schema_service_core::{
    validate_shared_surface_request, DisabledEmbeddingModel, SharedSurfacePublishAttachRequest,
    SharedSurfaceValidationError,
};

/// Thin re-export so Mini can validate without a direct core dependency.
pub fn validate_shared_surface_request_pub(
    request: &SharedSurfacePublishAttachRequest,
) -> Result<(), String> {
    validate_shared_surface_request(request).map_err(|e| e.to_string())
}

/// `GET /v1/apps/{app_id}` response. The persisted [`AppRecord`] fields
/// plus a derived `revoked` flag (true when the owner's dev pubkey is on
/// the schema service's offline denylist).
///
/// Public lookup-by-known-id, no auth — see
/// `exemem-workspace/docs/designs/app_identity_node_as_verifier.md` Step 1.
///
/// [`AppRecord`]: schema_service_core::snapshot::AppRecord
#[derive(Debug, Clone, Deserialize)]
pub struct AppLookup {
    pub app_id: String,
    pub owner_dev_pubkey: String,
    pub metadata: AppMetadata,
    /// Latest published SemVer. Older schema_service deployments/records omit
    /// this and deserialize as `0.0.0`.
    #[serde(default = "schema_service_core::snapshot::default_app_version")]
    pub version: String,
    pub registered_at: String,
    /// Lifecycle tier (`sandbox` / `live`). Defaults to `live` when an
    /// older schema_service omits the field, matching the registry's own
    /// back-compat default.
    #[serde(default)]
    pub tier: AppTier,
    /// macOS code-signature requirement declared in the app's manifest at
    /// publish time (app-isolation I3b). `None` when the app has not
    /// declared one, or when an older schema_service omits the field.
    #[serde(default)]
    pub code_signature: Option<AppCodeSignature>,
    /// Source checkout pointer for source-first app installs.
    #[serde(default)]
    pub source: Option<String>,
    /// Signed release tarball pointer.
    #[serde(default)]
    pub artifact: Option<AppArtifact>,
    /// Cross-app schemas/outputs the app declares it consumes
    /// ([`AppRecord::uses`] — transform-system-epic Phase E). Empty when the
    /// app declares none, or when an older schema_service omits the field.
    #[serde(default)]
    pub uses: Vec<String>,
    pub revoked: bool,
}

/// Outcome of `POST /v1/schemas`. Either the server accepted the proposal
/// (200 with `AlreadyExists`-shaped body, or 201 with `Added` / `Expanded`)
/// or it refused with a 409 because the `descriptive_name` is already bound
/// to a different active canonical. Returned from
/// [`SchemaServiceClient::add_schema_typed`].
///
/// `Accepted` boxes its `AddSchemaResponse` for the same reason
/// the response carries a full `Schema` (~1KB) while `Conflict` is only ~72
/// bytes; boxing keeps the enum variants size-balanced and satisfies
/// `clippy::large_enum_variant`.
#[derive(Debug, Clone)]
pub enum AddSchemaOutcome {
    /// 2xx — the request was accepted. Includes the resulting Schema,
    /// mutation mappers, and (on expansion) the replaced schema name.
    Accepted(Box<AddSchemaResponse>),
    /// 409 — an Approved schema with this `descriptive_name` already
    /// exists and the server refused to silently merge. The caller should
    /// surface this to the user with a rename / reuse prompt; retrying the
    /// same proposal will hit the same conflict.
    Conflict(DescriptiveNameConflict),
}

mod publish_auth;
mod retry;
use publish_auth::*;
use retry::*;

mod client_add_schema;
mod client_pow;
mod client_reads;
mod client_snapshot;

pub struct DevSchemaClaim<'a> {
    pub cert_b64: &'a str,
    pub dev_key: &'a SigningKey,
    pub env: Env,
}

struct AddSchemaOptions<'a> {
    schema_match_source: &'a str,
    fallback_reason: Option<&'a str>,
    offer_to_shared_discovery: bool,
    shared_surface: Option<SharedSurfaceMetadata>,
    schema_claim_auth: Option<SchemaClaimAuth>,
}

/// Client for communicating with the schema service `/v1/*` API.
#[derive(Clone)]
pub struct SchemaServiceClient {
    base_url: String,
    client: reqwest::Client,
    node_identity: Option<NodePublishIdentity>,
}

impl SchemaServiceClient {
    /// Create a new schema service client.
    ///
    /// `schema_service_url` should be the base URL (no trailing `/v1`) —
    /// for example `http://127.0.0.1:9102` for local dev, or the dev/prod
    /// API Gateway hostname resolved by the caller. fold_db_node owns the
    /// canonical URL registry (`environments.json` in that repo); do not
    /// hardcode the gateway hostname here.
    pub fn new(schema_service_url: &str) -> Self {
        // Timeout headroom: schema creation can involve LLM field
        // classification (Anthropic Haiku / Ollama) that takes 5–60s per
        // field under load.
        Self::new_with_timeout(schema_service_url, Duration::from_secs(120))
    }

    /// Create a client with an explicit total request timeout.
    ///
    /// Mini owner-socket handlers use this for direct declare so live schema
    /// service stalls return before the daemon's UDS handler deadline expires.
    pub fn new_with_timeout(schema_service_url: &str, timeout: Duration) -> Self {
        // trace-egress: propagate (schema_service /v1/* API; .send() callers wrap with observability::propagation::inject_w3c)
        let client = Self::build_http_client(timeout);
        Self {
            base_url: schema_service_url.trim_end_matches('/').to_string(),
            client,
            node_identity: None,
        }
    }

    fn build_http_client(timeout: Duration) -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(SCHEMA_SERVICE_HTTP_CONNECT_TIMEOUT.min(timeout))
            .no_proxy()
            .build()
            // trace-egress: propagate (fallback path; same target as builder above)
            .unwrap_or_else(|_| reqwest::Client::new())
    }

    /// Return a client that can answer the schema mutation PoW gate with this
    /// node's Ed25519 identity. Read-only calls ignore the identity.
    pub fn with_node_identity(mut self, signing_key: SigningKey, env: Env) -> Self {
        self.node_identity = Some(NodePublishIdentity::new(signing_key, env));
        self
    }
}
