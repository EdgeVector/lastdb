use schema_types::DataClassification;
use schema_types::FieldValueType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use schema_types::Schema;

use crate::schema_resolver_abi::FieldCoverageEvidence;
use crate::shared_surface::SharedSurfaceMetadata;
use crate::state_compositional::ComponentReuseAdvice;

/// A canonical field entry in the global field registry.
/// Carries description (for semantic matching), type (for enforcement),
/// optional data classification (for sensitivity labeling), and optional
/// interest category (for discovery/social features).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalField {
    pub description: String,
    pub field_type: FieldValueType,
    /// Field identity version (name+description+type+version → field_hash).
    /// Defaults to 1 for legacy registry rows.
    #[serde(default = "default_field_version")]
    pub version: u32,
    /// Data classification label for this field. `None` for legacy fields
    /// that were registered before classification was required.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classification: Option<DataClassification>,
    /// Interest category for discovery (e.g. "Photography", "Cooking", "Running").
    /// Assigned by LLM at field registration time. `None` for fields that don't
    /// map to a user interest (e.g. content_hash, source, id fields).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interest_category: Option<String>,
}

fn default_field_version() -> u32 {
    1
}

impl CanonicalField {
    /// Schema Service field identity hash for this registry entry under `name`.
    #[must_use]
    pub fn field_hash(&self, name: &str) -> String {
        schema_types::compute_field_hash(name, &self.description, &self.field_type, self.version)
    }
}

/// Response containing a list of available schema names
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemasListResponse {
    pub schemas: Vec<String>,
}

/// A schema plus its system/user classification, as returned over
/// `/v1/*` endpoints that surface schema definitions.
///
/// `system = true` marks infrastructure schemas that the schema
/// service ships with itself (`Fingerprint`, `Edge`, `Identity`,
/// `Persona`, …). Everything a client has proposed is
/// `system = false`. UIs (notably fold_db_node's schema list) use
/// the flag to group or hide the built-ins so users only see their
/// own data-bearing schemas by default.
///
/// Serialized with `#[serde(flatten)]` on `schema`, so the JSON
/// layout is the Schema's own fields plus a top-level `system`
/// boolean — existing clients that deserialize into `Schema` keep
/// working (they just ignore `system`), and new clients read the
/// flag without another HTTP round-trip.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaEnvelope {
    #[serde(flatten)]
    pub schema: Schema,
    pub system: bool,
}

/// Response containing all available schemas with their definitions
/// plus the system/user classification per schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvailableSchemasResponse {
    pub schemas: Vec<SchemaEnvelope>,
}

/// Query string for `GET /v1/canonicalization-near-misses`. All four
/// parameters are optional. `since`/`until` are RFC 3339 timestamps
/// applied to `NearMissRecord::timestamp`; `limit`/`offset` paginate the
/// filtered set, newest-first.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NearMissesQuery {
    /// Inclusive lower bound on record timestamp (RFC 3339).
    pub since: Option<String>,
    /// Exclusive upper bound on record timestamp (RFC 3339).
    pub until: Option<String>,
    /// Page size. Defaults to 100, capped at 1000 server-side.
    pub limit: Option<usize>,
    /// Starting offset into the filtered, newest-first record list.
    /// Defaults to 0.
    pub offset: Option<usize>,
}

/// Response body for `GET /v1/canonicalization-near-misses`. `total` is
/// the count of records matching the time-range filter (pre-pagination);
/// `next_offset` is `Some(offset + limit)` when more records remain or
/// `None` when the caller has reached the end of the filtered set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NearMissesResponse {
    pub near_misses: Vec<crate::near_miss::NearMissRecord>,
    pub total: usize,
    pub next_offset: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchemaAddOutcome {
    Added(Schema, HashMap<String, String>), // Schema and mutation_mappers
    AlreadyExists(Schema, HashMap<String, String>), // Exact same identity hash + mappers from canonicalization
    /// Existing schema was expanded with new fields (old schema name, expanded schema, mappers)
    Expanded(String, Schema, HashMap<String, String>),
    /// An Approved schema with the same `descriptive_name` already exists and
    /// the incoming proposal cannot be cleanly expanded into it (e.g. different
    /// `schema_type`, which would corrupt molecule reads via field-mapper
    /// carry-over — see PR #923). The caller must rename the new schema or
    /// adopt the existing canonical instead of registering a duplicate.
    DescriptiveNameConflict(DescriptiveNameConflict),
    /// The proposal was decomposed into nested components and at least one
    /// component reused an EXISTING canonical via a typed `SchemaRef`
    /// (`ref_fields`) instead of re-inlining or spawning a near-duplicate. The
    /// composed canonical references the reused sub-schemas; the residual
    /// (un-reused) part registered normally. Carries the composed schema, the
    /// mutation mappers, and the per-component reuse advice that drove the
    /// composition (`field → reused canonical descriptive_name`).
    ///
    /// **Additive + gated.** This variant is only ever produced once the
    /// compositional *apply* step is enabled (a follow-on to the advisory
    /// shadow pass — off by default). With compositional decomposition off,
    /// or in advisory-only mode, the add-schema path never returns `Composed`;
    /// components below the reuse threshold stay inline, so the default
    /// behavior is a pure superset of today's whole-schema canonicalization.
    /// See `state_compositional` for the shadow-first rollout.
    Composed(Schema, HashMap<String, String>, Vec<ComponentReuseAdvice>),
}

/// Body of `SchemaAddOutcome::DescriptiveNameConflict`. Also serialized
/// verbatim as the HTTP 409 body so clients can drive UI prompts ("rename
/// your schema" / "reuse the existing one") off the same hash.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescriptiveNameConflict {
    /// `identity_hash` of the existing Approved schema currently bound to
    /// `descriptive_name`.
    pub existing_canonical: String,
    /// The conflicting human-readable name.
    pub descriptive_name: String,
    /// One-line reason the server refused to expand. Stable string for log
    /// grep and UI copy fallback.
    pub reason: String,
}

/// One entry in the `POST /v1/admin/dedupe-descriptive-names` response.
/// Reports a `descriptive_name` that had multiple active schemas before the
/// dedupe ran, names the survivor (largest field set wins), and lists the
/// hashes now marked `superseded_by` the survivor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescriptiveNameDedupeGroup {
    /// The colliding human-readable name.
    pub descriptive_name: String,
    /// `identity_hash` of the schema kept as the active canonical.
    pub survivor: String,
    /// Field count on the survivor — useful for an operator scanning the
    /// audit log to confirm "largest field set wins" picked the right one.
    pub survivor_field_count: usize,
    /// `identity_hash`es of the schemas that were marked superseded by the
    /// survivor. Their data isn't deleted — `resolve_active_schema` will
    /// redirect future lookups to `survivor`.
    pub superseded: Vec<String>,
}

/// Request body for `POST /v1/admin/deprecate-schemas`.
///
/// Accepts direct schema identity hashes and/or descriptive names. Descriptive
/// names are scoped to `owner_app_id` when provided; otherwise they target the
/// legacy/unowned namespace.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeprecateSchemasRequest {
    #[serde(default)]
    pub schema_names: Vec<String>,
    #[serde(default)]
    pub descriptive_names: Vec<String>,
    #[serde(default)]
    pub owner_app_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeprecatedSchemaEntry {
    pub schema_name: String,
    pub descriptive_name: Option<String>,
    pub already_deprecated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeprecateSchemasResponse {
    pub deprecated: Vec<DeprecatedSchemaEntry>,
    pub not_found: Vec<String>,
}

/// Error response structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}

/// Request structure for adding a schema with mutation mappers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddSchemaRequest {
    pub schema: Schema,
    pub mutation_mappers: HashMap<String, String>,
    /// Low-cardinality caller/source label for schema-match telemetry.
    ///
    /// `direct` is the default for legacy clients. Local nodes that already
    /// ran their offline matcher before falling back to the live service send
    /// `local_matcher_fallback`, which lets operators separate local misses
    /// from first-party direct callers without high-cardinality labels.
    #[serde(default = "default_schema_match_source")]
    pub schema_match_source: String,
    /// Low-cardinality fallback reason when `schema_match_source` is
    /// `local_matcher_fallback`. Free-form user/schema details are forbidden;
    /// clients should use stable enum-like strings such as
    /// `no_candidate_schema` or `insufficient_field_coverage`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<String>,
    /// Whether this registration OFFERS the schema into the shared
    /// registry's discovery (writing to the shared commons) vs merely
    /// CLAIMS a local namespace.
    ///
    /// Per `designs-local-first-app-namespacing`, a local namespace claim
    /// (the default, `false`) is cert-free: the node already computes the
    /// deterministic `identity_hash`, so claiming records ownership of a
    /// content-addressed namespace with no DevCert. Only an explicit offer
    /// into shared discovery (`true`) is gated on the owner's DevCert. A
    /// fresh `fbrain init` / `fkanban init` and every node-side
    /// registration leave this `false`, so they never hit `cert_required`.
    ///
    /// `#[serde(default)]` keeps the wire backward-compatible: an older
    /// client that omits the field is treated as a local claim.
    #[serde(default)]
    pub offer_to_shared_discovery: bool,
    /// Optional explicit shared-surface envelope.
    ///
    /// When present, the request asserts intentional sharing with owner,
    /// purpose, compatibility, and provenance. Prefer the dedicated
    /// shared publish/attach path once it is mounted; this field lets the
    /// legacy `POST /v1/schemas` route observe and (later) enforce the
    /// same contract without a hard cutover. Private Mini declaration
    /// never sets this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shared_surface: Option<SharedSurfaceMetadata>,
}

fn default_schema_match_source() -> String {
    "direct".to_string()
}

/// Observe-mode classification for `POST /v1/schemas` mutation gate rollout.
///
/// The enum is telemetry-only: callers must not enforce a gate from this
/// classification until the rollout explicitly moves out of observe mode.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SchemaMutationGateIntent {
    IdempotentRepost,
    LocalClaim,
    SharedDiscoveryPublish,
    NewSharedMutation,
}

impl SchemaMutationGateIntent {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::IdempotentRepost => "idempotent_repost",
            Self::LocalClaim => "local_claim",
            Self::SharedDiscoveryPublish => "shared_discovery_publish",
            Self::NewSharedMutation => "new_shared_mutation",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SchemaMutationGateRequirement {
    DevCert,
    ApiKey,
    NodeKey,
    ProofOfWork,
}

impl SchemaMutationGateRequirement {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DevCert => "dev_cert",
            Self::ApiKey => "api_key",
            Self::NodeKey => "node_key",
            Self::ProofOfWork => "proof_of_work",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaMutationGateObservation {
    pub intent: SchemaMutationGateIntent,
    pub owner_app_id: Option<String>,
    pub required_gates: Vec<SchemaMutationGateRequirement>,
}

impl SchemaMutationGateObservation {
    pub fn requires(&self, gate: SchemaMutationGateRequirement) -> bool {
        self.required_gates.contains(&gate)
    }

    pub fn required_gate_labels(&self) -> String {
        if self.required_gates.is_empty() {
            return "none".to_string();
        }
        self.required_gates
            .iter()
            .map(|gate| gate.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaMatchTelemetrySnapshot {
    pub outcomes: HashMap<String, u64>,
    pub fallback_reasons: HashMap<String, u64>,
    pub sources: HashMap<String, u64>,
}

/// Response structure for adding a schema with mutation mappers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddSchemaResponse {
    pub schema: Schema,
    pub mutation_mappers: HashMap<String, String>,
    /// When a schema expansion occurred, this contains the old schema name
    /// that was replaced. The node should remove the old schema and load the new one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replaced_schema: Option<String>,
    /// True iff the returned schema is a system/infrastructure
    /// schema. See `SchemaEnvelope` for background. Always `false`
    /// for schemas added via this endpoint — the add-schema path is
    /// the user-data entry point — but emitted explicitly so clients
    /// don't need a second round-trip to classify.
    #[serde(default)]
    pub system: bool,
    /// True iff this registration was a COMPOSED outcome: the
    /// compositional decompose/apply step rewrote one or more of the
    /// proposal's nested `ref_fields` to reuse an existing canonical
    /// (`SchemaAddOutcome::Composed`) instead of re-inlining its fields.
    /// `false` for a plain `Added`/`Expanded`/`AlreadyExists` outcome.
    ///
    /// This is the authoritative, wire-visible marker of composition —
    /// the response `schema` carries the rewritten `ref_fields` either
    /// way, so the flag (not the presence of `ref_fields`) is what tells
    /// a client the apply path actually reused something. Only emitted
    /// when true (`#[serde(default, skip_serializing_if)]`) so the wire
    /// stays backward-compatible: older clients that ignore it are
    /// unaffected, and a non-composed response is byte-for-byte as before.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub composed: bool,
    /// Present when registration was resolved through component cover before
    /// minting a whole-schema identity. If `residue_fields` is empty, no new
    /// schema identity was registered; otherwise the response `schema` is the
    /// residual schema that was actually minted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub composition: Option<SchemaRegistrationComposition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRegistrationComposition {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_shared_schema_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub covered_components: Vec<SchemaRegistrationComponent>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub residue_fields: Vec<String>,
    pub field_coverage: FieldCoverageEvidence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRegistrationComponent {
    pub field: String,
    pub shared_schema_hash: String,
    pub confidence: f32,
}

/// Reload response structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReloadResponse {
    pub success: bool,
    pub schemas_loaded: usize,
}

/// Health check response structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    /// Monotonic count of catalog write attempts since this process
    /// started. Read it before a release and after an install: the two
    /// values are equal, because neither path writes a schema.
    #[serde(default)]
    pub schema_writes: u64,
}

/// A schema entry with its similarity score. The `schema` field
/// carries the system/user classification via `SchemaEnvelope` so
/// similarity results can be filtered or labelled without a second
/// lookup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimilarSchemaEntry {
    pub schema: SchemaEnvelope,
    pub similarity: f64,
}

/// Response for the find-similar-schemas endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimilarSchemasResponse {
    pub query_schema: String,
    pub threshold: f64,
    pub similar_schemas: Vec<SimilarSchemaEntry>,
}

/// Request for resetting the schema service database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResetRequest {
    pub confirm: bool,
}

/// Response for resetting the schema service database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResetResponse {
    pub success: bool,
    pub message: String,
}

/// A single schema lookup entry in a batch reuse request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaLookupEntry {
    pub descriptive_name: String,
    pub fields: Vec<String>,
}

/// Batch request: multiple schema names to check at once
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSchemaReuseRequest {
    pub schemas: Vec<SchemaLookupEntry>,
}

/// Result for a single matched schema in the batch reuse check.
/// `schema` carries the system/user classification via
/// `SchemaEnvelope`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaReuseMatch {
    pub schema: SchemaEnvelope,
    pub matched_descriptive_name: String,
    pub is_exact_match: bool,
    pub field_rename_map: HashMap<String, String>,
    pub is_superset: bool,
    pub unmapped_fields: Vec<String>,
}

/// Batch response: input descriptive_name -> match result.
/// Only names with matches are included; missing keys = no match found.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSchemaReuseResponse {
    pub matches: HashMap<String, SchemaReuseMatch>,
}

/// One proposed schema for the stateless resolve endpoint.
///
/// This intentionally carries only proposal metadata. `POST
/// /v1/schemas/resolve` is a read path: it never registers this shape as a
/// schema, never creates canonical fields, and never records shared discovery
/// state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaResolveProposal {
    pub descriptive_name: String,
    pub fields: Vec<String>,
    #[serde(default)]
    pub field_descriptions: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose_statement: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_app_id: Option<String>,
}

/// Stateless schema resolve request for local cache misses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaResolveRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_registry_version: Option<u64>,
    pub proposals: Vec<SchemaResolveProposal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SchemaResolveOutcome {
    Reuse,
    Novel,
    CandidateEquivalent,
    Refresh,
}

/// Per-proposal stateless resolve decision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaResolveResult {
    pub outcome: SchemaResolveOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_shared_schema_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#match: Option<SchemaReuseMatch>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub candidates: Vec<SchemaReuseMatch>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub candidate_shared_schema_hashes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaResolveResponse {
    pub registry_version: u64,
    pub cache_stale: bool,
    pub results: HashMap<String, SchemaResolveResult>,
}
