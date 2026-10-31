use schema_types::Schema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::shared_surface::SharedSurfaceMetadata;
use crate::state_compositional::ComponentReuseAdvice;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchemaAddOutcome {
    Added(Schema, HashMap<String, String>), // Schema and mutation_mappers
    AlreadyExists(Schema, HashMap<String, String>), // Exact same identity hash + mappers from canonicalization
    /// Existing schema was expanded with new fields (old schema name, expanded schema, mappers)
    Expanded(String, Schema, HashMap<String, String>),
    /// An Approved schema with the same `descriptive_name` already exists and
    /// the incoming proposal cannot be cleanly expanded into it (e.g. a
    /// different `schema_type`). The caller must rename the new schema or
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
