use schema_types::Schema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::shared_surface::SharedSurfaceMetadata;

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
    /// Low-cardinality caller label. Mini still sends this on `POST /v1/schemas`.
    ///
    /// `direct` is the default when a client omits the field. A node that
    /// already ran its offline matcher sends `local_matcher_fallback`.
    #[serde(default = "default_schema_match_source")]
    pub schema_match_source: String,
    /// Low-cardinality reason paired with `schema_match_source` when that
    /// label is `local_matcher_fallback`. Clients should use stable strings
    /// such as `no_candidate_schema` or `insufficient_field_coverage`.
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
    /// purpose, compatibility, and provenance. The metadata must pass
    /// `validate_surface_metadata`. Private Mini declaration never sets
    /// this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shared_surface: Option<SharedSurfaceMetadata>,
}

fn default_schema_match_source() -> String {
    "direct".to_string()
}
