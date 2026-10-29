use schema_types::Schema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::schema_resolver_abi::FieldCoverageEvidence;

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
