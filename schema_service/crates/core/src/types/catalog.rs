use schema_types::Schema;
use serde::{Deserialize, Serialize};

/// Response containing a list of available schema names
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemasListResponse {
    pub schemas: Vec<String>,
}

/// A schema plus a `system` flag, as returned over `/v1/*` endpoints
/// that surface schema definitions.
///
/// `system` is always `false`. The service does not ship a built-in
/// schema seed. The flag stays on the wire because clients deserialize
/// it as a required bool.
///
/// Serialized with `#[serde(flatten)]` on `schema`, so the JSON
/// layout is the Schema's own fields plus a top-level `system`
/// boolean.
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
