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
