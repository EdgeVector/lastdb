use super::*;

#[derive(Debug, Deserialize)]
pub(super) struct StageBody {
    pub(super) recipient_pubkey: String,
    #[serde(default)]
    pub(super) recipient_display_name: Option<String>,
    /// X25519 messaging public key (base64). Required for Mini approve-send.
    pub(super) messaging_public_key: String,
    /// Messaging pseudonym UUID. Required for Mini approve-send.
    pub(super) messaging_pseudonym: String,
    #[serde(default)]
    pub(super) mode: DeliveryMode,
    /// Query-defined slice legs. Each leg is one schema query.
    #[serde(default)]
    pub(super) legs: Vec<StageLeg>,
    /// Convenience: single schema + fields (expanded to one leg).
    #[serde(default)]
    pub(super) schema_name: Option<String>,
    #[serde(default)]
    pub(super) fields: Option<Vec<String>>,
    /// Optional query filter for the convenience schema_name path (e.g. SampleN / Page).
    #[serde(default)]
    pub(super) filter: Option<HashRangeFilter>,
    /// Two-pass field predicates. Serialized as `where`, matching `/api/query`.
    #[serde(default, rename = "where")]
    pub(super) field_predicates: Option<Vec<FieldPredicate>>,
    /// Convenience time window, e.g. "24h", "7d", "30m". Adds an `after`
    /// predicate against `since_field` (default `updated_at`).
    #[serde(default)]
    pub(super) since: Option<String>,
    #[serde(default)]
    pub(super) since_field: Option<String>,
    /// Optional field ordering. Accepts either a field string or the core
    /// `{ "field": "...", "order": "desc" }` shape.
    #[serde(default)]
    pub(super) order_by: Option<StageOrderBy>,
    /// Companion to string `order_by`.
    #[serde(default)]
    pub(super) order: Option<SortOrder>,
    /// Optional column allow/deny sugar for kanban Card slices.
    #[serde(default)]
    pub(super) columns_include: Option<Vec<String>>,
    #[serde(default)]
    pub(super) columns_exclude: Option<Vec<String>>,
    /// Cap applied after field predicates/order_by. Defaults from max_records
    /// for two-pass requests.
    #[serde(default)]
    pub(super) predicate_limit: Option<usize>,
    /// Cap records for messaging size (~64KB sealed blob). Applied as
    /// [`HashRangeFilter::SampleN`] when no explicit filter or two-pass
    /// predicate/order is set; otherwise it becomes `predicate_limit`.
    /// Prefer this for admin board summaries of large boards.
    #[serde(default)]
    pub(super) max_records: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub(super) struct StageLeg {
    pub(super) schema_name: String,
    pub(super) fields: Vec<String>,
    /// Optional hash keys (Hash schemas) — materialize only these records.
    /// When set, expands to one query leg per key with [`HashRangeFilter::HashKey`].
    #[serde(default)]
    pub(super) hash_keys: Option<Vec<String>>,
    #[serde(default)]
    pub(super) filter: Option<HashRangeFilter>,
    #[serde(default, rename = "where")]
    pub(super) field_predicates: Option<Vec<FieldPredicate>>,
    #[serde(default)]
    pub(super) order_by: Option<StageOrderBy>,
    #[serde(default)]
    pub(super) order: Option<SortOrder>,
    #[serde(default)]
    pub(super) predicate_limit: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub(super) enum StageOrderBy {
    Field(String),
    Query(QueryOrderBy),
}
