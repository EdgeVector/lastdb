use crate::schema::types::field::HashRangeFilter;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Numeric comparison filters for field values.
///
/// These filters compare current field values independently of the projection,
/// unlike `HashRangeFilter` which selects field keys. Multiple filters are
/// AND'd before ordering and `predicate_limit`. Missing values remain accepted;
/// a concrete non-numeric value fails its comparison.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ValueFilter {
    /// field value > threshold
    GreaterThan { field: String, value: f64 },
    /// field value < threshold
    LessThan { field: String, value: f64 },
    /// field value == the exact represented float target
    Equals { field: String, value: f64 },
    /// min <= field value <= max
    Between { field: String, min: f64, max: f64 },
}

/// Field predicates for the shared two-pass query scan.
///
/// These predicates are evaluated against a first pass that loads only the
/// predicate fields plus optional `order_by` field. Matching keys are then
/// fetched again with the requested projection fields. This is still an O(M)
/// scan over candidate keys, not a secondary index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldPredicate {
    /// field value == value
    Eq { field: String, value: Value },
    /// field value is one of values
    In { field: String, values: Vec<Value> },
    /// field timestamp >= instant. Instants accept RFC3339 strings or Unix seconds/millis.
    After { field: String, instant: Value },
    /// field timestamp <= instant. Instants accept RFC3339 strings or Unix seconds/millis.
    Before { field: String, instant: Value },
    /// field exists and is not null
    Present { field: String },
    /// field is missing or null
    Absent { field: String },
}

impl FieldPredicate {
    #[must_use]
    pub fn field_name(&self) -> &str {
        match self {
            Self::Eq { field, .. }
            | Self::In { field, .. }
            | Self::After { field, .. }
            | Self::Before { field, .. }
            | Self::Present { field }
            | Self::Absent { field } => field,
        }
    }
}

impl ValueFilter {
    /// Tests whether the given JSON value satisfies this filter condition.
    /// Returns `false` if the value is not numeric.
    pub fn matches(&self, field_value: &serde_json::Value) -> bool {
        let Some(number) = field_value.as_number() else {
            return false;
        };
        let compare = |value| crate::json_order::compare_number_to_f64(number, value);
        match self {
            Self::GreaterThan { value, .. } => {
                compare(*value).is_some_and(std::cmp::Ordering::is_gt)
            }
            Self::LessThan { value, .. } => compare(*value).is_some_and(std::cmp::Ordering::is_lt),
            Self::Equals { value, .. } => compare(*value).is_some_and(std::cmp::Ordering::is_eq),
            Self::Between { min, max, .. } => {
                compare(*min).is_some_and(std::cmp::Ordering::is_ge)
                    && compare(*max).is_some_and(std::cmp::Ordering::is_le)
            }
        }
    }

    /// Returns the field name this filter targets.
    pub fn field_name(&self) -> &str {
        match self {
            Self::GreaterThan { field, .. }
            | Self::LessThan { field, .. }
            | Self::Equals { field, .. }
            | Self::Between { field, .. } => field,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SortOrder {
    Asc,
    Desc,
}

impl Serialize for SortOrder {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Asc => serializer.serialize_str("asc"),
            Self::Desc => serializer.serialize_str("desc"),
        }
    }
}

impl<'de> Deserialize<'de> for SortOrder {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.to_lowercase().as_str() {
            "asc" => Ok(Self::Asc),
            "desc" => Ok(Self::Desc),
            _ => Err(serde::de::Error::custom(format!(
                "unknown sort order '{s}', expected 'asc' or 'desc'"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    pub schema_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_name: Option<String>,
    pub fields: Vec<String>,
    pub filter: Option<HashRangeFilter>,
    /// Accepted only so logical reads and counts can explicitly refuse history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub as_of: Option<DateTime<Utc>>,
    /// Accepted only for an explicit refusal. No read supports recursive depth.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rehydrate_depth: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort_order: Option<SortOrder>,
    /// Numeric conditions on current fields, independent of the projection.
    /// Multiple filters are AND'd before `order_by` and `predicate_limit`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_filters: Option<Vec<ValueFilter>>,
    /// Two-pass field predicates. Serialized as `where` for the owner-socket
    /// API: pass A loads only these fields (plus `order_by`), pass B loads the
    /// requested projection for matching keys.
    #[serde(default, rename = "where", skip_serializing_if = "Option::is_none")]
    pub field_predicates: Option<Vec<FieldPredicate>>,
    /// Optional field ordering for the two-pass candidate set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_by: Option<QueryOrderBy>,
    /// Optional cap after field and numeric conditions and `order_by`, before the
    /// projection pass. Named separately from `/api/query` pagination `limit`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate_limit: Option<usize>,
    /// Optional assertion for an exact keyed row count. The host evaluates it
    /// only on a keyed query with no post-key filters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_total_count: Option<usize>,
    /// When `true`, the query takes the full read path instead of a keyed
    /// count or alternate key shortcut. The default is `false`.
    #[serde(default)]
    pub include_tombstones: bool,
    /// Opt in to bounded secondary field reads for this query. One is serial.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secondary_concurrency: Option<usize>,
}

impl Query {
    /// Whether candidate keys need a later condition, ordering, or limit stage.
    /// Empty condition lists are equivalent to omitted conditions for both
    /// direct library callers and requests decoded from the wire.
    #[must_use]
    pub fn requires_post_key_work(&self) -> bool {
        self.value_filters
            .as_ref()
            .is_some_and(|filters| !filters.is_empty())
            || self
                .field_predicates
                .as_ref()
                .is_some_and(|predicates| !predicates.is_empty())
            || self.order_by.is_some()
            || self.predicate_limit.is_some()
    }

    #[must_use]
    pub fn new(schema_name: String, fields: Vec<String>) -> Self {
        Self {
            schema_name,
            key_name: None,
            fields,
            filter: None,
            as_of: None,
            rehydrate_depth: None,
            sort_order: None,
            value_filters: None,
            field_predicates: None,
            order_by: None,
            predicate_limit: None,
            expected_total_count: None,
            include_tombstones: false,
            secondary_concurrency: None,
        }
    }

    #[must_use]
    pub fn new_with_filter(
        schema_name: String,
        fields: Vec<String>,
        filter: Option<HashRangeFilter>,
    ) -> Self {
        Self {
            schema_name,
            key_name: None,
            fields,
            filter,
            as_of: None,
            rehydrate_depth: None,
            sort_order: None,
            value_filters: None,
            field_predicates: None,
            order_by: None,
            predicate_limit: None,
            expected_total_count: None,
            include_tombstones: false,
            secondary_concurrency: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryOrderBy {
    pub field: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<SortOrder>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
pub enum MutationType {
    Create,
    Update,
    /// User-facing hard delete. "Delete means gone": same erasure path as
    /// [`Self::Purge`] (no per-field tombstone records). Missing target is
    /// an idempotent success unless the request sets `must_exist: true`
    /// (loud miss). See north-star-lastdb-delete-returns-the-bytes.
    Delete,
    /// Compatibility alias of [`Self::Delete`] + `must_exist`. Same
    /// physical path; a missing target is a *loud* error ("refusing to
    /// silently no-op") so compliance audit can tell a subject was
    /// already gone. Not a second eraser. Prefer `delete` + `must_exist`
    /// on new clients.
    Purge,
}

impl<'de> Deserialize<'de> for MutationType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.to_lowercase().as_str() {
            "create" => Ok(Self::Create),
            "update" => Ok(Self::Update),
            "delete" => Ok(Self::Delete),
            "purge" => Ok(Self::Purge),
            _ => Err(serde::de::Error::custom(
                "unknown mutation type, expected create, update, or delete (purge is a must-exist alias)",
            )),
        }
    }
}

// Re-export Mutation from the dedicated mutation module
pub use super::mutation::Mutation;

use crate::schema::types::cas::CasExpectation;
use crate::schema::types::key_value::KeyValue;
use std::collections::HashMap;

/// How a mutation request handles post-write background convergence before
/// returning to the caller.
///
/// Default is [`Async`]: the handler returns after the core write reaches its
/// required publication or durability boundary. It does not drain unrelated
/// background work through the global task tracker.
///
/// Pass [`Sync`] only when the caller needs background work drained before
/// the response (e.g. a follow-up search that is outside the core query result).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MutationConvergence {
    /// Wait for background work that was already in flight when the wait began.
    Sync,
    /// Return after the write commits; report convergence as pending.
    /// Default: do not block the UDS/HTTP response on the global pending-task
    /// tracker (avoids multi-second `index_wait` under resident write-behind).
    #[default]
    Async,
}

impl MutationConvergence {
    pub fn waits(self) -> bool {
        matches!(self, Self::Sync)
    }
}

/// The local persistence guarantee that a mutation request requires before
/// the node returns its receipt.
///
/// Durability is independent from [`MutationConvergence`]. A durable request
/// waits for local mutation state to persist, but it does not drain unrelated
/// background convergence work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MutationDurability {
    /// Persist local mutation state before the node returns a durable receipt.
    Durable,
    /// Compatibility minimum; native commits still wait for a durable snapshot.
    #[default]
    Queued,
}

impl MutationDurability {
    pub fn waits_for_persist(self) -> bool {
        matches!(self, Self::Durable)
    }
}

/// Represents an operation that can be performed on the database
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum Operation {
    #[serde(rename = "mutation")]
    Mutation {
        schema: String,
        fields_and_values: HashMap<String, Value>,
        key_value: KeyValue,
        mutation_type: MutationType,
        #[serde(skip_serializing_if = "Option::is_none")]
        source_file_name: Option<String>,
        /// Optional compare-and-set precondition (see [`CasExpectation`]).
        /// Additive: absent on every operation submitted before CAS existed,
        /// and `skip_serializing_if` keeps their serialized form unchanged.
        /// Threaded onto the built [`Mutation`] so the node write path applies
        /// the write only if the current state at the key matches.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected: Option<CasExpectation>,
        /// Optional convergence mode. Defaults to `async` (no post-write
        /// background-task barrier). Pass `sync` only when the caller needs
        /// background work drained before the response.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        convergence: Option<MutationConvergence>,
        /// Optional requested local policy; the wire default is `queued`.
        /// Native requests always wait for disk durability with either value.
        /// This policy does not change convergence behavior.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        durability: Option<MutationDurability>,
        /// Local request policy on `delete`: `true` is loud-miss. Absent
        /// on every body submitted before this field existed. Never captured
        /// on the mutation-intent envelope.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        must_exist: Option<bool>,
        /// Delete every live row under `key_value.hash` whose range starts
        /// with this prefix. Only the owner single-mutation route accepts it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key_range_prefix: Option<String>,
    },
}
