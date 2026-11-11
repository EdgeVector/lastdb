use crate::schema::types::field::HashRangeFilter;
use serde::{Deserialize, Serialize};
use serde_json::Value;

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
    pub filter: Option<HashRangeFilter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort_order: Option<SortOrder>,
    /// Optional assertion for an exact keyed row count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_total_count: Option<usize>,
    /// When `true`, the query takes the full read path instead of a keyed
    /// count or alternate key shortcut. The default is `false`.
    #[serde(default)]
    pub include_tombstones: bool,
}

impl Query {
    #[must_use]
    pub fn new(schema_name: String) -> Self {
        Self::new_with_filter(schema_name, None)
    }

    #[must_use]
    pub fn new_with_filter(schema_name: String, filter: Option<HashRangeFilter>) -> Self {
        Self {
            schema_name,
            key_name: None,
            filter,
            sort_order: None,
            expected_total_count: None,
            include_tombstones: false,
        }
    }
}

/// Every write supplies a complete record or an explicit conditional deletion.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MutationType {
    Create,
    Replace,
    Delete,
    /// Delete with a revision condition that can match an applied delete.
    Purge,
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
        #[serde(rename = "record")]
        record_fields: HashMap<String, Value>,
        key_value: KeyValue,
        alternates: std::collections::BTreeMap<String, Option<KeyValue>>,
        mutation_type: MutationType,
        source_file_name: Option<String>,
        metadata: Option<HashMap<String, String>>,
        file: Option<crate::record_value::FileMarker>,
        expected: CasExpectation,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        convergence: Option<MutationConvergence>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        durability: Option<MutationDurability>,
        /// The definition the writer declared. A shared replace keeps stored
        /// fields this definition does not declare. Omitted, the replace names
        /// the current definition.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        definition_id: Option<String>,
    },
}
