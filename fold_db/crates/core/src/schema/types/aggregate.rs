//! Concrete aggregate-member intent carried with one source mutation.
//!
//! The mutation log stores the concrete member value. It does not store an
//! aggregation function or a computed total. Replay replaces one durable,
//! bounded member row and derives the summary delta from that replacement.

use super::key_value::KeyValue;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Maximum number of fixed metric fields in one aggregate association.
pub const MAX_AGGREGATE_METRICS: usize = 64;

/// Reserved member-row fields. A member schema must declare every field.
pub const AGGREGATE_SOURCE_SCHEMA_FIELD: &str = "__lastdb_aggregate_source_schema";
pub const AGGREGATE_SOURCE_KEY_FIELD: &str = "__lastdb_aggregate_source_key";
pub const AGGREGATE_TARGET_SCHEMA_FIELD: &str = "__lastdb_aggregate_target_schema";
pub const AGGREGATE_TARGET_KEY_FIELD: &str = "__lastdb_aggregate_target_key";
pub const AGGREGATE_WINNER_COUNTER_FIELD: &str = "__lastdb_aggregate_winner_counter";
pub const AGGREGATE_WINNER_WRITTEN_AT_FIELD: &str = "__lastdb_aggregate_winner_written_at";
pub const AGGREGATE_WINNER_WRITER_FIELD: &str = "__lastdb_aggregate_winner_writer";
pub const AGGREGATE_WINNER_MUTATION_FIELD: &str = "__lastdb_aggregate_winner_mutation";
pub const AGGREGATE_METRIC_FINGERPRINT_FIELD: &str = "__lastdb_aggregate_metric_fingerprint";
pub const AGGREGATE_VALUES_FIELD: &str = "__lastdb_aggregate_values";

/// Reserved summary-row fields. A target schema must declare both fields.
pub const AGGREGATE_VALID_FIELD: &str = "__lastdb_aggregate_valid";
pub const AGGREGATE_GUARD_TOKEN_FIELD: &str = "__lastdb_aggregate_guard_token";

/// All fields owned by the aggregate member implementation.
pub const AGGREGATE_MEMBER_RESERVED_FIELDS: &[&str] = &[
    AGGREGATE_SOURCE_SCHEMA_FIELD,
    AGGREGATE_SOURCE_KEY_FIELD,
    AGGREGATE_TARGET_SCHEMA_FIELD,
    AGGREGATE_TARGET_KEY_FIELD,
    AGGREGATE_WINNER_COUNTER_FIELD,
    AGGREGATE_WINNER_WRITTEN_AT_FIELD,
    AGGREGATE_WINNER_WRITER_FIELD,
    AGGREGATE_WINNER_MUTATION_FIELD,
    AGGREGATE_METRIC_FINGERPRINT_FIELD,
    AGGREGATE_VALUES_FIELD,
];

/// Replace one aggregate member's concrete contribution.
///
/// The source and member schemas are distinct HashRange schemas. The target is
/// a Hash schema. The member key equals the exact source key, so one source row
/// cannot move and leave a second counted row. The source mutation must persist
/// a live, declared non-key field and cannot contain a field tombstone. The
/// product treats an enrolled source as append/update-only. A logical removal
/// writes a live removal state with an all-zero contribution. Old clients that
/// emit a physical Delete or Purge are incompatible with aggregate activation
/// because a bare erasure intent has no replayable association on a peer that
/// has not seen the guard.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AggregateSet {
    /// Schema of the derived, point-readable summary row.
    pub target_schema_name: String,
    /// Exact key of the derived summary row.
    pub target_key_value: KeyValue,
    /// HashRange schema that stores one durable row per source member.
    pub member_schema_name: String,
    /// Exact HashRange key of this source member.
    pub member_key_value: KeyValue,
    /// Complete fixed vector for the association. Zero is an explicit value.
    pub contribution: BTreeMap<String, i64>,
}

impl AggregateSet {
    #[must_use]
    pub fn new(
        target_schema_name: String,
        target_key_value: KeyValue,
        member_schema_name: String,
        member_key_value: KeyValue,
        contribution: BTreeMap<String, i64>,
    ) -> Self {
        Self {
            target_schema_name,
            target_key_value,
            member_schema_name,
            member_key_value,
            contribution,
        }
    }

    /// Reject malformed identities before a caller can write its source row.
    pub fn validate(&self) -> Result<(), String> {
        if self.target_schema_name.trim().is_empty() {
            return Err("aggregate_set target_schema_name must not be empty".to_string());
        }
        if self.target_key_value.hash.is_none() && self.target_key_value.range.is_none() {
            return Err("aggregate_set target_key_value must not be empty".to_string());
        }
        if self.member_schema_name.trim().is_empty() {
            return Err("aggregate_set member_schema_name must not be empty".to_string());
        }
        if self.member_key_value.hash.is_none() || self.member_key_value.range.is_none() {
            return Err(
                "aggregate_set member_key_value requires both hash and range keys".to_string(),
            );
        }
        if self.contribution.is_empty() {
            return Err("aggregate_set contribution must not be empty".to_string());
        }
        if self.contribution.len() > MAX_AGGREGATE_METRICS {
            return Err(format!(
                "aggregate_set contribution exceeds {MAX_AGGREGATE_METRICS} metric fields"
            ));
        }
        if self
            .contribution
            .keys()
            .any(|field| field.trim().is_empty())
        {
            return Err("aggregate_set contribution field names must not be empty".to_string());
        }
        if self.contribution.keys().any(|field| {
            AGGREGATE_MEMBER_RESERVED_FIELDS.contains(&field.as_str())
                || matches!(
                    field.as_str(),
                    AGGREGATE_VALID_FIELD | AGGREGATE_GUARD_TOKEN_FIELD
                )
        }) {
            return Err("aggregate_set metric fields must not use reserved field names".into());
        }
        if self.contribution.values().any(|value| *value < 0) {
            return Err(
                "aggregate_set contribution values must be non-negative; replacement deltas are internal"
                    .to_string(),
            );
        }
        Ok(())
    }
}

/// Input to the node-local verified-repair finalize operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AggregateFinalize {
    pub source_schema_name: String,
    /// Hash-only source partition key. The range component must be absent.
    pub source_partition_key_value: KeyValue,
    pub target_schema_name: String,
    pub target_key_value: KeyValue,
    pub member_schema_name: String,
    pub expected_guard_token: String,
}

/// Input to the node-local bounded-member repair operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AggregateRepair {
    pub source_schema_name: String,
    /// Hash-only source partition key. The range component must be absent.
    pub source_partition_key_value: KeyValue,
    pub target_schema_name: String,
    pub target_key_value: KeyValue,
    pub member_schema_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_guard_token: Option<String>,
}
