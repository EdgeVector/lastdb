//! Bounded aggregate member and association state.
//!
//! No generation or whole-partition blob exists here. The durable authority is
//! one declared HashRange row per source member plus one bounded guard row per
//! source hash partition.

use super::aggregate::{
    AggregateSet, AGGREGATE_METRIC_FINGERPRINT_FIELD, AGGREGATE_SOURCE_KEY_FIELD,
    AGGREGATE_SOURCE_SCHEMA_FIELD, AGGREGATE_TARGET_KEY_FIELD, AGGREGATE_TARGET_SCHEMA_FIELD,
    AGGREGATE_VALUES_FIELD, AGGREGATE_WINNER_COUNTER_FIELD, AGGREGATE_WINNER_MUTATION_FIELD,
    AGGREGATE_WINNER_WRITER_FIELD, AGGREGATE_WINNER_WRITTEN_AT_FIELD,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};

const AGGREGATE_GUARD_VERSION: u8 = 3;

/// The source mutation's total LWW order. It must match the source write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateWinner {
    pub logical_counter: u64,
    pub written_at: u64,
    pub writer_id: String,
    pub mutation_uuid: String,
}

impl AggregateWinner {
    /// Compare with the shared source-tip order. The aggregate register has no
    /// atom id, so an equal four-part source order remains an explicit conflict
    /// instead of inventing a different fifth tiebreak.
    #[must_use]
    pub fn cmp_source(&self, other: &Self) -> std::cmp::Ordering {
        crate::atom::lww_order_key(
            self.written_at,
            self.logical_counter,
            &self.writer_id,
            &self.mutation_uuid,
            "",
        )
        .cmp(&crate::atom::lww_order_key(
            other.written_at,
            other.logical_counter,
            &other.writer_id,
            &other.mutation_uuid,
            "",
        ))
    }
}

/// One durable member row, stored in the caller-declared HashRange schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateMemberRecord {
    pub source_schema_name: String,
    pub source_key: String,
    pub target_schema_name: String,
    pub target_key: String,
    pub winner: AggregateWinner,
    pub metric_fingerprint: String,
    pub contribution: BTreeMap<String, i64>,
}

impl AggregateMemberRecord {
    /// Encode all reserved fields for one member-row mutation.
    #[must_use]
    pub fn to_fields(&self) -> HashMap<String, Value> {
        HashMap::from([
            (
                AGGREGATE_SOURCE_SCHEMA_FIELD.into(),
                Value::String(self.source_schema_name.clone()),
            ),
            (
                AGGREGATE_SOURCE_KEY_FIELD.into(),
                Value::String(self.source_key.clone()),
            ),
            (
                AGGREGATE_TARGET_SCHEMA_FIELD.into(),
                Value::String(self.target_schema_name.clone()),
            ),
            (
                AGGREGATE_TARGET_KEY_FIELD.into(),
                Value::String(self.target_key.clone()),
            ),
            (
                AGGREGATE_WINNER_COUNTER_FIELD.into(),
                Value::String(self.winner.logical_counter.to_string()),
            ),
            (
                AGGREGATE_WINNER_WRITTEN_AT_FIELD.into(),
                Value::String(self.winner.written_at.to_string()),
            ),
            (
                AGGREGATE_WINNER_WRITER_FIELD.into(),
                Value::String(self.winner.writer_id.clone()),
            ),
            (
                AGGREGATE_WINNER_MUTATION_FIELD.into(),
                Value::String(self.winner.mutation_uuid.clone()),
            ),
            (
                AGGREGATE_METRIC_FINGERPRINT_FIELD.into(),
                Value::String(self.metric_fingerprint.clone()),
            ),
            (
                AGGREGATE_VALUES_FIELD.into(),
                serde_json::to_value(&self.contribution)
                    .expect("aggregate contribution is JSON serializable"),
            ),
        ])
    }

    /// Decode one complete member row. Missing or malformed fields fail closed.
    pub fn from_fields(fields: &BTreeMap<String, Value>) -> Result<Self, String> {
        let string = |name: &str| -> Result<String, String> {
            fields
                .get(name)
                .and_then(Value::as_str)
                .map(str::to_string)
                .ok_or_else(|| {
                    format!("aggregate member field '{name}' is missing or not a string")
                })
        };
        let canonical_u64 = |name: &str| -> Result<u64, String> {
            let value = string(name)?;
            let bytes = value.as_bytes();
            if bytes.is_empty()
                || !bytes.iter().all(u8::is_ascii_digit)
                || (bytes.len() > 1 && bytes[0] == b'0')
            {
                return Err(format!(
                    "aggregate member field '{name}' is not a canonical unsigned decimal"
                ));
            }
            value.parse::<u64>().map_err(|_| {
                format!("aggregate member field '{name}' is outside the u64 decimal domain")
            })
        };
        let contribution: BTreeMap<String, i64> =
            serde_json::from_value(fields.get(AGGREGATE_VALUES_FIELD).cloned().ok_or_else(
                || format!("aggregate member field '{AGGREGATE_VALUES_FIELD}' is missing"),
            )?)
            .map_err(|error| format!("invalid aggregate member values: {error}"))?;
        Ok(Self {
            source_schema_name: string(AGGREGATE_SOURCE_SCHEMA_FIELD)?,
            source_key: string(AGGREGATE_SOURCE_KEY_FIELD)?,
            target_schema_name: string(AGGREGATE_TARGET_SCHEMA_FIELD)?,
            target_key: string(AGGREGATE_TARGET_KEY_FIELD)?,
            winner: AggregateWinner {
                logical_counter: canonical_u64(AGGREGATE_WINNER_COUNTER_FIELD)?,
                written_at: canonical_u64(AGGREGATE_WINNER_WRITTEN_AT_FIELD)?,
                writer_id: string(AGGREGATE_WINNER_WRITER_FIELD)?,
                mutation_uuid: string(AGGREGATE_WINNER_MUTATION_FIELD)?,
            },
            metric_fingerprint: string(AGGREGATE_METRIC_FINGERPRINT_FIELD)?,
            contribution,
        })
    }
}

/// Immutable association for one enrolled source hash partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateGuard {
    pub version: u8,
    pub source_schema_name: String,
    pub source_partition_hash: String,
    pub target_schema_name: String,
    pub target_key: String,
    pub member_schema_name: String,
    pub member_partition_hash: String,
    pub metric_fingerprint: String,
    pub metric_fields: Vec<String>,
    /// Highest source mutation that declared this association.
    pub enrollment_winner: AggregateWinner,
    /// Exact source row that carried `enrollment_winner`.
    pub enrollment_source_key: String,
    /// Exact canonical member row that proves the enrollment commit landed.
    pub enrollment_member_key: String,
    /// False while the guard is a crash-safe pre-commit reservation.
    pub committed: bool,
    /// Deterministic tiebreak and corruption check for the association fields.
    pub association_fingerprint: String,
}

impl AggregateGuard {
    #[must_use]
    pub fn new(
        source_schema_name: String,
        source_key: &super::key_value::KeyValue,
        aggregate: &AggregateSet,
        enrollment_winner: AggregateWinner,
    ) -> Self {
        let mut guard = Self {
            version: AGGREGATE_GUARD_VERSION,
            source_schema_name,
            source_partition_hash: source_key
                .hash
                .clone()
                .expect("aggregate source key requires hash"),
            target_schema_name: aggregate.target_schema_name.clone(),
            target_key: aggregate.target_key_value.to_storage_key(),
            member_schema_name: aggregate.member_schema_name.clone(),
            member_partition_hash: aggregate
                .member_key_value
                .hash
                .clone()
                .expect("AggregateSet::validate requires member hash"),
            metric_fingerprint: metric_fingerprint(aggregate.contribution.keys()),
            metric_fields: aggregate.contribution.keys().cloned().collect(),
            enrollment_winner,
            enrollment_source_key: source_key.to_storage_key(),
            enrollment_member_key: aggregate.member_key_value.to_storage_key(),
            committed: false,
            association_fingerprint: String::new(),
        };
        guard.association_fingerprint = guard.compute_association_fingerprint();
        guard
    }

    #[must_use]
    fn compute_association_fingerprint(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(b"lastdb-aggregate-association-v1\0");
        for value in [
            self.source_schema_name.as_str(),
            self.source_partition_hash.as_str(),
            self.target_schema_name.as_str(),
            self.target_key.as_str(),
            self.member_schema_name.as_str(),
            self.member_partition_hash.as_str(),
            self.metric_fingerprint.as_str(),
        ] {
            hasher.update(value.as_bytes());
            hasher.update([0]);
        }
        format!("{:x}", hasher.finalize())
    }

    #[must_use]
    pub fn same_association(&self, other: &Self) -> bool {
        self.association_fingerprint == other.association_fingerprint
            && self.source_schema_name == other.source_schema_name
            && self.source_partition_hash == other.source_partition_hash
            && self.target_schema_name == other.target_schema_name
            && self.target_key == other.target_key
            && self.member_schema_name == other.member_schema_name
            && self.member_partition_hash == other.member_partition_hash
            && self.metric_fingerprint == other.metric_fingerprint
            && self.metric_fields == other.metric_fields
    }

    /// Deterministic association declaration order for replay conflicts.
    #[must_use]
    pub fn cmp_enrollment(&self, other: &Self) -> std::cmp::Ordering {
        self.enrollment_winner
            .cmp_source(&other.enrollment_winner)
            .then_with(|| {
                self.association_fingerprint
                    .cmp(&other.association_fingerprint)
            })
    }

    #[must_use]
    pub fn committed(mut self) -> Self {
        self.committed = true;
        self
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.version != AGGREGATE_GUARD_VERSION {
            return Err(format!(
                "unsupported aggregate guard version {}",
                self.version
            ));
        }
        if self.metric_fields.is_empty()
            || self.metric_fingerprint != metric_fingerprint(self.metric_fields.iter())
        {
            return Err("aggregate guard metric fingerprint does not match its fields".into());
        }
        if self.association_fingerprint != self.compute_association_fingerprint() {
            return Err("aggregate guard association fingerprint is invalid".into());
        }
        let source_key = super::key_value::KeyValue::from_storage_key(&self.enrollment_source_key);
        let member_key = super::key_value::KeyValue::from_storage_key(&self.enrollment_member_key);
        if source_key.hash.as_deref() != Some(self.source_partition_hash.as_str())
            || member_key.hash.as_deref() != Some(self.member_partition_hash.as_str())
            || member_key.range.is_none()
        {
            return Err("aggregate guard enrollment keys are invalid".into());
        }
        Ok(())
    }
}

/// Deterministic, storage-prefix-visible metadata key for one source partition.
#[must_use]
pub fn aggregate_guard_key(
    storage_prefix: Option<&str>,
    source_schema_name: &str,
    source_partition_hash: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"lastdb-aggregate-guard-v1\0");
    hasher.update(source_schema_name.as_bytes());
    hasher.update([0]);
    hasher.update(source_partition_hash.as_bytes());
    let base = format!("aggregate_guard:v1:{:x}", hasher.finalize());
    crate::schema::types::field::build_storage_key(storage_prefix, &base)
}

/// Deterministic, storage-prefix-visible reverse owner for one summary row.
#[must_use]
pub fn aggregate_target_guard_key(
    storage_prefix: Option<&str>,
    target_schema_name: &str,
    target_key: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"lastdb-aggregate-target-guard-v1\0");
    hasher.update(target_schema_name.as_bytes());
    hasher.update([0]);
    hasher.update(target_key.as_bytes());
    let base = format!("aggregate_target_guard:v1:{:x}", hasher.finalize());
    crate::schema::types::field::build_storage_key(storage_prefix, &base)
}

/// Fingerprint the exact, sorted metric-field set.
#[must_use]
pub fn metric_fingerprint<'a>(fields: impl IntoIterator<Item = &'a String>) -> String {
    let mut fields: Vec<&str> = fields.into_iter().map(String::as_str).collect();
    fields.sort_unstable();
    let mut hasher = Sha256::new();
    hasher.update(b"lastdb-aggregate-metrics-v1\0");
    for field in fields {
        hasher.update(field.as_bytes());
        hasher.update([0]);
    }
    format!("{:x}", hasher.finalize())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateApply {
    Applied(BTreeMap<String, i64>),
    IgnoredStale,
    IgnoredRetry,
}

/// Apply one member replacement to a point-read summary.
///
/// All metric fields remain present, including zero. A negative result proves
/// that the member rows and summary disagree, so the caller fails closed.
pub fn apply_member_replacement(
    old: Option<&AggregateMemberRecord>,
    incoming: &AggregateMemberRecord,
    current_summary: &BTreeMap<String, i64>,
) -> Result<AggregateApply, String> {
    if incoming.metric_fingerprint != metric_fingerprint(incoming.contribution.keys()) {
        return Err("incoming aggregate member metric fingerprint is invalid".into());
    }
    if let Some(old) = old {
        if old.source_schema_name != incoming.source_schema_name
            || old.source_key != incoming.source_key
            || old.target_schema_name != incoming.target_schema_name
            || old.target_key != incoming.target_key
            || old.metric_fingerprint != incoming.metric_fingerprint
        {
            return Err("aggregate member key collision or target relocation was detected".into());
        }
        if incoming.winner.cmp_source(&old.winner).is_lt() {
            return Ok(AggregateApply::IgnoredStale);
        }
        if incoming.winner.cmp_source(&old.winner).is_eq() {
            if incoming.contribution == old.contribution {
                return Ok(AggregateApply::IgnoredRetry);
            }
            return Err("aggregate_set has conflicting values for one source winner".into());
        }
    }

    let empty_values = BTreeMap::new();
    let old_values = old.map_or(&empty_values, |member| &member.contribution);
    let mut next = BTreeMap::new();
    for field in incoming.contribution.keys() {
        let total = *current_summary
            .get(field)
            .ok_or_else(|| format!("aggregate summary is missing fixed metric field '{field}'"))?;
        let old_value = *old_values.get(field).unwrap_or(&0);
        let new_value = incoming.contribution[field];
        let delta = new_value
            .checked_sub(old_value)
            .ok_or_else(|| format!("aggregate_set delta overflow for {field}"))?;
        let value = total
            .checked_add(delta)
            .ok_or_else(|| format!("aggregate_set summary overflow for {field}"))?;
        if value < 0 {
            return Err(format!(
                "aggregate_set summary underflow for {field}; repair is required"
            ));
        }
        next.insert(field.clone(), value);
    }
    Ok(AggregateApply::Applied(next))
}
