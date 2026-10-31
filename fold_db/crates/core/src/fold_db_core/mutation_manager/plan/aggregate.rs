//! Aggregate decisions over captured guards, grants, and summary values:
//! association checks, the repair grant and its keys, the finalize action,
//! and the durable-receipt requirement.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::schema::types::aggregate::{AGGREGATE_GUARD_TOKEN_FIELD, AGGREGATE_VALID_FIELD};
use crate::schema::types::aggregate_state::aggregate_target_guard_key;
use crate::schema::types::{AggregateFinalize, AggregateGuard, AggregateWinner, KeyValue};
use crate::schema::SchemaError;

use super::super::receipt::{ResidentCommitReceipt, ResidentDurability};

/// Local proof that a complete bounded repair produced the metric generation
/// that finalize is about to certify. The token text alone is not authority:
/// a source writer controls its mutation UUID and could otherwise forge the
/// historical `repair:` spelling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::fold_db_core::mutation_manager) struct AggregateRepairGrant {
    version: u8,
    repair_token: String,
    association_fingerprint: String,
    enrollment_winner: AggregateWinner,
    totals_fingerprint: String,
    totals: BTreeMap<String, i64>,
}

/// The grant a complete repair of `guard` writes for its finalize call.
pub(in crate::fold_db_core::mutation_manager) fn repair_grant(
    repair_token: String,
    guard: &AggregateGuard,
    totals: BTreeMap<String, i64>,
) -> AggregateRepairGrant {
    AggregateRepairGrant {
        version: 1,
        repair_token,
        association_fingerprint: guard.association_fingerprint.clone(),
        enrollment_winner: guard.enrollment_winner.clone(),
        totals_fingerprint: totals_fingerprint(&totals),
        totals,
    }
}

pub(in crate::fold_db_core::mutation_manager) fn repair_grant_key(
    storage_prefix: Option<&str>,
    target_schema_name: &str,
    target_key: &str,
) -> String {
    format!(
        "{}:repair_grant",
        aggregate_target_guard_key(storage_prefix, target_schema_name, target_key)
    )
}

pub(in crate::fold_db_core::mutation_manager) fn finalized_grant_key(
    storage_prefix: Option<&str>,
    target_schema_name: &str,
    target_key: &str,
) -> String {
    format!(
        "{}:finalized",
        repair_grant_key(storage_prefix, target_schema_name, target_key)
    )
}

pub(in crate::fold_db_core::mutation_manager) fn totals_fingerprint(
    totals: &BTreeMap<String, i64>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"lastdb-aggregate-totals-v1\0");
    for (field, total) in totals {
        hasher.update(field.as_bytes());
        hasher.update([0]);
        hasher.update(total.to_be_bytes());
    }
    format!("{:x}", hasher.finalize())
}

/// An aggregate phase accepts only a durable commit receipt.
pub(in crate::fold_db_core::mutation_manager) fn require_durable_receipt(
    receipt: ResidentCommitReceipt,
    phase: &str,
) -> Result<ResidentCommitReceipt, SchemaError> {
    if receipt.durability != ResidentDurability::Durable {
        return Err(SchemaError::InvalidData(format!(
            "aggregate {phase} returned a queued receipt"
        )));
    }
    Ok(receipt)
}

pub(in crate::fold_db_core::mutation_manager) fn association_matches(
    guard: &AggregateGuard,
    source: &str,
    source_hash: &str,
    target: &str,
    target_key: &KeyValue,
    member: &str,
) -> bool {
    guard.source_schema_name == source
        && guard.source_partition_hash == source_hash
        && guard.target_schema_name == target
        && guard.target_key == target_key.to_storage_key()
        && guard.member_schema_name == member
        && guard.committed
}

pub(in crate::fold_db_core::mutation_manager) fn target_owner_matches(
    owner: &AggregateGuard,
    guard: &AggregateGuard,
) -> bool {
    owner.same_association(guard) && owner.cmp_enrollment(guard) == std::cmp::Ordering::Equal
}

pub(in crate::fold_db_core::mutation_manager) enum FinalizeAction {
    AlreadyValid,
    WriteValidMarker,
}

pub(in crate::fold_db_core::mutation_manager) fn validate_finalize_request(
    finalize: &AggregateFinalize,
) -> Result<(), SchemaError> {
    if finalize.source_schema_name.trim().is_empty()
        || finalize.target_schema_name.trim().is_empty()
        || finalize.member_schema_name.trim().is_empty()
        || finalize.expected_guard_token.trim().is_empty()
    {
        return Err(SchemaError::InvalidData(
            "aggregate finalize identities and expected_guard_token must not be empty".into(),
        ));
    }
    if finalize.source_partition_key_value.hash.is_none()
        || finalize.source_partition_key_value.range.is_some()
    {
        return Err(SchemaError::InvalidData(
            "aggregate finalize source_partition_key_value must be hash-only".into(),
        ));
    }
    Ok(())
}

/// Decide the finalize action from the captured summary status and grant.
pub(in crate::fold_db_core::mutation_manager) fn finalize_action(
    finalize: &AggregateFinalize,
    guard: &AggregateGuard,
    grant: &AggregateRepairGrant,
    status: &BTreeMap<String, serde_json::Value>,
    has_active_grant: bool,
) -> Result<FinalizeAction, SchemaError> {
    let current_token = status
        .get(AGGREGATE_GUARD_TOKEN_FIELD)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| SchemaError::InvalidData("aggregate summary has no guard token".into()))?;
    if current_token != finalize.expected_guard_token {
        return Err(SchemaError::InvalidData(
            "aggregate finalize guard token changed during repair".into(),
        ));
    }
    let mut totals = BTreeMap::new();
    for field in &guard.metric_fields {
        let value = status
            .get(field)
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "aggregate finalize summary metric '{field}' is missing or malformed"
                ))
            })?;
        totals.insert(field.clone(), value);
    }
    if grant.version != 1
        || grant.repair_token != finalize.expected_guard_token
        || grant.association_fingerprint != guard.association_fingerprint
        || grant.enrollment_winner != guard.enrollment_winner
        || grant.totals != totals
        || grant.totals_fingerprint != totals_fingerprint(&totals)
    {
        return Err(SchemaError::InvalidData(
            "aggregate finalize repair grant does not match the complete metric generation".into(),
        ));
    }
    let valid = status
        .get(AGGREGATE_VALID_FIELD)
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| {
            SchemaError::InvalidData("aggregate summary has no integer valid marker".into())
        })?;
    if !matches!(valid, 0 | 1) {
        return Err(SchemaError::InvalidData(
            "aggregate summary valid marker must be 0 or 1".into(),
        ));
    }
    if valid == 1 {
        return Ok(FinalizeAction::AlreadyValid);
    }
    if !has_active_grant {
        return Err(SchemaError::InvalidData(
            "aggregate finalized proof cannot authorize a new valid marker".into(),
        ));
    }
    Ok(FinalizeAction::WriteValidMarker)
}
