//! Finalization decisions use captured summary/grant facts without I/O.

use super::*;

pub(super) enum FinalizeAction {
    AlreadyValid,
    WriteValidMarker,
}

pub(super) fn validate_request(finalize: &AggregateFinalize) -> Result<(), SchemaError> {
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

pub(super) fn finalize_action(
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
        || grant.totals_fingerprint != MutationManager::aggregate_totals_fingerprint(&totals)
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
