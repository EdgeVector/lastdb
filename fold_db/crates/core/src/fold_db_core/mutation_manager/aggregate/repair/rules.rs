//! Member reconciliation computes facts; the entry point orders cleanup and refusal.

use super::*;
use crate::schema::types::MutationIds;

pub(super) struct RepairPlan {
    pub(super) totals: BTreeMap<String, i64>,
    pub(super) orphan_purges: Vec<Mutation>,
    pub(super) complete: bool,
}

pub(super) fn reconcile_members(
    guard: &AggregateGuard,
    rows: HashMap<KeyValue, BTreeMap<String, serde_json::Value>>,
    source_winners: &HashMap<KeyValue, AggregateWinner>,
) -> Result<RepairPlan, SchemaError> {
    let mut totals = guard
        .metric_fields
        .iter()
        .cloned()
        .map(|field| (field, 0))
        .collect();
    let mut source_members = std::collections::HashSet::new();
    let mut orphan_purges = Vec::new();
    for (member_key, fields) in rows {
        reconcile_repair_member(
            guard,
            member_key,
            &fields,
            source_winners,
            &mut totals,
            &mut source_members,
            &mut orphan_purges,
        )?;
    }
    let complete = source_members.len() == source_winners.len()
        && source_winners
            .keys()
            .all(|key| source_members.contains(key));
    Ok(RepairPlan {
        totals,
        orphan_purges,
        complete,
    })
}

pub(super) fn validate_request(repair: &AggregateRepair) -> Result<(), SchemaError> {
    if repair.source_partition_key_value.hash.is_none()
        || repair.source_partition_key_value.range.is_some()
    {
        return Err(SchemaError::InvalidData(
            "aggregate repair requires a hash-only source partition".into(),
        ));
    }
    if repair
        .expected_guard_token
        .as_deref()
        .is_some_and(|token| token.trim().is_empty())
    {
        return Err(SchemaError::InvalidData(
            "aggregate repair expected_guard_token must not be empty".into(),
        ));
    }
    Ok(())
}

fn reconcile_repair_member(
    guard: &AggregateGuard,
    member_key: KeyValue,
    fields: &BTreeMap<String, serde_json::Value>,
    source_winners: &HashMap<KeyValue, AggregateWinner>,
    totals: &mut BTreeMap<String, i64>,
    source_members: &mut std::collections::HashSet<KeyValue>,
    absent_member_purges: &mut Vec<Mutation>,
) -> Result<(), SchemaError> {
    if fields.len() != AGGREGATE_MEMBER_RESERVED_FIELDS.len() {
        return Err(SchemaError::InvalidData(format!(
            "aggregate member '{}' is partial",
            member_key.to_storage_key()
        )));
    }
    if member_key.hash.as_deref() != Some(guard.member_partition_hash.as_str()) {
        return Err(SchemaError::InvalidData(
            "aggregate repair returned a member outside its guarded partition".into(),
        ));
    }
    let member = AggregateMemberRecord::from_fields(fields).map_err(SchemaError::InvalidData)?;
    let member_source_key = KeyValue::from_storage_key(&member.source_key)?;
    // The source schema was verified HashRange, so the key must carry a range.
    let source_key_shape_matches = member_source_key.range.is_some();
    let canonical_member_key = member_source_key.clone();
    if member.source_schema_name != guard.source_schema_name
        || member_source_key.hash.as_deref() != Some(guard.source_partition_hash.as_str())
        || !source_key_shape_matches
        || member_key != canonical_member_key
        || member.target_schema_name != guard.target_schema_name
        || member.target_key != guard.target_key
        || member.metric_fingerprint != guard.metric_fingerprint
        || member.contribution.keys().cloned().collect::<Vec<_>>() != guard.metric_fields
        || member.contribution.values().any(|value| *value < 0)
    {
        return Err(SchemaError::InvalidData(
            "aggregate repair found a member outside the guarded association".into(),
        ));
    }
    let Some(source_winner) = source_winners.get(&member_source_key) else {
        let mut purge = Mutation::new(
            guard.member_schema_name.clone(),
            HashMap::new(),
            member_key,
            String::new(),
            MutationType::Purge,
        );
        purge.aggregate_derived_internal = true;
        absent_member_purges.push(purge);
        return Ok(());
    };
    if source_winner != &member.winner {
        return Err(SchemaError::InvalidData(format!(
            "aggregate member '{}' does not match the latest source winner",
            member_source_key.to_storage_key()
        )));
    }
    if !source_members.insert(member_source_key) {
        return Err(SchemaError::InvalidData(
            "aggregate repair found duplicate rows for one source member".into(),
        ));
    }
    for (field, contribution) in member.contribution {
        let total = totals
            .get_mut(&field)
            .expect("member metric set matched guard");
        *total = total.checked_add(contribution).ok_or_else(|| {
            SchemaError::InvalidData(format!("aggregate repair overflow for metric '{field}'"))
        })?;
    }
    Ok(())
}
