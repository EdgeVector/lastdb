//! Member decisions use source and member values, without database effects.

use super::*;

pub(super) fn incoming_member(ctx: &ApplyCtx<'_>, source: &Mutation) -> AggregateMemberRecord {
    let aggregate = &ctx.aggregate;
    AggregateMemberRecord {
        source_schema_name: source.schema_name.clone(),
        source_key: ctx.source_key.to_storage_key(),
        target_schema_name: aggregate.target_schema_name.clone(),
        target_key: aggregate.target_key_value.to_storage_key(),
        winner: ctx.incoming_winner.clone(),
        metric_fingerprint: metric_fingerprint(aggregate.contribution.keys()),
        contribution: aggregate.contribution.clone(),
    }
}

/// Aggregate member advancement requires at least one changed source value.
/// Equal source values permit an exact signed retry or an unchanged member;
/// a new member winner over those same values receives an explicit refusal.
/// This aggregate rule is separate from normal tip clock advancement.
pub(super) fn check_source_advance(
    source: &Mutation,
    source_fields: &BTreeMap<String, serde_json::Value>,
    current_source_winner: Option<&AggregateWinner>,
    incoming: &AggregateMemberRecord,
    old_member: Option<&AggregateMemberRecord>,
    source_existed: bool,
) -> Result<(), SchemaError> {
    let source_field_advances = source.fields_and_values.iter().any(|(field, value)| {
        source_fields
            .get(field)
            .is_none_or(|current| current != value)
    });
    if source_existed && !source_field_advances {
        let current_source_winner = current_source_winner.ok_or_else(|| {
            SchemaError::InvalidData(
                "aggregate source row has no authoritative current winner".into(),
            )
        })?;
        let exact_source_replay = incoming.winner.cmp_source(current_source_winner).is_eq();
        let member_stays_unchanged =
            old_member.is_some_and(|old| !incoming.winner.cmp_source(&old.winner).is_gt());
        if !exact_source_replay && !member_stays_unchanged {
            return Err(SchemaError::InvalidData(format!(
                "aggregate_set cannot advance its member winner or value over byte-identical source fields (incoming={:?}, current={current_source_winner:?})",
                incoming.winner
            )));
        }
    }
    Ok(())
}

pub(super) fn interpret_member(
    recovering: bool,
    member_fields: BTreeMap<String, serde_json::Value>,
) -> Result<Option<AggregateMemberRecord>, SchemaError> {
    if recovering {
        // A failed first data generation can persist any prefix of the
        // member schema's fixed fields. No committed guard or reverse owner
        // exists, so the same signed intent can safely overwrite the whole
        // canonical row and recompute from a zero target baseline.
        return Ok(None);
    }
    if member_fields.is_empty() {
        return Ok(None);
    }
    if member_fields.len() != AGGREGATE_MEMBER_RESERVED_FIELDS.len() {
        return Err(SchemaError::InvalidData(
            "aggregate member row is partial or collides with an existing row".into(),
        ));
    }
    AggregateMemberRecord::from_fields(&member_fields)
        .map(Some)
        .map_err(SchemaError::InvalidData)
}

/// Missing derived state keeps the captured total until a complete repair.
pub(super) fn replacement(
    replacement_base: Option<&AggregateMemberRecord>,
    incoming: &AggregateMemberRecord,
    current_summary: &BTreeMap<String, i64>,
    missing_derived_for_existing_source: bool,
) -> Result<AggregateApply, SchemaError> {
    if missing_derived_for_existing_source {
        // Persist the member without adding it to an unknown summary generation.
        return Ok(AggregateApply::Applied(current_summary.clone()));
    }
    apply_member_replacement(replacement_base, incoming, current_summary)
        .map_err(SchemaError::InvalidData)
}
