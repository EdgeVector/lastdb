//! Source, member and summary reads that feed one aggregate source apply.

use super::*;

impl MutationManager {
    /// Read source existence before this mutation changes the source row.
    /// An absent member is safe to add to a ready summary only for a truly
    /// new source. If the source already exists, the missing member is lost
    /// derived state and the bounded repair must rebuild the total.
    pub(super) async fn read_source_fields(
        &self,
        ctx: &ApplyCtx<'_>,
        source: &Mutation,
    ) -> Result<(BTreeMap<String, serde_json::Value>, Option<AggregateWinner>), SchemaError> {
        let source_schema = self
            .schema_manager
            .get_schema_metadata(&source.schema_name)?
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "aggregate source schema '{}' not found",
                    source.schema_name
                ))
            })?;
        let source_field_names: Vec<String> =
            source_schema.runtime_fields.keys().cloned().collect();
        let source_probe =
            Self::aggregate_probe(source.schema_name.clone(), ctx.source_key.clone());
        let (source_row, current_source_winner) = self
            .read_current_row_fields_with_winner(
                &source_probe,
                &source_field_names,
                ctx.storage_prefix,
            )
            .await?;
        match source_row {
            CurrentRowFields::Absent => Ok((BTreeMap::new(), current_source_winner)),
            CurrentRowFields::Present(fields) => Ok((fields, current_source_winner)),
            CurrentRowFields::Corrupt { field, atom_uuid } => {
                self.invalidate_expected(ctx).await?;
                Err(SchemaError::InvalidData(format!(
                    "aggregate source field '{field}' has unresolved or tombstoned atom '{atom_uuid}'"
                )))
            }
        }
    }

    pub(super) async fn read_old_member(
        &self,
        ctx: &ApplyCtx<'_>,
        gs: &GuardState,
    ) -> Result<Option<AggregateMemberRecord>, SchemaError> {
        let recovering = gs.recovering_first_enrollment;
        let member_probe = Self::aggregate_probe(
            ctx.aggregate.member_schema_name.clone(),
            ctx.aggregate.member_key_value.clone(),
        );
        let member_field_names: Vec<String> = AGGREGATE_MEMBER_RESERVED_FIELDS
            .iter()
            .map(|field| (*field).to_string())
            .collect();
        let member_fields = match self
            .read_current_row_fields(&member_probe, &member_field_names, ctx.storage_prefix)
            .await?
        {
            CurrentRowFields::Absent => BTreeMap::new(),
            CurrentRowFields::Present(fields) => fields,
            CurrentRowFields::Corrupt { field, atom_uuid } => {
                self.invalidate_expected(ctx).await?;
                if recovering {
                    BTreeMap::new()
                } else {
                    return Err(SchemaError::InvalidData(format!(
                        "aggregate member field '{field}' has unresolved atom '{atom_uuid}'"
                    )));
                }
            }
        };
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
        if member_fields.len() != member_field_names.len() {
            self.invalidate_expected(ctx).await?;
            return Err(SchemaError::InvalidData(
                "aggregate member row is partial or collides with an existing row".into(),
            ));
        }
        match AggregateMemberRecord::from_fields(&member_fields) {
            Ok(member) => Ok(Some(member)),
            Err(error) => {
                self.invalidate_expected(ctx).await?;
                Err(SchemaError::InvalidData(error))
            }
        }
    }

    pub(super) fn build_incoming_member(
        ctx: &ApplyCtx<'_>,
        source: &Mutation,
    ) -> AggregateMemberRecord {
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

    /// Aggregate values are a materialized function of the persisted source
    /// row. Fold's molecule layer intentionally keeps a same-atom write as
    /// a no-op, including its old winner. Therefore, a newer member winner
    /// is valid only when this mutation changes at least one live source
    /// field. Exact signed retries and stale intents remain no-ops.
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

    pub(super) async fn read_summary_state(
        &self,
        ctx: &ApplyCtx<'_>,
        source: &Mutation,
        gs: &GuardState,
        old_member: &Option<AggregateMemberRecord>,
    ) -> Result<SummaryState, SchemaError> {
        let expected = &ctx.expected_guard;
        let mut summary_field_names = expected.metric_fields.clone();
        summary_field_names.push(AGGREGATE_VALID_FIELD.into());
        summary_field_names.push(AGGREGATE_GUARD_TOKEN_FIELD.into());
        let summary_probe = Self::aggregate_probe(
            ctx.aggregate.target_schema_name.clone(),
            ctx.aggregate.target_key_value.clone(),
        );
        let mut summary_fields = match self
            .read_current_row_fields(&summary_probe, &summary_field_names, ctx.storage_prefix)
            .await?
        {
            CurrentRowFields::Absent => BTreeMap::new(),
            CurrentRowFields::Present(fields) => fields,
            CurrentRowFields::Corrupt { field, atom_uuid } => {
                self.invalidate_expected(ctx).await?;
                return Err(SchemaError::InvalidData(format!(
                    "aggregate summary field '{field}' has unresolved atom '{atom_uuid}'"
                )));
            }
        };
        if gs.recovering_first_enrollment {
            summary_fields = expected
                .metric_fields
                .iter()
                .cloned()
                .map(|field| (field, serde_json::Value::from(0)))
                .collect();
            summary_fields.insert(AGGREGATE_VALID_FIELD.into(), serde_json::Value::from(0));
            summary_fields.insert(
                AGGREGATE_GUARD_TOKEN_FIELD.into(),
                serde_json::Value::String(source.uuid.clone()),
            );
        }
        let metrics_complete = expected.metric_fields.iter().all(|field| {
            summary_fields
                .get(field)
                .and_then(serde_json::Value::as_i64)
                .is_some()
        });
        let mut current_summary = BTreeMap::new();
        for field in &expected.metric_fields {
            current_summary.insert(
                field.clone(),
                summary_fields
                    .get(field)
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0),
            );
        }
        let valid_value = summary_fields
            .get(AGGREGATE_VALID_FIELD)
            .and_then(serde_json::Value::as_i64);
        let old_token = summary_fields
            .get(AGGREGATE_GUARD_TOKEN_FIELD)
            .and_then(serde_json::Value::as_str);
        let status_complete = matches!(valid_value, Some(0 | 1)) && old_token.is_some();
        let summary_missing = summary_fields.is_empty();
        if !summary_missing && (!metrics_complete || !status_complete) {
            self.invalidate_expected(ctx).await?;
            return Err(SchemaError::InvalidData(
                "aggregate summary is partial or malformed; repair is required".into(),
            ));
        }
        let valid = if summary_missing {
            false
        } else {
            valid_value == Some(1)
        };
        let mut guard_token = if summary_missing {
            source.uuid.clone()
        } else {
            old_token.expect("checked token").to_string()
        };
        // A valid summary without this member is a missing-member condition.
        // Keep the value repairable, but never keep it ready.
        if old_member.is_none() && !summary_fields.is_empty() && valid {
            guard_token.clone_from(&source.uuid);
        }
        Ok(SummaryState {
            current_summary,
            valid,
            guard_token,
        })
    }

    pub(super) async fn compute_member_apply(
        &self,
        ctx: &ApplyCtx<'_>,
        replacement_base: Option<&AggregateMemberRecord>,
        incoming: &AggregateMemberRecord,
        current_summary: &BTreeMap<String, i64>,
        missing_derived_for_existing_source: bool,
    ) -> Result<AggregateApply, SchemaError> {
        if missing_derived_for_existing_source {
            // Persist the replacement member, but do not add it to an unknown
            // summary generation. Repair proves the complete source/member
            // bijection and recomputes the bounded total before finalization.
            return Ok(AggregateApply::Applied(current_summary.clone()));
        }
        match apply_member_replacement(replacement_base, incoming, current_summary) {
            Ok(apply) => Ok(apply),
            Err(error) => {
                self.invalidate_expected(ctx).await?;
                Err(SchemaError::InvalidData(error))
            }
        }
    }
}
