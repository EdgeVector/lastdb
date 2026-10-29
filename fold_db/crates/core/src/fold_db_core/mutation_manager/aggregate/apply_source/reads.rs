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
                self.refuse_after_invalidate(
                    ctx,
                    SchemaError::InvalidData(format!(
                        "aggregate source field '{field}' has unresolved or tombstoned atom '{atom_uuid}'"
                    )),
                )
                .await
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
        match member_rules::interpret_member(recovering, &member_fields) {
            Ok(member) => Ok(member),
            Err(error) => self.refuse_after_invalidate(ctx, error).await,
        }
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
        let summary_fields = match self
            .read_current_row_fields(&summary_probe, &summary_field_names, ctx.storage_prefix)
            .await?
        {
            CurrentRowFields::Absent => BTreeMap::new(),
            CurrentRowFields::Present(fields) => fields,
            CurrentRowFields::Corrupt { field, atom_uuid } => {
                return self
                    .refuse_after_invalidate(
                        ctx,
                        SchemaError::InvalidData(format!(
                            "aggregate summary field '{field}' has unresolved atom '{atom_uuid}'"
                        )),
                    )
                    .await;
            }
        };
        match rules::interpret_summary(
            &expected.metric_fields,
            &source.uuid,
            gs.recovering_first_enrollment,
            old_member.is_some(),
            summary_fields,
        ) {
            Ok(summary) => Ok(summary),
            Err(error) => self.refuse_after_invalidate(ctx, error).await,
        }
    }
}
