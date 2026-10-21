//! Guard and data-generation writes for one aggregate source apply.

use super::*;

impl MutationManager {
    /// Reservations never replace the last committed guards. Restart discards
    /// these keys and leaves every affected target invalid.
    pub(super) async fn put_provisional_guards(
        &self,
        ctx: &ApplyCtx<'_>,
        gs: &GuardState,
        plan: &PublishPlan,
    ) -> Result<(), SchemaError> {
        let provisional = {
            let mut guard = plan.desired_guard.clone();
            guard.committed = false;
            guard
        };
        self.db_ops
            .metadata()
            .put_typed_durable(&ctx.guard_pending_key, &provisional)
            .await?;
        self.db_ops
            .metadata()
            .put_typed_durable(&gs.target_pending_key, &provisional)
            .await?;
        Ok(())
    }

    pub(super) fn build_data_mutations(
        &self,
        source: Mutation,
        ctx: &ApplyCtx<'_>,
        incoming: &AggregateMemberRecord,
        apply: AggregateApply,
        guard_token: &str,
    ) -> Result<(Vec<Mutation>, DerivedClock), SchemaError> {
        let aggregate = &ctx.aggregate;
        let mut data_mutations = vec![source];
        let mut derived_clock = None;
        if let AggregateApply::Applied(next_summary) = apply {
            let mut member = Mutation::new(
                aggregate.member_schema_name.clone(),
                incoming.to_fields(),
                aggregate.member_key_value.clone(),
                data_mutations[0].pub_key.clone(),
                MutationType::Update,
            );
            member.aggregate_derived_internal = true;
            let mut summary_values: HashMap<String, serde_json::Value> = next_summary
                .into_iter()
                .map(|(field, value)| (field, serde_json::Value::from(value)))
                .collect();
            summary_values.insert(AGGREGATE_VALID_FIELD.into(), serde_json::Value::from(0));
            summary_values.insert(
                AGGREGATE_GUARD_TOKEN_FIELD.into(),
                serde_json::Value::String(guard_token.to_string()),
            );
            let mut summary = Mutation::new(
                aggregate.target_schema_name.clone(),
                summary_values,
                aggregate.target_key_value.clone(),
                data_mutations[0].pub_key.clone(),
                MutationType::Update,
            );
            summary.aggregate_derived_internal = true;
            let mut derived_owned = vec![member, summary];
            derived_clock = self.prepare_mutation_author_clocks(&mut derived_owned)?;
            data_mutations.extend(derived_owned);
        }
        Ok((data_mutations, derived_clock))
    }

    pub(super) async fn write_data_generation(
        &self,
        ctx: &ApplyCtx<'_>,
        data_mutations: Vec<Mutation>,
        derived_clock: DerivedClock,
        guard_token: &str,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        if let Some((reservation, state)) = derived_clock {
            reservation.submit_and_wait(state).await?;
        }
        let data_result = self
            .write_mutations_batch_inner(data_mutations, ctx.storage_prefix, ctx.origin)
            .await;
        match data_result {
            Ok(receipt) => Ok(receipt),
            Err(error) => {
                // A flush failure can follow resident publication. Persist the
                // invalid marker again and retain the matching reservations.
                // A retry can then complete the same logical generation; a
                // conflicting request discards the provisional association.
                let _ = self
                    .invalidate_guard_targets_durable(
                        &[&ctx.expected_guard],
                        guard_token,
                        "",
                        ctx.storage_prefix,
                        WriteOrigin::Replay,
                    )
                    .await;
                Err(error)
            }
        }
    }

    pub(super) async fn commit_guards(
        &self,
        ctx: &ApplyCtx<'_>,
        gs: &GuardState,
        plan: PublishPlan,
    ) -> Result<(), SchemaError> {
        let committed = plan.desired_guard.committed();
        let metadata = self.db_ops.metadata();
        metadata
            .put_typed_durable(&ctx.guard_key, &committed)
            .await?;
        metadata
            .put_typed_durable(&gs.target_guard_key, &committed)
            .await?;
        metadata
            .delete_typed_durable(&ctx.guard_pending_key)
            .await?;
        metadata
            .delete_typed_durable(&gs.target_pending_key)
            .await?;
        if let Some(old_target_guard_key) = plan.old_target_guard_key {
            metadata.delete_typed_durable(&old_target_guard_key).await?;
        }
        Ok(())
    }

    /// A stable association can retain its prior ready state. The marker is
    /// a separate, single-schema durable phase and therefore can never race
    /// ahead of a partial source/member/metric generation. `guard_token` has
    /// already advanced to this write's identity (`changes_member` implies the
    /// advance at the invalidation step), so the marker persists that value.
    pub(super) async fn write_valid_marker(
        &self,
        ctx: &ApplyCtx<'_>,
        guard_token: &str,
    ) -> Result<(), SchemaError> {
        let mut marker = Mutation::new(
            ctx.aggregate.target_schema_name.clone(),
            HashMap::from([
                (AGGREGATE_VALID_FIELD.into(), serde_json::Value::from(1)),
                (
                    AGGREGATE_GUARD_TOKEN_FIELD.into(),
                    serde_json::Value::String(guard_token.to_string()),
                ),
            ]),
            ctx.aggregate.target_key_value.clone(),
            String::new(),
            MutationType::Update,
        );
        marker.aggregate_derived_internal = true;
        let marker_clock =
            self.prepare_mutation_author_clocks(std::slice::from_mut(&mut marker))?;
        if let Some((reservation, state)) = marker_clock {
            reservation.submit_and_wait(state).await?;
        }
        self.write_mutations_batch_inner(vec![marker], ctx.storage_prefix, WriteOrigin::Replay)
            .await?;
        Ok(())
    }
}
