//! Guard and data-generation writes for one aggregate source apply.

use super::super::rules::{derived_mutation, summary_mutation};
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
        self.put_durable_copies(
            [
                ctx.guard_pending_key.as_str(),
                gs.target_pending_key.as_str(),
            ],
            &provisional,
        )
        .await
    }

    /// Write one value to each metadata key, in order. A failed key stops the rest.
    async fn put_durable_copies<T: serde::Serialize + Send + Sync>(
        &self,
        keys: [&str; 2],
        value: &T,
    ) -> Result<(), SchemaError> {
        for key in keys {
            self.db_ops.metadata().put_typed_durable(key, value).await?;
        }
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
            let member = derived_mutation(
                aggregate.member_schema_name.clone(),
                incoming.to_fields(),
                aggregate.member_key_value.clone(),
                data_mutations[0].pub_key.clone(),
                MutationType::Update,
            );
            let summary_values: HashMap<String, serde_json::Value> = next_summary
                .into_iter()
                .map(|(field, value)| (field, serde_json::Value::from(value)))
                .collect();
            let summary = summary_mutation(
                aggregate.target_schema_name.clone(),
                aggregate.target_key_value.clone(),
                summary_values,
                false,
                guard_token.to_string(),
                data_mutations[0].pub_key.clone(),
            );
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
            Ok(receipt) => Self::require_durable_receipt(receipt, "data generation"),
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
        self.put_durable_copies(
            [ctx.guard_key.as_str(), gs.target_guard_key.as_str()],
            &committed,
        )
        .await?;
        self.delete_durable_copies([
            ctx.guard_pending_key.as_str(),
            gs.target_pending_key.as_str(),
        ])
        .await?;
        if let Some(old_target_guard_key) = plan.old_target_guard_key {
            self.db_ops
                .metadata()
                .delete_typed_durable(&old_target_guard_key)
                .await?;
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
        let marker = summary_mutation(
            ctx.aggregate.target_schema_name.clone(),
            ctx.aggregate.target_key_value.clone(),
            HashMap::new(),
            true,
            guard_token.to_string(),
            String::new(),
        );
        self.commit_clocked_mutations(
            vec![marker],
            ctx.storage_prefix,
            WriteOrigin::Replay,
            "valid marker",
            false,
        )
        .await?;
        Ok(())
    }
}
