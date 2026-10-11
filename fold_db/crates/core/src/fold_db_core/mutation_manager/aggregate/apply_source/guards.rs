//! Guard reconciliation, invalidation and publish planning for an aggregate source apply.

use super::*;

impl MutationManager {
    /// Read and validate the committed and pending source guards.
    pub(super) async fn load_raw_guards(
        &self,
        ctx: &ApplyCtx<'_>,
    ) -> Result<(Option<AggregateGuard>, Option<AggregateGuard>, bool), SchemaError> {
        let raw_current_guard = self
            .db_ops
            .metadata()
            .get_typed::<AggregateGuard>(&ctx.guard_key)
            .await?;
        if let Some(current) = &raw_current_guard {
            current.validate().map_err(SchemaError::InvalidData)?;
        }
        let raw_pending_guard = self
            .db_ops
            .metadata()
            .get_typed::<AggregateGuard>(&ctx.guard_pending_key)
            .await?;
        if let Some(pending) = &raw_pending_guard {
            pending.validate().map_err(SchemaError::InvalidData)?;
        }
        let matching_provisional_retry = raw_pending_guard
            .as_ref()
            .is_some_and(|pending| is_matching_retry(pending, &ctx.expected_guard));
        Ok((
            raw_current_guard,
            raw_pending_guard,
            matching_provisional_retry,
        ))
    }

    pub(super) fn target_lock_keys(
        ctx: &ApplyCtx<'_>,
        raw_current: Option<&AggregateGuard>,
        raw_pending: Option<&AggregateGuard>,
    ) -> Vec<String> {
        let prefix = ctx.storage_prefix;
        let mut keys = vec![Self::aggregate_target_lock_key(
            prefix,
            &ctx.aggregate.target_schema_name,
            &ctx.aggregate.target_key_value,
        )];
        let replay_conflict =
            raw_current.filter(|current| !current.same_association(&ctx.expected_guard));
        if let Some(current) = replay_conflict {
            keys.push(Self::aggregate_target_lock_key(
                prefix,
                &current.target_schema_name,
                &KeyValue::from_storage_key(&current.target_key),
            ));
        }
        if let Some(pending) = raw_pending {
            keys.push(Self::aggregate_target_lock_key(
                prefix,
                &pending.target_schema_name,
                &KeyValue::from_storage_key(&pending.target_key),
            ));
        }
        keys
    }

    /// Reconcile pending reservations, then the committed guards and owner.
    pub(super) async fn reconcile_guard_state(
        &self,
        ctx: &ApplyCtx<'_>,
        raw_current: Option<AggregateGuard>,
        raw_pending: Option<AggregateGuard>,
        matching_provisional_retry: bool,
    ) -> Result<GuardState, SchemaError> {
        let prefix = ctx.storage_prefix;
        let expected = &ctx.expected_guard;
        if let Some(pending) = raw_pending {
            let pending_target_key = aggregate_target_guard_key(
                prefix,
                &pending.target_schema_name,
                &pending.target_key,
            );
            let pending_target_key = format!("{pending_target_key}:pending");
            if !is_matching_retry(&pending, expected) {
                self.reconcile_aggregate_guard(&ctx.guard_pending_key, pending, prefix)
                    .await?;
            }
            self.reconcile_target_pending(&pending_target_key, expected, prefix)
                .await?;
        }
        let current = match raw_current {
            Some(current) => {
                self.reconcile_aggregate_guard(&ctx.guard_key, current, prefix)
                    .await?
            }
            None => None,
        };
        let target_guard_key =
            aggregate_target_guard_key(prefix, &expected.target_schema_name, &expected.target_key);
        let target_pending_key = format!("{target_guard_key}:pending");
        self.reconcile_target_pending(&target_pending_key, expected, prefix)
            .await?;
        let target_owner = self
            .read_active_target_guard(&target_guard_key, prefix)
            .await?;
        let recovering_first_enrollment =
            matching_provisional_retry && current.is_none() && target_owner.is_none();
        Ok(GuardState {
            current,
            target_owner,
            target_guard_key,
            target_pending_key,
            recovering_first_enrollment,
        })
    }

    pub(super) async fn reconcile_target_pending(
        &self,
        pending_key: &str,
        expected: &AggregateGuard,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        if let Some(target_pending) = self
            .db_ops
            .metadata()
            .get_typed::<AggregateGuard>(pending_key)
            .await?
        {
            if !is_matching_retry(&target_pending, expected) {
                self.reconcile_aggregate_guard(pending_key, target_pending, storage_prefix)
                    .await?;
            }
        }
        Ok(())
    }

    pub(super) async fn invalidate_affected(
        &self,
        ctx: &ApplyCtx<'_>,
        gs: &GuardState,
    ) -> Result<(), SchemaError> {
        self.invalidate_guard_targets_durable(
            &gs.affected(&ctx.expected_guard),
            &ctx.source_uuid,
            &ctx.source_pub_key,
            ctx.storage_prefix,
            ctx.origin,
        )
        .await
    }

    /// Invalidate the expected guard's target after a failed validation.
    pub(super) async fn invalidate_expected(&self, ctx: &ApplyCtx<'_>) -> Result<(), SchemaError> {
        self.invalidate_guard_targets_durable(
            &[&ctx.expected_guard],
            &ctx.source_uuid,
            &ctx.source_pub_key,
            ctx.storage_prefix,
            ctx.origin,
        )
        .await
    }

    /// A late loser must not invalidate the unchanged target winner. It can
    /// only stale a different target already owned by this same source
    /// partition, and only while that reverse owner is current.
    pub(super) async fn apply_losing_replay(
        &self,
        mut source: Mutation,
        ctx: &ApplyCtx<'_>,
        gs: &GuardState,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        let prefix = ctx.storage_prefix;
        let mut affected = Vec::new();
        if let Some(current) = gs.source_conflict(&ctx.expected_guard) {
            let reverse_key = aggregate_target_guard_key(
                prefix,
                &current.target_schema_name,
                &current.target_key,
            );
            if self
                .read_active_target_guard(&reverse_key, prefix)
                .await?
                .as_ref()
                .is_some_and(|reverse| is_matching_retry(reverse, current))
            {
                affected.push(current);
            }
        }
        self.invalidate_guard_targets_durable(
            &affected,
            &source.uuid,
            &source.pub_key,
            prefix,
            ctx.origin,
        )
        .await?;
        source.aggregate_set = None;
        source.synchronous = Some(true);
        let receipt = self
            .write_mutations_batch_inner(vec![source], prefix, ctx.origin)
            .await?;
        Self::require_durable_receipt(receipt, "losing replay")
    }

    pub(super) fn plan_publish(ctx: &ApplyCtx<'_>, gs: &GuardState) -> PublishPlan {
        let expected = &ctx.expected_guard;
        let old_target_guard_key = gs.current.as_ref().and_then(|current| {
            let old_key = aggregate_target_guard_key(
                ctx.storage_prefix,
                &current.target_schema_name,
                &current.target_key,
            );
            (old_key != gs.target_guard_key).then_some(old_key)
        });
        let mut desired_guard = expected.clone();
        for existing in gs.current.iter().chain(gs.target_owner.iter()) {
            if existing.same_association(expected)
                && existing.cmp_enrollment(&desired_guard).is_gt()
            {
                desired_guard = existing.clone();
            }
        }
        let advances = |current: &AggregateGuard| {
            !current.same_association(&desired_guard)
                || desired_guard.cmp_enrollment(current).is_gt()
        };
        let publish_forward = gs.current.as_ref().is_none_or(advances);
        let publish_target = gs.target_owner.as_ref().is_none_or(advances);
        PublishPlan {
            desired_guard,
            publish_forward,
            publish_target,
            old_target_guard_key,
        }
    }
}
