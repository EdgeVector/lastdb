//! Applying one aggregate source mutation to its targets.

mod guards;
mod member_rules;
mod reads;
mod rules;
mod writes;

use super::*;
use rules::{is_matching_retry, GuardState, PublishPlan, SummaryState};

type DerivedClock = Option<(
    super::super::author_clock::AuthorClockPersistReservation,
    crate::schema::types::MutationAuthorClockState,
)>;

/// Immutable inputs derived from the signed source before any lock is taken.
struct ApplyCtx<'a> {
    storage_prefix: Option<&'a str>,
    origin: WriteOrigin,
    aggregate: AggregateSet,
    source_key: KeyValue,
    source_uuid: String,
    source_pub_key: String,
    incoming_winner: AggregateWinner,
    expected_guard: AggregateGuard,
    guard_key: String,
    guard_pending_key: String,
}

impl MutationManager {
    /// Apply one already-signed source and reconstruct its local member/summary.
    pub(in crate::fold_db_core::mutation_manager) async fn apply_aggregate_source(
        &self,
        source: Mutation,
        storage_prefix: Option<&str>,
        origin: WriteOrigin,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        let ctx = self.build_apply_ctx(&source, storage_prefix, origin)?;
        let _guard_lock = self
            .aggregate_named_lock(format!("aggregate_guard_lock\u{1f}{}", ctx.guard_key))
            .write_owned()
            .await;
        self.with_aggregate_guards(_guard_lock, async {
            let (raw_current, raw_pending, matching_provisional_retry) =
                self.load_raw_guards(&ctx).await?;
            let lock_keys =
                Self::target_lock_keys(&ctx, raw_current.as_ref(), raw_pending.as_ref())?;
            let _target_locks = self.acquire_aggregate_named_locks(lock_keys).await;
            self.with_aggregate_guards(_target_locks, async {
                let gs = self
                    .reconcile_guard_state(
                        &ctx,
                        raw_current,
                        raw_pending,
                        matching_provisional_retry,
                    )
                    .await?;

                self.apply_guarded_source(source, &ctx, gs).await
            })
            .await
        })
        .await
    }

    async fn apply_guarded_source(
        &self,
        mut source: Mutation,
        ctx: &ApplyCtx<'_>,
        gs: GuardState,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        let expected = &ctx.expected_guard;
        let migration_winner =
            gs.source_conflict(expected).is_some() || gs.target_conflict(expected).is_some();
        rules::reject_request_migration(ctx.origin, migration_winner)?;
        if gs.loses_replay_conflict(expected) {
            return self.apply_losing_replay(source, ctx, &gs).await;
        }
        let plan = rules::plan_publish(ctx, &gs);

        let (source_fields, current_source_winner) = self.read_source_fields(ctx, &source).await?;
        let source_existed = !source_fields.is_empty();
        let old_member = self.read_old_member(ctx, &gs).await?;
        let incoming = member_rules::incoming_member(ctx, &source);
        member_rules::check_source_advance(
            &source,
            &source_fields,
            current_source_winner.as_ref(),
            &incoming,
            old_member.as_ref(),
            source_existed,
        )?;

        // Conflict invalidation follows every request-shape and source-value
        // check. A rejected AggregateSet must not stale either target.
        if migration_winner {
            self.invalidate_affected(ctx, &gs).await?;
        }

        let mut summary = self
            .read_summary_state(ctx, &source, &gs, &old_member)
            .await?;

        // A deterministic replay winner can move a source partition or take a
        // target from another partition. The target was durably invalidated
        // above. Overwrite this canonical member as a fresh contribution; the
        // explicit bounded repair recomputes the complete new association
        // before any caller can restore valid=1.
        let replacement_base = (!migration_winner).then_some(old_member.as_ref()).flatten();
        let missing_derived_for_existing_source =
            source_existed && old_member.is_none() && !gs.recovering_first_enrollment;
        let apply = match member_rules::replacement(
            replacement_base,
            &incoming,
            &summary.current_summary,
            missing_derived_for_existing_source,
        ) {
            Ok(apply) => apply,
            Err(error) => return self.refuse_after_invalidate(ctx, error).await,
        };
        let prior_valid = summary.valid && !missing_derived_for_existing_source;
        let association_stable = gs.association_stable(expected);
        let changes_member = matches!(apply, AggregateApply::Applied(_));
        let repairs_guard = plan.publish_forward || plan.publish_target;

        // The complete data generation may span three schemas. A separate,
        // durable invalid marker must precede it because those schema writes
        // cannot become durable atomically after a process stop.
        if changes_member || repairs_guard {
            self.invalidate_affected(ctx, &gs).await?;
            summary.guard_token.clone_from(&source.uuid);
        }
        if repairs_guard {
            self.put_provisional_guards(ctx, &gs, &plan).await?;
        }

        source.aggregate_set = None;
        source.synchronous = Some(true);
        let (data_mutations, derived_clock) =
            self.build_data_mutations(source, ctx, &incoming, apply, &summary.guard_token)?;
        let data_receipt = self
            .write_data_generation(ctx, data_mutations, derived_clock, &summary.guard_token)
            .await?;
        if repairs_guard {
            self.commit_guards(ctx, &gs, plan).await?;
        }
        if changes_member && association_stable && prior_valid {
            self.write_valid_marker(ctx, &summary.guard_token).await?;
        }
        Ok(data_receipt)
    }

    fn build_apply_ctx<'a>(
        &self,
        source: &Mutation,
        storage_prefix: Option<&'a str>,
        origin: WriteOrigin,
    ) -> Result<ApplyCtx<'a>, SchemaError> {
        let aggregate = source.aggregate_set.clone().ok_or_else(|| {
            SchemaError::InvalidData("aggregate apply requires aggregate_set".into())
        })?;
        let (source_key, aggregate) = self.validate_aggregate_contract(source, &aggregate)?;
        let written_at = source.imported_written_at.ok_or_else(|| {
            SchemaError::InvalidData("aggregate source is missing its signed author clock".into())
        })?;
        if source.author_clock_writer_id.is_empty() {
            return Err(SchemaError::InvalidData(
                "aggregate source is missing its signed author writer".into(),
            ));
        }
        let incoming_winner = AggregateWinner {
            logical_counter: source.logical_counter,
            written_at,
            writer_id: source.author_clock_writer_id.clone(),
            mutation_uuid: source.uuid.clone(),
        };
        let expected_guard = AggregateGuard::new(
            source.schema_name.clone(),
            &source_key,
            &aggregate,
            incoming_winner.clone(),
        );
        expected_guard
            .validate()
            .map_err(SchemaError::InvalidData)?;
        let guard_key = aggregate_guard_key(
            storage_prefix,
            &expected_guard.source_schema_name,
            &expected_guard.source_partition_hash,
        );
        let guard_pending_key = format!("{guard_key}:pending");
        Ok(ApplyCtx {
            storage_prefix,
            origin,
            aggregate,
            source_key,
            source_uuid: source.uuid.clone(),
            source_pub_key: source.pub_key.clone(),
            incoming_winner,
            expected_guard,
            guard_key,
            guard_pending_key,
        })
    }
}
