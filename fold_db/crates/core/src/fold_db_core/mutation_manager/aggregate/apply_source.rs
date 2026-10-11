//! Applying one aggregate source mutation to its targets.

// lint:file-size-ok one method family per file; helpers of the split apply_aggregate_source
use super::*;

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

/// Guard rows after pending reservations were reconciled.
struct GuardState {
    current: Option<AggregateGuard>,
    target_owner: Option<AggregateGuard>,
    target_guard_key: String,
    target_pending_key: String,
    recovering_first_enrollment: bool,
}

impl GuardState {
    fn source_conflict(&self, expected: &AggregateGuard) -> Option<&AggregateGuard> {
        self.current
            .as_ref()
            .filter(|current| !current.same_association(expected))
    }

    fn target_conflict(&self, expected: &AggregateGuard) -> Option<&AggregateGuard> {
        self.target_owner
            .as_ref()
            .filter(|current| !current.same_association(expected))
    }

    fn loses_replay_conflict(&self, expected: &AggregateGuard) -> bool {
        self.source_conflict(expected)
            .into_iter()
            .chain(self.target_conflict(expected))
            .any(|current| !expected.cmp_enrollment(current).is_gt())
    }

    fn association_stable(&self, expected: &AggregateGuard) -> bool {
        let stable = |guard: &Option<AggregateGuard>| {
            guard
                .as_ref()
                .is_some_and(|guard| guard.committed && guard.same_association(expected))
        };
        stable(&self.current) && stable(&self.target_owner)
    }

    fn affected<'a>(&'a self, expected: &'a AggregateGuard) -> Vec<&'a AggregateGuard> {
        let mut affected: Vec<&AggregateGuard> = self.current.iter().collect();
        affected.extend(self.target_owner.iter());
        affected.push(expected);
        affected
    }
}

/// Which guard rows this apply must publish, and which old row to drop.
struct PublishPlan {
    desired_guard: AggregateGuard,
    publish_forward: bool,
    publish_target: bool,
    old_target_guard_key: Option<String>,
}

/// Summary row state read before the data generation.
struct SummaryState {
    current_summary: BTreeMap<String, i64>,
    valid: bool,
    guard_token: String,
}

fn is_matching_retry(pending: &AggregateGuard, expected: &AggregateGuard) -> bool {
    pending.same_association(expected)
        && pending.cmp_enrollment(expected) == std::cmp::Ordering::Equal
}

impl MutationManager {
    /// Apply one already-signed source and reconstruct its local member/summary.
    pub(in crate::fold_db_core::mutation_manager) async fn apply_aggregate_source(
        &self,
        mut source: Mutation,
        storage_prefix: Option<&str>,
        origin: WriteOrigin,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        let ctx = self.build_apply_ctx(&source, storage_prefix, origin)?;
        let _guard_lock = self
            .aggregate_named_lock(format!("aggregate_guard_lock\u{1f}{}", ctx.guard_key))
            .lock_owned()
            .await;
        let (raw_current, raw_pending, matching_provisional_retry) =
            self.load_raw_guards(&ctx).await?;
        let lock_keys = Self::target_lock_keys(&ctx, raw_current.as_ref(), raw_pending.as_ref());
        let _target_locks = self.acquire_aggregate_named_locks(lock_keys).await;
        let gs = self
            .reconcile_guard_state(&ctx, raw_current, raw_pending, matching_provisional_retry)
            .await?;

        let expected = &ctx.expected_guard;
        let migration_winner =
            gs.source_conflict(expected).is_some() || gs.target_conflict(expected).is_some();
        Self::reject_request_migration(origin, migration_winner)?;
        if gs.loses_replay_conflict(expected) {
            return self.apply_losing_replay(source, &ctx, &gs).await;
        }
        let plan = Self::plan_publish(&ctx, &gs);

        let (source_fields, current_source_winner) = self.read_source_fields(&ctx, &source).await?;
        let source_existed = !source_fields.is_empty();
        let old_member = self.read_old_member(&ctx, &gs).await?;
        let incoming = Self::build_incoming_member(&ctx, &source);
        Self::check_source_advance(
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
            self.invalidate_affected(&ctx, &gs).await?;
        }

        let mut summary = self
            .read_summary_state(&ctx, &source, &gs, &old_member)
            .await?;

        // A deterministic replay winner can move a source partition or take a
        // target from another partition. The target was durably invalidated
        // above. Overwrite this canonical member as a fresh contribution; the
        // explicit bounded repair recomputes the complete new association
        // before any caller can restore valid=1.
        let replacement_base = (!migration_winner).then_some(old_member.as_ref()).flatten();
        let missing_derived_for_existing_source =
            source_existed && old_member.is_none() && !gs.recovering_first_enrollment;
        let apply = self
            .compute_member_apply(
                &ctx,
                replacement_base,
                &incoming,
                &summary.current_summary,
                missing_derived_for_existing_source,
            )
            .await?;
        let prior_valid = summary.valid && !missing_derived_for_existing_source;
        let association_stable = gs.association_stable(expected);
        let changes_member = matches!(apply, AggregateApply::Applied(_));
        let repairs_guard = plan.publish_forward || plan.publish_target;

        // The complete data generation may span three schemas. A separate,
        // durable invalid marker must precede it because those schema writes
        // cannot become durable atomically after a process stop.
        if changes_member || repairs_guard {
            self.invalidate_affected(&ctx, &gs).await?;
            summary.guard_token.clone_from(&source.uuid);
        }
        if repairs_guard {
            self.put_provisional_guards(&ctx, &gs, &plan).await?;
        }

        source.aggregate_set = None;
        source.synchronous = Some(true);
        let (data_mutations, derived_clock) =
            self.build_data_mutations(source, &ctx, &incoming, apply, &summary.guard_token)?;
        let data_receipt = self
            .write_data_generation(&ctx, data_mutations, derived_clock, &summary.guard_token)
            .await?;
        if repairs_guard {
            self.commit_guards(&ctx, &gs, plan).await?;
        }
        if changes_member && association_stable && prior_valid {
            self.write_valid_marker(&ctx, &summary.guard_token).await?;
        }
        Ok(data_receipt)
    }

    fn reject_request_migration(
        origin: WriteOrigin,
        migration_winner: bool,
    ) -> Result<(), SchemaError> {
        if matches!(origin, WriteOrigin::Request) && migration_winner {
            return Err(SchemaError::InvalidData(
                "aggregate source partition or target is already enrolled with a different association"
                    .into(),
            ));
        }
        Ok(())
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

    /// Read and validate the committed and pending source guards.
    async fn load_raw_guards(
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

    fn target_lock_keys(
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
    async fn reconcile_guard_state(
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

    async fn reconcile_target_pending(
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

    async fn invalidate_affected(
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
    async fn invalidate_expected(&self, ctx: &ApplyCtx<'_>) -> Result<(), SchemaError> {
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
    async fn apply_losing_replay(
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

    fn plan_publish(ctx: &ApplyCtx<'_>, gs: &GuardState) -> PublishPlan {
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

    /// Read source existence before this mutation changes the source row.
    /// An absent member is safe to add to a ready summary only for a truly
    /// new source. If the source already exists, the missing member is lost
    /// derived state and the bounded repair must rebuild the total.
    async fn read_source_fields(
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

    async fn read_old_member(
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

    fn build_incoming_member(ctx: &ApplyCtx<'_>, source: &Mutation) -> AggregateMemberRecord {
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
    fn check_source_advance(
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

    async fn read_summary_state(
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

    async fn compute_member_apply(
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

    /// Reservations never replace the last committed guards. Restart discards
    /// these keys and leaves every affected target invalid.
    async fn put_provisional_guards(
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

    fn build_data_mutations(
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
            member.synchronous = Some(true);
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
            summary.synchronous = Some(true);
            let mut derived_owned = vec![member, summary];
            derived_clock = self.prepare_mutation_author_clocks(&mut derived_owned)?;
            data_mutations.extend(derived_owned);
        }
        Ok((data_mutations, derived_clock))
    }

    async fn write_data_generation(
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

    async fn commit_guards(
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
    async fn write_valid_marker(
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
        marker.synchronous = Some(true);
        let marker_clock =
            self.prepare_mutation_author_clocks(std::slice::from_mut(&mut marker))?;
        if let Some((reservation, state)) = marker_clock {
            reservation.submit_and_wait(state).await?;
        }
        let marker_result = self
            .write_mutations_batch_inner(vec![marker], ctx.storage_prefix, WriteOrigin::Replay)
            .await;
        Self::require_durable_receipt(marker_result?, "valid marker")?;
        Ok(())
    }
}
