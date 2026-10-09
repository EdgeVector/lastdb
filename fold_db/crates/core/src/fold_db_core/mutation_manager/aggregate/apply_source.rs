//! Applying one aggregate source mutation to its targets.
// lint:file-size-ok moved verbatim from aggregate.rs; one method family per file

use super::*;

impl MutationManager {
    /// Apply one already-signed source and reconstruct its local member/summary.
    // lint:fn-size-ok verbatim move from aggregate.rs; splitting this function is separate work
    pub(in crate::fold_db_core::mutation_manager) async fn apply_aggregate_source(
        &self,
        mut source: Mutation,
        storage_prefix: Option<&str>,
        origin: WriteOrigin,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        let aggregate = source.aggregate_set.clone().ok_or_else(|| {
            SchemaError::InvalidData("aggregate apply requires aggregate_set".into())
        })?;
        let (source_key, aggregate) = self.validate_aggregate_contract(&source, &aggregate)?;
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
        let _guard_lock = self
            .aggregate_named_lock(format!("aggregate_guard_lock\u{1f}{guard_key}"))
            .lock_owned()
            .await;
        let raw_current_guard = self
            .db_ops
            .metadata()
            .get_typed::<AggregateGuard>(&guard_key)
            .await?;
        if let Some(current) = &raw_current_guard {
            current.validate().map_err(SchemaError::InvalidData)?;
        }
        let raw_pending_guard = self
            .db_ops
            .metadata()
            .get_typed::<AggregateGuard>(&guard_pending_key)
            .await?;
        if let Some(pending) = &raw_pending_guard {
            pending.validate().map_err(SchemaError::InvalidData)?;
        }
        let matching_provisional_retry = raw_pending_guard.as_ref().is_some_and(|pending| {
            pending.same_association(&expected_guard)
                && pending.cmp_enrollment(&expected_guard) == std::cmp::Ordering::Equal
        });

        let new_target_lock_key = Self::aggregate_target_lock_key(
            storage_prefix,
            &aggregate.target_schema_name,
            &aggregate.target_key_value,
        );
        let mut target_lock_keys = vec![new_target_lock_key];
        let replay_conflict = raw_current_guard
            .as_ref()
            .filter(|current| !current.same_association(&expected_guard));
        if let Some(current) = replay_conflict {
            target_lock_keys.push(Self::aggregate_target_lock_key(
                storage_prefix,
                &current.target_schema_name,
                &KeyValue::from_storage_key(&current.target_key),
            ));
        }
        if let Some(pending) = &raw_pending_guard {
            target_lock_keys.push(Self::aggregate_target_lock_key(
                storage_prefix,
                &pending.target_schema_name,
                &KeyValue::from_storage_key(&pending.target_key),
            ));
        }
        let _target_locks = self.acquire_aggregate_named_locks(target_lock_keys).await;
        if let Some(pending) = raw_pending_guard {
            let pending_target_key = aggregate_target_guard_key(
                storage_prefix,
                &pending.target_schema_name,
                &pending.target_key,
            );
            let pending_target_key = format!("{pending_target_key}:pending");
            let matching_retry = pending.same_association(&expected_guard)
                && pending.cmp_enrollment(&expected_guard) == std::cmp::Ordering::Equal;
            if !matching_retry {
                self.reconcile_aggregate_guard(&guard_pending_key, pending, storage_prefix)
                    .await?;
            }
            if let Some(target_pending) = self
                .db_ops
                .metadata()
                .get_typed::<AggregateGuard>(&pending_target_key)
                .await?
            {
                let matching_retry = target_pending.same_association(&expected_guard)
                    && target_pending.cmp_enrollment(&expected_guard) == std::cmp::Ordering::Equal;
                if !matching_retry {
                    self.reconcile_aggregate_guard(
                        &pending_target_key,
                        target_pending,
                        storage_prefix,
                    )
                    .await?;
                }
            }
        }
        let current_guard = match raw_current_guard {
            Some(current) => {
                self.reconcile_aggregate_guard(&guard_key, current, storage_prefix)
                    .await?
            }
            None => None,
        };
        let target_guard_key = aggregate_target_guard_key(
            storage_prefix,
            &expected_guard.target_schema_name,
            &expected_guard.target_key,
        );
        let target_pending_key = format!("{target_guard_key}:pending");
        if let Some(target_pending) = self
            .db_ops
            .metadata()
            .get_typed::<AggregateGuard>(&target_pending_key)
            .await?
        {
            let matching_retry = target_pending.same_association(&expected_guard)
                && target_pending.cmp_enrollment(&expected_guard) == std::cmp::Ordering::Equal;
            if !matching_retry {
                self.reconcile_aggregate_guard(&target_pending_key, target_pending, storage_prefix)
                    .await?;
            }
        }
        let target_owner = self
            .read_active_target_guard(&target_guard_key, storage_prefix)
            .await?;
        let recovering_first_enrollment =
            matching_provisional_retry && current_guard.is_none() && target_owner.is_none();

        let source_conflict = current_guard
            .as_ref()
            .filter(|current| !current.same_association(&expected_guard));
        let target_conflict = target_owner
            .as_ref()
            .filter(|current| !current.same_association(&expected_guard));
        if matches!(origin, WriteOrigin::Request)
            && (source_conflict.is_some() || target_conflict.is_some())
        {
            return Err(SchemaError::InvalidData(
                "aggregate source partition or target is already enrolled with a different association"
                    .into(),
            ));
        }

        let loses_replay_conflict = source_conflict
            .into_iter()
            .chain(target_conflict)
            .any(|current| !expected_guard.cmp_enrollment(current).is_gt());
        if loses_replay_conflict {
            // A late loser must not invalidate the unchanged target winner.
            // It can only stale a different target already owned by this same
            // source partition, and only while that reverse owner is current.
            let mut affected = Vec::new();
            if let Some(current) = current_guard
                .as_ref()
                .filter(|current| !current.same_association(&expected_guard))
            {
                let reverse_key = aggregate_target_guard_key(
                    storage_prefix,
                    &current.target_schema_name,
                    &current.target_key,
                );
                if self
                    .read_active_target_guard(&reverse_key, storage_prefix)
                    .await?
                    .as_ref()
                    .is_some_and(|reverse| {
                        reverse.same_association(current)
                            && reverse.cmp_enrollment(current) == std::cmp::Ordering::Equal
                    })
                {
                    affected.push(current);
                }
            }
            self.invalidate_guard_targets_durable(
                &affected,
                &source.uuid,
                &source.pub_key,
                storage_prefix,
                origin,
            )
            .await?;
            source.aggregate_set = None;
            source.synchronous = Some(true);
            let receipt = self
                .write_mutations_batch_inner(vec![source], storage_prefix, origin)
                .await?;
            return Self::require_durable_receipt(receipt, "losing replay");
        }

        let old_target_guard_key = current_guard.as_ref().and_then(|current| {
            let old_key = aggregate_target_guard_key(
                storage_prefix,
                &current.target_schema_name,
                &current.target_key,
            );
            (old_key != target_guard_key).then_some(old_key)
        });
        let mut desired_guard = expected_guard.clone();
        for existing in current_guard.iter().chain(target_owner.iter()) {
            if existing.same_association(&expected_guard)
                && existing.cmp_enrollment(&desired_guard).is_gt()
            {
                desired_guard = existing.clone();
            }
        }
        let publish_forward = current_guard.as_ref().is_none_or(|current| {
            !current.same_association(&desired_guard)
                || desired_guard.cmp_enrollment(current).is_gt()
        });
        let publish_target = target_owner.as_ref().is_none_or(|current| {
            !current.same_association(&desired_guard)
                || desired_guard.cmp_enrollment(current).is_gt()
        });

        // Read source existence before this mutation changes the source row.
        // An absent member is safe to add to a ready summary only for a truly
        // new source. If the source already exists, the missing member is lost
        // derived state and the bounded repair must rebuild the total.
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
        let source_probe = Self::aggregate_probe(source.schema_name.clone(), source_key.clone());
        let (source_row, current_source_winner) = self
            .read_current_row_fields_with_winner(&source_probe, &source_field_names, storage_prefix)
            .await?;
        let source_fields = match source_row {
            CurrentRowFields::Absent => BTreeMap::new(),
            CurrentRowFields::Present(fields) => fields,
            CurrentRowFields::Corrupt { field, atom_uuid } => {
                self.invalidate_guard_targets_durable(
                    &[&expected_guard],
                    &source.uuid,
                    &source.pub_key,
                    storage_prefix,
                    origin,
                )
                .await?;
                return Err(SchemaError::InvalidData(format!(
                    "aggregate source field '{field}' has unresolved or tombstoned atom '{atom_uuid}'"
                )));
            }
        };
        let source_existed = !source_fields.is_empty();

        let member_probe = Self::aggregate_probe(
            aggregate.member_schema_name.clone(),
            aggregate.member_key_value.clone(),
        );
        let member_field_names: Vec<String> = AGGREGATE_MEMBER_RESERVED_FIELDS
            .iter()
            .map(|field| (*field).to_string())
            .collect();
        let member_fields = match self
            .read_current_row_fields(&member_probe, &member_field_names, storage_prefix)
            .await?
        {
            CurrentRowFields::Absent => BTreeMap::new(),
            CurrentRowFields::Present(fields) => fields,
            CurrentRowFields::Corrupt { field, atom_uuid } => {
                self.invalidate_guard_targets_durable(
                    &[&expected_guard],
                    &source.uuid,
                    &source.pub_key,
                    storage_prefix,
                    origin,
                )
                .await?;
                if recovering_first_enrollment {
                    BTreeMap::new()
                } else {
                    return Err(SchemaError::InvalidData(format!(
                        "aggregate member field '{field}' has unresolved atom '{atom_uuid}'"
                    )));
                }
            }
        };
        let old_member = if recovering_first_enrollment {
            // A failed first data generation can persist any prefix of the
            // member schema's fixed fields. No committed guard or reverse owner
            // exists, so the same signed intent can safely overwrite the whole
            // canonical row and recompute from a zero target baseline.
            None
        } else if member_fields.is_empty() {
            None
        } else {
            if member_fields.len() != member_field_names.len() {
                self.invalidate_guard_targets_durable(
                    &[&expected_guard],
                    &source.uuid,
                    &source.pub_key,
                    storage_prefix,
                    origin,
                )
                .await?;
                return Err(SchemaError::InvalidData(
                    "aggregate member row is partial or collides with an existing row".into(),
                ));
            }
            match AggregateMemberRecord::from_fields(&member_fields) {
                Ok(member) => Some(member),
                Err(error) => {
                    self.invalidate_guard_targets_durable(
                        &[&expected_guard],
                        &source.uuid,
                        &source.pub_key,
                        storage_prefix,
                        origin,
                    )
                    .await?;
                    return Err(SchemaError::InvalidData(error));
                }
            }
        };

        let incoming = AggregateMemberRecord {
            source_schema_name: source.schema_name.clone(),
            source_key: source_key.to_storage_key(),
            target_schema_name: aggregate.target_schema_name.clone(),
            target_key: aggregate.target_key_value.to_storage_key(),
            winner: incoming_winner,
            metric_fingerprint: metric_fingerprint(aggregate.contribution.keys()),
            contribution: aggregate.contribution.clone(),
        };

        // Aggregate values are a materialized function of the persisted source
        // row. Fold's molecule layer intentionally keeps a same-atom write as
        // a no-op, including its old winner. Therefore, a newer member winner
        // is valid only when this mutation changes at least one live source
        // field. Exact signed retries and stale intents remain no-ops.
        let source_field_advances = source.fields_and_values.iter().any(|(field, value)| {
            source_fields
                .get(field)
                .is_none_or(|current| current != value)
        });
        if source_existed && !source_field_advances {
            let current_source_winner = current_source_winner.as_ref().ok_or_else(|| {
                SchemaError::InvalidData(
                    "aggregate source row has no authoritative current winner".into(),
                )
            })?;
            let exact_source_replay = incoming.winner.cmp_source(current_source_winner).is_eq();
            let member_stays_unchanged = old_member
                .as_ref()
                .is_some_and(|old| !incoming.winner.cmp_source(&old.winner).is_gt());
            if !exact_source_replay && !member_stays_unchanged {
                return Err(SchemaError::InvalidData(format!(
                    "aggregate_set cannot advance its member winner or value over byte-identical source fields (incoming={:?}, current={current_source_winner:?})",
                    incoming.winner
                )));
            }
        }

        // Conflict invalidation follows every request-shape and source-value
        // check. A rejected AggregateSet must not stale either target.
        if source_conflict.is_some() || target_conflict.is_some() {
            let mut affected: Vec<&AggregateGuard> = current_guard.iter().collect();
            affected.extend(target_owner.iter());
            affected.push(&expected_guard);
            self.invalidate_guard_targets_durable(
                &affected,
                &source.uuid,
                &source.pub_key,
                storage_prefix,
                origin,
            )
            .await?;
        }

        let mut summary_field_names = expected_guard.metric_fields.clone();
        summary_field_names.push(AGGREGATE_VALID_FIELD.into());
        summary_field_names.push(AGGREGATE_GUARD_TOKEN_FIELD.into());
        let summary_probe = Self::aggregate_probe(
            aggregate.target_schema_name.clone(),
            aggregate.target_key_value.clone(),
        );
        let mut summary_fields = match self
            .read_current_row_fields(&summary_probe, &summary_field_names, storage_prefix)
            .await?
        {
            CurrentRowFields::Absent => BTreeMap::new(),
            CurrentRowFields::Present(fields) => fields,
            CurrentRowFields::Corrupt { field, atom_uuid } => {
                self.invalidate_guard_targets_durable(
                    &[&expected_guard],
                    &source.uuid,
                    &source.pub_key,
                    storage_prefix,
                    origin,
                )
                .await?;
                return Err(SchemaError::InvalidData(format!(
                    "aggregate summary field '{field}' has unresolved atom '{atom_uuid}'"
                )));
            }
        };
        if recovering_first_enrollment {
            summary_fields = expected_guard
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
        let metrics_complete = expected_guard.metric_fields.iter().all(|field| {
            summary_fields
                .get(field)
                .and_then(serde_json::Value::as_i64)
                .is_some()
        });
        let mut current_summary = BTreeMap::new();
        for field in &expected_guard.metric_fields {
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
            self.invalidate_guard_targets_durable(
                &[&expected_guard],
                &source.uuid,
                &source.pub_key,
                storage_prefix,
                origin,
            )
            .await?;
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

        // A deterministic replay winner can move a source partition or take a
        // target from another partition. The target was durably invalidated
        // above. Overwrite this canonical member as a fresh contribution; the
        // explicit bounded repair recomputes the complete new association
        // before any caller can restore valid=1.
        let migration_winner = source_conflict.is_some() || target_conflict.is_some();
        let replacement_base = (!migration_winner).then_some(old_member.as_ref()).flatten();
        let missing_derived_for_existing_source =
            source_existed && old_member.is_none() && !recovering_first_enrollment;
        let apply = if missing_derived_for_existing_source {
            // Persist the replacement member, but do not add it to an unknown
            // summary generation. Repair proves the complete source/member
            // bijection and recomputes the bounded total before finalization.
            AggregateApply::Applied(current_summary.clone())
        } else {
            match apply_member_replacement(replacement_base, &incoming, &current_summary) {
                Ok(apply) => apply,
                Err(error) => {
                    self.invalidate_guard_targets_durable(
                        &[&expected_guard],
                        &source.uuid,
                        &source.pub_key,
                        storage_prefix,
                        origin,
                    )
                    .await?;
                    return Err(SchemaError::InvalidData(error));
                }
            }
        };
        let prior_valid = valid && !missing_derived_for_existing_source;
        let association_stable = current_guard
            .as_ref()
            .is_some_and(|guard| guard.committed && guard.same_association(&expected_guard))
            && target_owner
                .as_ref()
                .is_some_and(|guard| guard.committed && guard.same_association(&expected_guard));
        let changes_member = matches!(apply, AggregateApply::Applied(_));
        let repairs_guard = publish_forward || publish_target;

        // The complete data generation may span three schemas. A separate,
        // durable invalid marker must precede it because those schema writes
        // cannot become durable atomically after a process stop.
        if changes_member || repairs_guard {
            let mut affected: Vec<&AggregateGuard> = current_guard.iter().collect();
            affected.extend(target_owner.iter());
            affected.push(&expected_guard);
            self.invalidate_guard_targets_durable(
                &affected,
                &source.uuid,
                &source.pub_key,
                storage_prefix,
                origin,
            )
            .await?;
            guard_token.clone_from(&source.uuid);
        }

        let provisional = {
            let mut guard = desired_guard.clone();
            guard.committed = false;
            guard
        };
        if repairs_guard {
            // Reservations never replace the last committed guards. Restart
            // discards these keys and leaves every affected target invalid.
            self.db_ops
                .metadata()
                .put_typed_durable(&guard_pending_key, &provisional)
                .await?;
            self.db_ops
                .metadata()
                .put_typed_durable(&target_pending_key, &provisional)
                .await?;
        }

        source.aggregate_set = None;
        source.synchronous = Some(true);
        let mut data_mutations = vec![source];
        let mut derived_clock = None;
        if let AggregateApply::Applied(next_summary) = apply {
            let mut member = Mutation::new(
                aggregate.member_schema_name,
                incoming.to_fields(),
                aggregate.member_key_value,
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
                serde_json::Value::String(guard_token.clone()),
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

        if let Some((reservation, state)) = derived_clock {
            reservation.submit_and_wait(state).await?;
        }

        let data_result = self
            .write_mutations_batch_inner(data_mutations, storage_prefix, origin)
            .await;
        let data_receipt = match data_result {
            Ok(receipt) => Self::require_durable_receipt(receipt, "data generation")?,
            Err(error) => {
                // A flush failure can follow resident publication. Persist the
                // invalid marker again and retain the matching reservations.
                // A retry can then complete the same logical generation; a
                // conflicting request discards the provisional association.
                let _ = self
                    .invalidate_guard_targets_durable(
                        &[&expected_guard],
                        &guard_token,
                        "",
                        storage_prefix,
                        WriteOrigin::Replay,
                    )
                    .await;
                return Err(error);
            }
        };
        if repairs_guard {
            let committed = desired_guard.clone().committed();
            self.db_ops
                .metadata()
                .put_typed_durable(&guard_key, &committed)
                .await?;
            self.db_ops
                .metadata()
                .put_typed_durable(&target_guard_key, &committed)
                .await?;
            self.db_ops
                .metadata()
                .delete_typed_durable(&guard_pending_key)
                .await?;
            self.db_ops
                .metadata()
                .delete_typed_durable(&target_pending_key)
                .await?;
            if let Some(old_target_guard_key) = old_target_guard_key {
                self.db_ops
                    .metadata()
                    .delete_typed_durable(&old_target_guard_key)
                    .await?;
            }
        }

        // A stable association can retain its prior ready state. The marker is
        // a separate, single-schema durable phase and therefore can never race
        // ahead of a partial source/member/metric generation. `guard_token` has
        // already advanced to this write's identity above (`changes_member`
        // implies the advance at the invalidation step), so the marker must
        // persist that current value, not the token this write started with.
        if changes_member && association_stable && prior_valid {
            let mut marker = Mutation::new(
                aggregate.target_schema_name,
                HashMap::from([
                    (AGGREGATE_VALID_FIELD.into(), serde_json::Value::from(1)),
                    (
                        AGGREGATE_GUARD_TOKEN_FIELD.into(),
                        serde_json::Value::String(guard_token.clone()),
                    ),
                ]),
                aggregate.target_key_value,
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
                .write_mutations_batch_inner(vec![marker], storage_prefix, WriteOrigin::Replay)
                .await;
            let marker_receipt = Self::require_durable_receipt(marker_result?, "valid marker")?;
            let _ = marker_receipt;
        }
        Ok(data_receipt)
    }
}
