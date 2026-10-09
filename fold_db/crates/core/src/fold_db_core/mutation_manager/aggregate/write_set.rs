//! Public aggregate set write entry points and invalidation writes.

use super::*;

impl MutationManager {
    /// Commit one source mutation and its bounded aggregate member replacement.
    pub async fn write_aggregate_set_with_access_receipt(
        &self,
        source: Mutation,
        access_context: &crate::access::AccessContext,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        self.write_aggregate_set_with_access_receipt_cloud(
            source,
            access_context,
            CloudCapturePolicy::Async,
        )
        .await
    }

    /// Single-route sibling that can secure a durable aggregate delete intent.
    pub async fn write_aggregate_set_with_access_receipt_cloud(
        &self,
        mut source: Mutation,
        access_context: &crate::access::AccessContext,
        cloud_policy: CloudCapturePolicy,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        #[cfg(not(feature = "cloud-sync"))]
        let cloud_mutation_uuid = source.uuid.clone();
        let aggregate = source.aggregate_set.clone().ok_or_else(|| {
            SchemaError::InvalidData("aggregate commit requires aggregate_set".into())
        })?;
        // Validate before clock allocation and before guard enrollment.
        self.validate_aggregate_contract(&source, &aggregate)?;
        let storage_prefix = self
            .resolve_aggregate_storage_prefix(&source.schema_name, &aggregate, access_context)
            .await?;
        let source_clock =
            self.prepare_mutation_author_clocks(std::slice::from_mut(&mut source))?;
        let source_revision = source.logical_counter;
        if let Some((reservation, state)) = source_clock {
            reservation.submit_and_wait(state).await?;
        }

        #[cfg(feature = "cloud-sync")]
        let mut envelopes = crate::sync::mutation_intent::encode_mutations(
            std::slice::from_ref(&source),
            storage_prefix.as_deref(),
        );
        #[cfg(feature = "cloud-sync")]
        crate::sync::mutation_intent::retain_persisted_field_atom_uuids(
            &mut envelopes,
            std::slice::from_ref(&source),
            |schema_name| {
                self.schema_manager
                    .get_schema_metadata(schema_name)
                    .ok()
                    .flatten()
                    .map(|schema| schema.runtime_fields.keys().cloned().collect())
            },
        );

        let result = {
            #[cfg(feature = "cloud-sync")]
            {
                crate::sync::capture::capture_logical_commit_with_policy(
                    self.capture_router(),
                    envelopes,
                    cloud_policy,
                    self.apply_aggregate_source(
                        source,
                        storage_prefix.as_deref(),
                        WriteOrigin::Request,
                    ),
                )
                .await
            }
            #[cfg(not(feature = "cloud-sync"))]
            {
                let cloud = (!matches!(cloud_policy, CloudCapturePolicy::Async)).then(|| {
                    CloudMutationReceipt::unavailable(
                        cloud_mutation_uuid,
                        "cloud sync capture is unavailable in this build",
                    )
                });
                self.apply_aggregate_source(source, storage_prefix.as_deref(), WriteOrigin::Request)
                    .await
                    .map(|receipt| (receipt, cloud))
            }
        };
        let (mut receipt, cloud) = result?;
        receipt.revision = Some(source_revision);
        receipt.cloud = cloud;
        Ok(receipt)
    }

    #[cfg(feature = "cloud-sync")]
    pub(in crate::fold_db_core::mutation_manager) async fn apply_replayed_aggregate_set(
        &self,
        mut mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        if mutations.len() != 1 {
            return Err(SchemaError::InvalidData(
                "aggregate replay must contain exactly one source mutation".into(),
            ));
        }
        self.apply_aggregate_source(
            mutations.pop().expect("one mutation checked"),
            storage_prefix,
            WriteOrigin::Replay,
        )
        .await
    }

    /// Add an atomic invalidation for every enrolled ordinary source write.
    // lint:fn-size-ok verbatim move from aggregate.rs; splitting this function is separate work
    pub(in crate::fold_db_core::mutation_manager) async fn write_with_aggregate_invalidations(
        &self,
        mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
        origin: WriteOrigin,
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        struct Candidate {
            mutation_index: usize,
            guard_key: String,
            source_hash: String,
        }

        let mut candidates = Vec::new();
        for (mutation_index, mutation) in mutations.iter().enumerate() {
            if mutation.aggregate_set.is_some() {
                continue;
            }
            let Some(schema) = self
                .schema_manager
                .get_schema_metadata(&mutation.schema_name)?
            else {
                continue;
            };
            if !matches!(
                schema.schema_type,
                DeclarativeSchemaType::Hash | DeclarativeSchemaType::HashRange
            ) {
                continue;
            }
            let key = Self::resolve_mutation_key_value(&mutation.schema_name, &schema, mutation)?;
            let Some(source_hash) = key.hash.clone() else {
                continue;
            };
            candidates.push(Candidate {
                mutation_index,
                guard_key: aggregate_guard_key(storage_prefix, &mutation.schema_name, &source_hash),
                source_hash,
            });
        }
        let _guard_locks = self
            .acquire_aggregate_named_locks(
                candidates
                    .iter()
                    .map(|candidate| format!("aggregate_guard_lock\u{1f}{}", candidate.guard_key))
                    .collect(),
            )
            .await;

        let mut by_target: BTreeMap<String, (AggregateGuard, AggregateWinner, String, String)> =
            BTreeMap::new();
        for candidate in candidates {
            let Some(guard) = self
                .read_reconciled_aggregate_guard(&candidate.guard_key, storage_prefix)
                .await?
            else {
                continue;
            };
            guard.validate().map_err(SchemaError::InvalidData)?;
            let source = &mutations[candidate.mutation_index];
            if guard.source_schema_name != source.schema_name
                || guard.source_partition_hash != candidate.source_hash
            {
                return Err(SchemaError::InvalidData(
                    "aggregate guard identity does not match its storage key".into(),
                ));
            }
            if matches!(
                source.mutation_type,
                MutationType::Delete | MutationType::Purge
            ) {
                return Err(SchemaError::InvalidData(
                    "enrolled aggregate sources reject Delete and Purge; write a live logical removal value with an explicit zero AggregateSet"
                        .into(),
                ));
            }
            if source
                .fields_and_values
                .values()
                .any(crate::atom::is_tombstone_value)
            {
                return Err(SchemaError::InvalidData(
                    "enrolled aggregate sources reject field tombstones; write a live logical removal value with an explicit zero AggregateSet"
                        .into(),
                ));
            }
            let winner = AggregateWinner {
                logical_counter: source.logical_counter,
                written_at: source.imported_written_at.unwrap_or(0),
                writer_id: source.author_clock_writer_id.clone(),
                mutation_uuid: source.uuid.clone(),
            };
            let target_key = KeyValue::from_storage_key(&guard.target_key);
            let lock_key = Self::aggregate_target_lock_key(
                storage_prefix,
                &guard.target_schema_name,
                &target_key,
            );
            match by_target.get(&lock_key) {
                Some((_, current, _, _)) if !winner.cmp_source(current).is_gt() => {}
                _ => {
                    by_target.insert(
                        lock_key,
                        (guard, winner, source.uuid.clone(), source.pub_key.clone()),
                    );
                }
            }
        }
        let _target_locks = self
            .acquire_aggregate_named_locks(by_target.keys().cloned().collect())
            .await;

        let mut invalidations = Vec::with_capacity(by_target.len());
        let mut invalidated_guards = Vec::with_capacity(by_target.len());
        for (_, (guard, _, token, pub_key)) in by_target {
            // The reverse owner (if any) is informational only: a missing or
            // association-mismatched reverse row must never be read as "no
            // work". This forward guard is this source partition's reconciled,
            // committed enrollment, so the ordinary write below always changes
            // data that feeds the target; skipping invalidation here would
            // let the source write land while the target keeps a live
            // `valid=1` marker that no longer reflects it.
            let target_schema = self
                .schema_manager
                .get_schema_metadata(&guard.target_schema_name)?
                .ok_or_else(|| {
                    SchemaError::InvalidData(format!(
                        "aggregate target schema '{}' not found",
                        guard.target_schema_name
                    ))
                })?;
            for field in [AGGREGATE_VALID_FIELD, AGGREGATE_GUARD_TOKEN_FIELD] {
                if !target_schema.runtime_fields.contains_key(field) {
                    return Err(SchemaError::InvalidField(format!(
                        "aggregate target schema '{}' must declare field '{field}'",
                        guard.target_schema_name
                    )));
                }
            }
            let mut invalidation = Mutation::new(
                guard.target_schema_name.clone(),
                HashMap::from([
                    (AGGREGATE_VALID_FIELD.into(), serde_json::Value::from(0)),
                    (
                        AGGREGATE_GUARD_TOKEN_FIELD.into(),
                        serde_json::Value::String(token),
                    ),
                ]),
                KeyValue::from_storage_key(&guard.target_key),
                pub_key,
                MutationType::Update,
            );
            invalidation.aggregate_derived_internal = true;
            invalidation.synchronous = Some(true);
            invalidations.push(invalidation);
            invalidated_guards.push(guard);
        }
        if !invalidations.is_empty() {
            let invalidation_clock = self.prepare_mutation_author_clocks(&mut invalidations)?;
            if let Some((reservation, state)) = invalidation_clock {
                reservation.submit_and_wait(state).await?;
            }
            let result = self
                .write_mutations_batch_inner(invalidations, storage_prefix, WriteOrigin::Replay)
                .await;
            Self::require_durable_receipt(result?, "ordinary source invalidation")?;
            for guard in &invalidated_guards {
                let grant_key = Self::aggregate_repair_grant_key(
                    storage_prefix,
                    &guard.target_schema_name,
                    &guard.target_key,
                );
                self.db_ops
                    .metadata()
                    .delete_typed_durable(&grant_key)
                    .await?;
                let finalized_key = Self::aggregate_finalized_grant_key(
                    storage_prefix,
                    &guard.target_schema_name,
                    &guard.target_key,
                );
                self.db_ops
                    .metadata()
                    .delete_typed_durable(&finalized_key)
                    .await?;
            }
        }

        self.write_mutations_batch_inner_with_clock(
            mutations,
            storage_prefix,
            origin,
            author_clock_barrier,
        )
        .await
    }
}
