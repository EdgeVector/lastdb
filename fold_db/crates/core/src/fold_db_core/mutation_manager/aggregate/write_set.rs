//! Public aggregate set write entry points and invalidation writes.

use super::invalidation_rules::InvalidationTarget;
use super::rules::summary_mutation;
use super::*;

struct InvalidationCandidate {
    mutation_index: usize,
    guard_key: String,
    source_hash: String,
}

impl MutationManager {
    /// Commit one aggregate source with guarded member and summary writes.
    pub async fn write_aggregate_set_with_access_receipt(
        &self,
        mut source: Mutation,
        access_context: &crate::access::AccessContext,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        self.refuse_logical_capture("aggregate operations")?;
        let aggregate = source.aggregate_set.clone().ok_or_else(|| {
            SchemaError::InvalidData("aggregate commit requires aggregate_set".into())
        })?;
        // Validate before clock allocation and before guard enrollment.
        self.validate_aggregate_contract(&source, &aggregate)?;
        let storage_prefix = self
            .resolve_aggregate_storage_prefix(
                &source.schema_name,
                &aggregate.target_schema_name,
                &aggregate.member_schema_name,
                access_context,
            )
            .await?;
        let source_clock =
            self.prepare_mutation_author_clocks(std::slice::from_mut(&mut source))?;
        let source_revision = source.logical_counter;
        if let Some((reservation, state)) = source_clock {
            reservation.submit_and_wait(state).await?;
        }

        let mut receipt = crate::sync::capture::with_mutation_admission(
            self.capture_router(),
            self.apply_aggregate_source(source, storage_prefix.as_deref(), WriteOrigin::Request),
        )
        .await?;
        receipt.revision = Some(source_revision);
        Ok(receipt)
    }

    /// Invalidate enrolled targets durably before the ordinary source write.
    pub(in crate::fold_db_core::mutation_manager) async fn write_with_aggregate_invalidations(
        &self,
        mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
        origin: WriteOrigin,
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        let candidates = self.collect_invalidation_candidates(&mutations, storage_prefix)?;
        let guard_locks = self
            .acquire_ordinary_source_guards(
                candidates
                    .iter()
                    .map(|candidate| candidate.guard_key.clone())
                    .collect(),
            )
            .await?;
        let unenrolled = guard_locks.unenrolled();
        self.with_aggregate_guards(guard_locks, async {
            if unenrolled {
                return self
                    .write_mutations_batch_inner_with_clock(
                        mutations,
                        storage_prefix,
                        origin,
                        author_clock_barrier,
                    )
                    .await;
            }

            let by_target = self
                .resolve_invalidation_targets(candidates, &mutations, storage_prefix)
                .await?;
            let _target_locks = self
                .acquire_aggregate_named_locks(by_target.keys().cloned().collect())
                .await;
            self.with_aggregate_guards(_target_locks, async {
                let (invalidations, invalidated_guards) = self.build_invalidations(by_target)?;
                self.apply_invalidations(invalidations, invalidated_guards, storage_prefix)
                    .await?;

                self.write_mutations_batch_inner_with_clock(
                    mutations,
                    storage_prefix,
                    origin,
                    author_clock_barrier,
                )
                .await
            })
            .await
        })
        .await
    }

    /// Ordinary hash-keyed source mutations that may be enrolled in an aggregate.
    fn collect_invalidation_candidates(
        &self,
        mutations: &[Mutation],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<InvalidationCandidate>, SchemaError> {
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
            let key = self.resolve_mutation_key_value(&mutation.schema_name, &schema, mutation)?;
            let Some(source_hash) = key.hash.clone() else {
                continue;
            };
            candidates.push(InvalidationCandidate {
                mutation_index,
                guard_key: aggregate_guard_key(storage_prefix, &mutation.schema_name, &source_hash),
                source_hash,
            });
        }
        Ok(candidates)
    }

    /// Read each candidate's guard and keep the newest source per aggregate target.
    async fn resolve_invalidation_targets(
        &self,
        candidates: Vec<InvalidationCandidate>,
        mutations: &[Mutation],
        storage_prefix: Option<&str>,
    ) -> Result<BTreeMap<String, InvalidationTarget>, SchemaError> {
        let mut by_target: BTreeMap<String, InvalidationTarget> = BTreeMap::new();
        for candidate in candidates {
            let Some(guard) = self
                .read_reconciled_aggregate_guard(&candidate.guard_key, storage_prefix)
                .await?
            else {
                continue;
            };
            guard.validate().map_err(SchemaError::InvalidData)?;
            let source = &mutations[candidate.mutation_index];
            invalidation_rules::check_enrolled_source(&guard, source, &candidate.source_hash)?;
            let winner = AggregateWinner {
                logical_counter: source.logical_counter,
                written_at: source.imported_written_at.unwrap_or(0),
                writer_id: source.author_clock_writer_id.clone(),
                mutation_uuid: source.uuid.clone(),
            };
            let target_key = KeyValue::from_storage_key(&guard.target_key)?;
            let lock_key = Self::aggregate_target_lock_key(
                storage_prefix,
                &guard.target_schema_name,
                &target_key,
            );
            match by_target.get(&lock_key) {
                Some(current) if !winner.cmp_source(&current.winner).is_gt() => {}
                _ => {
                    by_target.insert(
                        lock_key,
                        InvalidationTarget {
                            guard,
                            winner,
                            token: source.uuid.clone(),
                            pub_key: source.pub_key.clone(),
                        },
                    );
                }
            }
        }
        Ok(by_target)
    }

    /// Build one `valid=0` invalidation mutation per resolved aggregate target.
    fn build_invalidations(
        &self,
        by_target: BTreeMap<String, InvalidationTarget>,
    ) -> Result<(Vec<Mutation>, Vec<AggregateGuard>), SchemaError> {
        let mut invalidations = Vec::with_capacity(by_target.len());
        let mut invalidated_guards = Vec::with_capacity(by_target.len());
        for (_, target) in by_target {
            let InvalidationTarget {
                guard,
                token,
                pub_key,
                ..
            } = target;
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
            invalidation_rules::check_target_fields(&target_schema, &guard.target_schema_name)?;
            let invalidation = summary_mutation(
                guard.target_schema_name.clone(),
                KeyValue::from_storage_key(&guard.target_key)?,
                HashMap::new(),
                false,
                token,
                pub_key,
            );
            invalidations.push(invalidation);
            invalidated_guards.push(guard);
        }
        Ok((invalidations, invalidated_guards))
    }

    /// Write the invalidations durably, then drop the repair and finalized grants.
    async fn apply_invalidations(
        &self,
        mut invalidations: Vec<Mutation>,
        invalidated_guards: Vec<AggregateGuard>,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        if invalidations.is_empty() {
            return Ok(());
        }
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
        Ok(())
    }
}
