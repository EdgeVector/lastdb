//! One validated batch owns its receipt identities across every write verb.

use super::*;

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) async fn write_mutations_batch_inner_with_clock(
        &self,
        mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        if mutations.is_empty() {
            return Ok(ResidentCommitReceipt::empty());
        }
        self.validate_mutation_batch(&mutations)?;
        let mutation_ids = mutations
            .iter()
            .map(|mutation| mutation.uuid.clone())
            .collect();
        let mut receipt = self
            .write_validated_batch(mutations, storage_prefix, author_clock_barrier)
            .await?;
        // Identities describe the accepted caller inputs, including idempotent
        // repeats. Internal grouping, filtering, and erasure order cannot change
        // the positional association between an input and its receipt identity.
        receipt.mutation_ids = mutation_ids;
        Ok(receipt)
    }

    async fn write_validated_batch(
        &self,
        mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        // Delete peers are resolved by the durable worker under its complete
        // native write boundary, after any updates in this mixed request.
        self.prepare_batch_molecule_keys(&mutations).await?;
        self.reject_blocked_mutation_targets(&mutations)?;
        self.reject_internal_only_mutations(&mutations)?;

        // A format 7 home deletes inside the native request: a delete merges
        // into its record as the delete floor, in caller order with the other
        // writes (write/records.rs). There is no tip, barrier or erase lane to
        // peel to. Such a home refuses aggregate writes, and a peer import
        // merges records in its own request (peer_import/records).
        if self.db_ops.atoms().record_home() {
            return self
                .write_create_update_batch_async(mutations, storage_prefix, author_clock_barrier)
                .await;
        }

        // Separate tip removal and physical Purge from Create/Update atom
        // creation. Both user Delete forms converge tips without reclaiming
        // shared bodies; must_exist adds only a caller precondition. Purge
        // retains its physical atom erasure.
        let (purges, deletes, mutations) =
            super::super::super::purge::split_off_hard_erasures(mutations);
        let (must_exist_deletes, deletes): (Vec<_>, Vec<_>) = deletes
            .into_iter()
            .partition(|mutation| mutation.must_exist == Some(true));
        // Mixed batch: any Purge/Delete riding with create/update becomes a
        // later envelope on the same schema lane. All erasures wait for disk
        // completion before success. Purge and must-exist Delete preserve
        // loud missing-target errors.
        let has_erasures =
            !purges.is_empty() || !deletes.is_empty() || !must_exist_deletes.is_empty();
        let peels = HardErasurePeels {
            purges,
            must_exist_deletes,
            deletes,
        };
        if mutations.is_empty() {
            let erased = self
                .enqueue_peeled_erasures(peels, storage_prefix, author_clock_barrier)
                .await?;
            // Pure erasure never reaches the create/update pipeline, so this
            // is its only resident-commit and operation accounting site.
            crate::request_phases::add_counter(
                crate::request_phases::RequestCounter::ResidentCommits,
                u64::from(erased > 0),
            );
            crate::request_phases::add_counter(
                crate::request_phases::RequestCounter::ResidentOperations,
                erased,
            );
            let mut receipt = ResidentCommitReceipt::empty();
            receipt.operations.deleted = erased;
            receipt.durability = ResidentDurability::Durable;
            return Ok(receipt);
        }
        if has_erasures {
            return self
                .write_mixed_batch_with_erasures(
                    mutations,
                    peels,
                    storage_prefix,
                    author_clock_barrier,
                )
                .await;
        }
        self.write_create_update_batch_async(mutations, storage_prefix, author_clock_barrier)
            .await
    }

    /// Reject mutations that target aggregate-owned schemas or ask for
    /// semantics the ordinary batch path cannot honor.
    fn reject_internal_only_mutations(&self, mutations: &[Mutation]) -> Result<(), SchemaError> {
        for mutation in mutations {
            mutation.reject_illegal_must_exist()?;
            let schema = self
                .schema_manager
                .require_schema_metadata(&mutation.schema_name)?;
            let aggregate_owned_schema = schema
                .runtime_fields
                .contains_key(crate::schema::types::aggregate::AGGREGATE_VALID_FIELD)
                || schema
                    .runtime_fields
                    .contains_key(crate::schema::types::aggregate::AGGREGATE_GUARD_TOKEN_FIELD)
                || crate::schema::types::aggregate::AGGREGATE_MEMBER_RESERVED_FIELDS
                    .iter()
                    .any(|field| schema.runtime_fields.contains_key(*field));
            let writes_reserved_field = mutation.fields_and_values.keys().any(|field| {
                crate::schema::types::aggregate::AGGREGATE_MEMBER_RESERVED_FIELDS
                    .contains(&field.as_str())
                    || matches!(
                        field.as_str(),
                        crate::schema::types::aggregate::AGGREGATE_VALID_FIELD
                            | crate::schema::types::aggregate::AGGREGATE_GUARD_TOKEN_FIELD
                    )
            });
            if !mutation.aggregate_derived_internal
                && (aggregate_owned_schema || writes_reserved_field)
            {
                return Err(SchemaError::InvalidData(
                    "aggregate member and summary schemas are internal-only mutation targets"
                        .into(),
                ));
            }
            if mutation.aggregate_set.is_some() {
                return Err(SchemaError::InvalidData(
                    "aggregate_set requires the aggregate commit path; ordinary mutation batches must not acknowledge a source value without its derived member and summary".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Mixed batch: any Purge/Delete riding with create/update becomes a
    /// later envelope on the same schema lane.
    async fn write_mixed_batch_with_erasures(
        &self,
        mutations: Vec<Mutation>,
        peels: HardErasurePeels,
        storage_prefix: Option<&str>,
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        tracing::warn!(
            writes = mutations.len(),
            purges = peels.purges.len(),
            deletes = peels.deletes.len() + peels.must_exist_deletes.len(),
            "mixed create/update+Purge batch: placing erasures after writes on the persist lane"
        );
        let mut receipt = self
            .write_create_update_batch_async(
                mutations,
                storage_prefix,
                author_clock_barrier.clone(),
            )
            .await?;
        let erased = self
            .enqueue_peeled_erasures(peels, storage_prefix, author_clock_barrier)
            .await?;
        receipt.operations.merge(ResidentCommitOperations {
            deleted: erased,
            ..Default::default()
        });
        // Every erase can be spared. Cover the earlier writes explicitly
        // before upgrading the whole mixed request's durability receipt.
        let runtime = self
            .db_ops
            .namespaced_store()
            .disk_pack_runtime()
            .ok_or_else(|| {
                SchemaError::InvalidData("logical writes require native disk storage".into())
            })?;
        runtime.flush_native().await?;
        receipt.durability = ResidentDurability::Durable;
        crate::request_phases::add_counter(
            crate::request_phases::RequestCounter::ResidentOperations,
            erased,
        );
        Ok(receipt)
    }

    /// Enqueue every erasure in Purge, DeleteMustExist, Delete order. Return the
    /// accepted input count, including absent or spared keys, after completion.
    async fn enqueue_peeled_erasures(
        &self,
        peels: HardErasurePeels,
        storage_prefix: Option<&str>,
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) -> Result<u64, SchemaError> {
        let HardErasurePeels {
            purges,
            must_exist_deletes,
            deletes,
        } = peels;
        let mut accepted = 0u64;
        for (peel, verb) in [
            (purges, super::super::super::purge::HardEraseVerb::Purge),
            (
                must_exist_deletes,
                super::super::super::purge::HardEraseVerb::DeleteMustExist,
            ),
            (deletes, super::super::super::purge::HardEraseVerb::Delete),
        ] {
            if peel.is_empty() {
                continue;
            }
            let missing = match verb {
                super::super::super::purge::HardEraseVerb::Delete => {
                    super::super::super::purge::PurgeMissingPolicy::Skip
                }
                super::super::super::purge::HardEraseVerb::Purge
                | super::super::super::purge::HardEraseVerb::DeleteMustExist => {
                    super::super::super::purge::PurgeMissingPolicy::Refuse
                }
            };
            let ids = self
                .enqueue_hard_erasures_on_lanes(
                    peel,
                    storage_prefix,
                    missing,
                    verb,
                    author_clock_barrier.clone(),
                )
                .await?;
            accepted = accepted.saturating_add(ids.len() as u64);
        }
        Ok(accepted)
    }
}

/// Hard-erasure mutations peeled off a batch, by verb.
struct HardErasurePeels {
    purges: Vec<Mutation>,
    must_exist_deletes: Vec<Mutation>,
    deletes: Vec<Mutation>,
}
