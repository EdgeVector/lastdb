//! Moved verbatim out of the parent module; see the parent for context.

use super::*;

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) async fn write_mutations_batch_inner_with_clock(
        &self,
        mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
        origin: WriteOrigin,
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        if mutations.is_empty() {
            return Ok(ResidentCommitReceipt::empty());
        }
        self.validate_mutation_layouts(&mutations)?;
        // Delete peers are resolved by the durable worker under its complete
        // native write boundary, after any updates in this mixed request.
        self.prepare_batch_molecule_keys(&mutations, storage_prefix)
            .await?;
        self.reject_blocked_mutation_targets(&mutations)?;
        self.validate_key_field_payloads(&mutations)?;
        self.reject_internal_only_mutations(&mutations)?;

        // Separate tip removal and physical Purge from Create/Update atom
        // creation. Both user Delete forms converge tips without reclaiming
        // shared bodies; must_exist adds only a caller precondition. Purge
        // retains its physical atom erasure. Replay uses Skip for every peel
        // because it repeats an earlier accepted request (see WriteOrigin).
        let (purges, deletes, mutations) =
            super::super::super::purge::split_off_hard_erasures(mutations);
        let (must_exist_deletes, deletes): (Vec<_>, Vec<_>) = deletes
            .into_iter()
            .partition(|mutation| mutation.must_exist == Some(true));
        // Mixed batch: any Purge/Delete riding with create/update becomes a
        // later envelope on the same schema lane. All erasures wait for disk
        // completion before success. Purge and must-exist Delete preserve
        // loud missing-target errors for requests; replay uses Skip.
        let mixed = !mutations.is_empty()
            && (!purges.is_empty() || !deletes.is_empty() || !must_exist_deletes.is_empty());
        if mixed {
            return self
                .write_mixed_batch_with_erasures(
                    mutations,
                    HardErasurePeels {
                        purges,
                        must_exist_deletes,
                        deletes,
                    },
                    storage_prefix,
                    origin,
                    author_clock_barrier,
                )
                .await;
        }
        let hard_ids = self
            .enqueue_peeled_erasures(
                HardErasurePeels {
                    purges,
                    must_exist_deletes,
                    deletes,
                },
                storage_prefix,
                origin,
                author_clock_barrier.clone(),
            )
            .await?;

        self.commit_remaining_after_erasures(
            mutations,
            hard_ids,
            storage_prefix,
            origin,
            author_clock_barrier,
        )
        .await
    }

    /// Account the erased ids, then run any remaining create/update mutations
    /// through the standard pipeline with the hard-erasure ids first.
    async fn commit_remaining_after_erasures(
        &self,
        mutations: Vec<Mutation>,
        mut hard_ids: Vec<String>,
        storage_prefix: Option<&str>,
        origin: WriteOrigin,
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        let erased = hard_ids.len() as u64;
        let hard_operations = ResidentCommitOperations {
            deleted: erased,
            ..Default::default()
        };
        // A pure-erasure batch never reaches the create/update pipeline, so
        // this is the only site that can count it as resident work.
        if mutations.is_empty() {
            crate::request_phases::add_counter(
                crate::request_phases::RequestCounter::ResidentCommits,
                u64::from(erased > 0),
            );
            crate::request_phases::add_counter(
                crate::request_phases::RequestCounter::ResidentOperations,
                erased,
            );
            let mut receipt = ResidentCommitReceipt::from_ids(hard_ids, hard_operations);
            receipt.durability = ResidentDurability::Durable;
            return Ok(receipt);
        }
        // Process the remaining mutations through the standard pipeline,
        // then append so hard-erasure IDs come first (the order we peeled
        // them in).
        let mut receipt = self
            .write_create_update_batch_async(
                mutations,
                storage_prefix,
                matches!(origin, WriteOrigin::Replay),
                author_clock_barrier,
            )
            .await?;
        hard_ids.extend(receipt.mutation_ids);
        receipt.mutation_ids = hard_ids;
        receipt.operations.merge(hard_operations);
        crate::request_phases::add_counter(
            crate::request_phases::RequestCounter::ResidentOperations,
            erased,
        );
        Ok(receipt)
    }

    /// Reject mutations that target aggregate-owned schemas or ask for
    /// semantics the ordinary batch path cannot honor.
    fn reject_internal_only_mutations(&self, mutations: &[Mutation]) -> Result<(), SchemaError> {
        for mutation in mutations {
            mutation.reject_illegal_must_exist()?;
            let schema = self
                .schema_manager
                .get_schema_metadata(&mutation.schema_name)?
                .ok_or_else(|| {
                    SchemaError::InvalidData(format!("Schema '{}' not found", mutation.schema_name))
                })?;
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
        origin: WriteOrigin,
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        let HardErasurePeels {
            purges,
            must_exist_deletes,
            deletes,
        } = peels;
        tracing::warn!(
            writes = mutations.len(),
            purges = purges.len(),
            deletes = deletes.len() + must_exist_deletes.len(),
            "mixed create/update+Purge batch: placing erasures after writes on the persist lane"
        );
        let mut receipt = self
            .write_create_update_batch_async(
                mutations,
                storage_prefix,
                matches!(origin, WriteOrigin::Replay),
                author_clock_barrier.clone(),
            )
            .await?;
        // The erasure ids are deliberately NOT appended to the mixed
        // batch's id list — that was already true before the receipt
        // existed and callers depend on it. Their COUNTS still belong to
        // this logical commit, or a mixed batch would report deleting
        // nothing.
        let mut erased = 0u64;
        for (peel, verb) in [
            (purges, super::super::super::purge::HardEraseVerb::Purge),
            (
                must_exist_deletes,
                super::super::super::purge::HardEraseVerb::DeleteMustExist,
            ),
            (deletes, super::super::super::purge::HardEraseVerb::Delete),
        ] {
            let missing = match verb {
                super::super::super::purge::HardEraseVerb::Delete => {
                    super::super::super::purge::PurgeMissingPolicy::Skip
                }
                super::super::super::purge::HardEraseVerb::Purge
                | super::super::super::purge::HardEraseVerb::DeleteMustExist => {
                    origin.missing_policy_for_loud_erasure()
                }
            };
            let ids = self
                .enqueue_hard_erasures_on_lanes(
                    peel,
                    storage_prefix,
                    missing,
                    verb,
                    true,
                    author_clock_barrier.clone(),
                )
                .await?;
            erased = erased.saturating_add(ids.len() as u64);
        }
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

    /// Enqueue each peeled erasure group on its persist lanes and return the
    /// erased ids in Purge, DeleteMustExist, Delete order.
    async fn enqueue_peeled_erasures(
        &self,
        peels: HardErasurePeels,
        storage_prefix: Option<&str>,
        origin: WriteOrigin,
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) -> Result<Vec<String>, SchemaError> {
        let HardErasurePeels {
            purges,
            must_exist_deletes,
            deletes,
        } = peels;
        let mut hard_ids = Vec::new();
        if !purges.is_empty() {
            hard_ids.extend(
                self.enqueue_hard_erasures_on_lanes(
                    purges,
                    storage_prefix,
                    origin.missing_policy_for_loud_erasure(),
                    super::super::super::purge::HardEraseVerb::Purge,
                    true,
                    author_clock_barrier.clone(),
                )
                .await?,
            );
        }
        if !must_exist_deletes.is_empty() {
            hard_ids.extend(
                self.enqueue_hard_erasures_on_lanes(
                    must_exist_deletes,
                    storage_prefix,
                    origin.missing_policy_for_loud_erasure(),
                    super::super::super::purge::HardEraseVerb::DeleteMustExist,
                    true,
                    author_clock_barrier.clone(),
                )
                .await?,
            );
        }
        if !deletes.is_empty() {
            hard_ids.extend(
                self.enqueue_hard_erasures_on_lanes(
                    deletes,
                    storage_prefix,
                    super::super::super::purge::PurgeMissingPolicy::Skip,
                    super::super::super::purge::HardEraseVerb::Delete,
                    true,
                    author_clock_barrier,
                )
                .await?,
            );
        }
        Ok(hard_ids)
    }
}

/// Hard-erasure mutations peeled off a batch, by verb.
struct HardErasurePeels {
    purges: Vec<Mutation>,
    must_exist_deletes: Vec<Mutation>,
    deletes: Vec<Mutation>,
}
