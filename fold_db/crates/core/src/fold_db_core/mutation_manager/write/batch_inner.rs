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

        // Hard erasure (compliance Purge + user Delete) has destructive
        // semantics that diverge from the rest of the pipeline: it writes
        // nothing into the molecule tip path. Peel both verbs out here so
        // the standard write pipeline only sees Create/Update. Three peels:
        // Purge/Refuse and Delete+must_exist/Refuse still reach
        // `purge_records_bulk` (molecule entries + atom history +
        // embeddings); plain Delete/Skip converges only the tip
        // (`converge_delete_tips` — molecule entries + `tv:` chain, atom
        // reclaim deferred to a later janitor). On replay all three are
        // Skip (see [`WriteOrigin`]).
        let (purges, deletes, mutations) =
            super::super::super::purge::split_off_hard_erasures(mutations);
        let (must_exist_deletes, deletes): (Vec<_>, Vec<_>) = deletes
            .into_iter()
            .partition(|mutation| mutation.must_exist == Some(true));
        let (force_hard_durable, deletes_wait_for_durable) =
            self.erasure_durability(&purges, &must_exist_deletes, &deletes, origin);
        let all_hard_erasures_durable = deletes.is_empty() || deletes_wait_for_durable;
        // Mixed batch: any Purge/Delete riding with create/update becomes a
        // later envelope on the same schema lane. A request-origin Purge or
        // must-exist Delete waits for that envelope so its loud missing-target
        // error reaches the requester. Replay uses Skip.
        // A pure Skip Delete on a live request also peels: stamp the resident
        // tombstone overlay, ack, and let the lane hard-erase. Measured on
        // the primary 2026-08-20: kanban deleteRecord is a pure Delete batch
        // and paid `purge_plan` on the HTTP 200 (110 calls, 0 records purged,
        // avg 1.8s, max 70s). Replay still erases inline so a captured intent
        // is fully applied before the download cursor advances.
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
                    force_hard_durable,
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
                deletes_wait_for_durable,
                author_clock_barrier.clone(),
            )
            .await?;

        self.commit_remaining_after_erasures(
            mutations,
            hard_ids,
            all_hard_erasures_durable,
            storage_prefix,
            origin,
            author_clock_barrier,
        )
        .await
    }

    fn erasure_durability(
        &self,
        purges: &[Mutation],
        must_exist_deletes: &[Mutation],
        deletes: &[Mutation],
        origin: WriteOrigin,
    ) -> (bool, bool) {
        // Native queries read disk and accepted batches, not resident tombstones.
        // Wait for the native lane's complete stored deletion before its reply.
        let native = self.native_erasure_requires_durable();
        let forced = native
            || purges
                .iter()
                .chain(must_exist_deletes)
                .chain(deletes)
                .any(|mutation| mutation.synchronous == Some(true));
        let wait = native
            || !matches!(origin, WriteOrigin::Request)
            || deletes
                .iter()
                .any(|mutation| mutation.synchronous == Some(true));
        (forced, wait)
    }

    fn native_erasure_requires_durable(&self) -> bool {
        self.db_ops.namespaced_store().disk_pack_runtime().is_some()
    }

    /// Account the erased ids, then run any remaining create/update mutations
    /// through the standard pipeline with the hard-erasure ids first.
    async fn commit_remaining_after_erasures(
        &self,
        mutations: Vec<Mutation>,
        mut hard_ids: Vec<String>,
        all_hard_erasures_durable: bool,
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
            if all_hard_erasures_durable {
                receipt.durability = ResidentDurability::Durable;
            }
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
        force_hard_durable: bool,
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
        let wait_for_durable =
            receipt.durability == ResidentDurability::Durable || force_hard_durable;
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
                    wait_for_durable
                        || missing == super::super::super::purge::PurgeMissingPolicy::Refuse,
                    author_clock_barrier.clone(),
                )
                .await?;
            erased = erased.saturating_add(ids.len() as u64);
        }
        receipt.operations.merge(ResidentCommitOperations {
            deleted: erased,
            ..Default::default()
        });
        if let Some(runtime) = self.db_ops.namespaced_store().disk_pack_runtime() {
            // Every erase can be spared. Cover the earlier writes explicitly
            // before upgrading the whole mixed request's durability receipt.
            runtime.flush_native().await?;
            receipt.durability = ResidentDurability::Durable;
        }
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
        deletes_wait_for_durable: bool,
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
            let ids: Vec<String> = deletes.iter().map(|m| m.uuid.clone()).collect();
            let returned = self
                .enqueue_hard_erasures_on_lanes(
                    deletes,
                    storage_prefix,
                    super::super::super::purge::PurgeMissingPolicy::Skip,
                    super::super::super::purge::HardEraseVerb::Delete,
                    deletes_wait_for_durable,
                    author_clock_barrier,
                )
                .await?;
            // A waiting enqueue returns the ids it erased; a fire-and-forget
            // one reports the submitted ids.
            hard_ids.extend(if deletes_wait_for_durable {
                returned
            } else {
                ids
            });
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
