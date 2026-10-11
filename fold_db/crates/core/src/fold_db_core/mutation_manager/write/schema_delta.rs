//! Per-schema delta preparation, publish, and post-publish persist enqueue.

use super::*;

impl MutationManager {
    /// Build protein sibling tips from the current entry-schema base.
    ///
    /// Imported mutations first apply to a prospective entry molecule. This
    /// keeps a losing imported entry from changing a sibling tip.
    pub(super) async fn build_protein_sibling_updates(
        &self,
        schema: &crate::schema::types::Schema,
        schema_mutations: &[Mutation],
        mutation_key_values: &[crate::schema::types::KeyValue],
        atom_results: &[(usize, String, crate::atom::Atom)],
    ) -> Result<
        Vec<(
            String,
            crate::db_operations::MoleculeData,
            HashSet<crate::db_operations::ChangedKey>,
        )>,
        SchemaError,
    > {
        let sibling_schema = if schema_mutations
            .iter()
            .any(|mutation| mutation.imported_written_at.is_some())
        {
            let mut prospective = schema.clone();
            self.apply_mutations_to_molecules(
                &mut prospective,
                schema_mutations,
                mutation_key_values,
                atom_results.to_vec(),
            );
            prospective
        } else {
            schema.clone()
        };
        self.fold_protein_siblings_after_write(
            &sibling_schema,
            schema_mutations,
            mutation_key_values,
            atom_results,
        )
        .await
    }

    /// Prepare one schema group's atoms, restored tips, and persist capacity.
    /// Publishes no resident tip change.
    pub(super) async fn prepare_schema_delta(
        &self,
        schema_name: String,
        mut schema_mutations: Vec<Mutation>,
        idempotency_entries: Vec<(String, String)>,
        storage_prefix: Option<&str>,
        force_durable: bool,
        timing_breakdown: &mut HashMap<&str, std::time::Duration>,
    ) -> Result<super::super::molecules::PreparedSchemaDelta, SchemaError> {
        // Register before atom creation. An atom reclaim cut samples this
        // tracker before it scans durable references, so every atom older than
        // that cut belongs to a task the cut waits for.
        let pending_persist_task = Some(self.pending_tasks.begin());
        let load_start = std::time::Instant::now();
        let mut schema = self
            .load_schema_for_delta(&schema_name, &mut schema_mutations, storage_prefix)
            .await?;
        let share_prefixes = self
            .active_share_prefixes(&schema_name, storage_prefix)
            .await?;
        let tracks_retention_age = storage_prefix.is_none()
            && matches!(
                &schema.schema_type,
                crate::schema::types::schema::DeclarativeSchemaType::Hash
                    | crate::schema::types::schema::DeclarativeSchemaType::Single
            );
        let tracks_retention_hash_partitions = storage_prefix.is_none()
            && matches!(
                &schema.schema_type,
                crate::schema::types::schema::DeclarativeSchemaType::HashRange
            );
        Self::add_timing(timing_breakdown, "schema_load", load_start.elapsed());

        let phase1_start = std::time::Instant::now();
        let (mutation_key_values, atom_results, deferred_atoms) = self
            .prepare_atoms_and_key_values(
                &schema_name,
                &schema,
                &mut schema_mutations,
                storage_prefix,
            )?;
        Self::add_timing(
            timing_breakdown,
            "  - create_atoms_batch",
            phase1_start.elapsed(),
        );

        let changed_keys =
            Self::changed_keys_by_field(&schema, &schema_mutations, &mutation_key_values);
        let restore_start = std::time::Instant::now();
        self.restore_missing_molecules(&mut schema, &changed_keys)
            .await?;
        Self::add_timing(
            timing_breakdown,
            "  - restore_molecules",
            restore_start.elapsed(),
        );

        let atom_results = self.dedupe_atom_results(
            &schema_name,
            &schema,
            &schema_mutations,
            &mutation_key_values,
            atom_results,
            timing_breakdown,
        );

        // Prepare sibling tip data before resident apply. There must be no
        // await between apply and the infallible lane fill, or cancellation
        // can drop the only durable owner after the value becomes visible.
        let fold_start = std::time::Instant::now();
        let sibling_updates = self
            .build_protein_sibling_updates(
                &schema,
                &schema_mutations,
                &mutation_key_values,
                &atom_results,
            )
            .await?;
        // Capture before the resident rebase. The gate check rejects a
        // sibling change that overlaps either preparation step.
        let sibling_prepared = self.prepared_sibling_slot_revisions(&sibling_updates);
        let sibling_updates = self.rebase_sibling_updates_on_resident(&schema, sibling_updates);
        Self::add_timing(
            timing_breakdown,
            "  - protein_sibling_fold",
            fold_start.elapsed(),
        );

        let search_batch = Self::build_index_change_batch(
            &schema_name,
            &schema,
            &schema_mutations,
            &mutation_key_values,
        );
        let (defer_reservation, lane_reservation, wait_for_persist_lane) = self
            .reserve_persist_capacity(
                &schema_name,
                storage_prefix,
                force_durable,
                deferred_atoms.as_deref(),
                &idempotency_entries,
                search_batch.as_ref(),
                &share_prefixes,
            )?;

        let prepared = self.prepared_slot_revisions(&schema, &changed_keys);
        Ok(super::super::molecules::PreparedSchemaDelta {
            pending_persist_task,
            schema_name,
            schema,
            schema_mutations,
            mutation_key_values,
            atom_results,
            deferred_atoms,
            search_batch,
            share_prefixes,
            sibling_updates,
            sibling_prepared,
            idempotency_entries,
            changed_keys,
            prepared,
            tracks_retention_age,
            tracks_retention_hash_partitions,
            defer_reservation,
            lane_reservation,
            wait_for_persist_lane,
        })
    }


    /// Fill one schema envelope while the caller owns every affected gate.
    ///
    /// Resident tips are already published when this runs. The function must
    /// not fail the request: an Err here skips MutationIntent capture and
    /// author-clock submit while RAM already shows the new tips.
    pub(super) fn finalize_prepared_schema_delta(
        &self,
        delta: &mut super::super::molecules::PreparedSchemaDelta,
        gate_outcome: &super::super::molecules::ApplyGateOutcome,
        timing_breakdown: &mut HashMap<&str, std::time::Duration>,
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) -> Option<tokio::sync::oneshot::Receiver<crate::request_phases::RequestCounts>> {
        let sibling_updates = std::mem::take(&mut delta.sibling_updates);
        let resident_applied = delta
            .schema
            .runtime_fields
            .values()
            .all(|field| field.common().storage_prefix().is_none());
        let mut batch_dirty_keys = if resident_applied {
            Self::resident_batch_dirty_keys(
                &delta.schema,
                &gate_outcome.modified_fields,
                delta.deferred_atoms.as_deref(),
            )
        } else {
            Vec::new()
        };
        if resident_applied {
            batch_dirty_keys.extend(Self::sibling_tip_dirty_keys(&sibling_updates));
        }

        let inject_persist = {
            {
                false
            }
        };

        if inject_persist {
            tracing::warn!(
                schema = %delta.schema_name,
                "injected post-publish path; persistence remains on the schema lane"
            );
        }
        let (completion_tx, completion_rx) = if delta.wait_for_persist_lane {
            let (tx, rx) = tokio::sync::oneshot::channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let fill_start = std::time::Instant::now();
        self.enqueue_post_publish_persist(
            delta,
            sibling_updates,
            gate_outcome,
            batch_dirty_keys,
            completion_tx,
            author_clock_barrier,
        );
        Self::add_timing(
            timing_breakdown,
            "  - write_molecules_batch_deferred",
            fill_start.elapsed(),
        );

        completion_rx
    }
}
