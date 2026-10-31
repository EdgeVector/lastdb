//! Per-schema delta preparation, publish, and post-publish persist enqueue.

use super::*;

impl MutationManager {
    /// Prepare one schema group's atoms, restored tips, and persist capacity.
    /// Publishes no resident tip change.
    pub(super) async fn prepare_schema_delta(
        &self,
        schema_name: String,
        mut schema_mutations: Vec<Mutation>,
        idempotency_entries: Vec<(String, String)>,
        storage_prefix: Option<&str>,
        timing_breakdown: &mut HashMap<&str, std::time::Duration>,
    ) -> Result<super::super::molecules::PreparedSchemaDelta, SchemaError> {
        // Register before atom creation. An atom reclaim cut samples this
        // tracker before it scans durable references, so every atom older than
        // that cut belongs to a task the cut waits for.
        let pending_persist_task = Some(self.pending_tasks.begin());
        let load_start = std::time::Instant::now();
        let mut schema = self
            .load_schema_for_delta(&schema_name, storage_prefix)
            .await?;
        let share_prefixes = self
            .active_share_prefixes(&schema_name, storage_prefix)
            .await?;
        let (tracks_retention_age, tracks_retention_hash_partitions) =
            super::super::plan::molecules::retention_tracking(&schema, storage_prefix);
        Self::add_timing(timing_breakdown, "schema_load", load_start.elapsed());

        let phase1_start = std::time::Instant::now();
        let super::super::molecules::PreparedAtoms {
            mutation_key_values,
            atom_results,
            deferred_atoms,
        } = self.prepare_atoms_and_key_values(&schema_name, &schema, &mut schema_mutations)?;
        Self::add_timing(
            timing_breakdown,
            "  - create_atoms_batch",
            phase1_start.elapsed(),
        );
        let changed_keys = super::super::plan::molecules::changed_keys_by_field(
            &schema,
            &schema_mutations,
            &mutation_key_values,
        );
        let restore_start = std::time::Instant::now();
        self.reload_changed_for_prepare(&mut schema, &changed_keys)
            .await?;
        Self::add_timing(
            timing_breakdown,
            "  - restore_molecules",
            restore_start.elapsed(),
        );
        let search_batch = Self::build_index_change_batch(
            &schema_name,
            &schema,
            &schema_mutations,
            &mutation_key_values,
        );

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

            idempotency_entries,
            changed_keys,
            tracks_retention_age,
            tracks_retention_hash_partitions,
        })
    }
}
