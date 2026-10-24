//! Per-schema delta preparation, publish, and post-publish persist enqueue.

use super::*;

struct PreparedAtomPhase {
    mutation_key_values: Vec<crate::schema::types::KeyValue>,
    atom_results: Vec<super::super::molecules::PreparedFieldAtom>,
    changed_keys: HashMap<String, HashSet<crate::db_operations::ChangedKey>>,
}

impl MutationManager {
    /// Whether the schema feeds the retention-age or hash-partition trackers.
    fn retention_tracking(
        schema: &crate::schema::types::Schema,
        storage_prefix: Option<&str>,
    ) -> (bool, bool) {
        use crate::schema::types::schema::DeclarativeSchemaType as Kind;
        let tracks_age =
            storage_prefix.is_none() && matches!(&schema.schema_type, Kind::Hash | Kind::Single);
        let tracks_hash_partitions =
            storage_prefix.is_none() && matches!(&schema.schema_type, Kind::HashRange);
        (tracks_age, tracks_hash_partitions)
    }

    /// Create atoms and key values, then restore the current selected tips.
    async fn prepare_atom_phase(
        &self,
        schema_name: &str,
        schema: &mut crate::schema::types::Schema,
        schema_mutations: &mut [Mutation],
        timing_breakdown: &mut HashMap<&str, std::time::Duration>,
    ) -> Result<PreparedAtomPhase, SchemaError> {
        let phase1_start = std::time::Instant::now();
        let (mutation_key_values, atom_results) =
            self.prepare_atoms_and_key_values(schema_name, schema, schema_mutations)?;
        Self::add_timing(
            timing_breakdown,
            "  - create_atoms_batch",
            phase1_start.elapsed(),
        );

        let changed_keys =
            Self::changed_keys_by_field(schema, schema_mutations, &mutation_key_values);
        let restore_start = std::time::Instant::now();
        self.reload_changed_for_prepare(schema, &changed_keys)
            .await?;
        Self::add_timing(
            timing_breakdown,
            "  - restore_molecules",
            restore_start.elapsed(),
        );

        Ok(PreparedAtomPhase {
            mutation_key_values,
            atom_results,
            changed_keys,
        })
    }

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
            Self::retention_tracking(&schema, storage_prefix);
        Self::add_timing(timing_breakdown, "schema_load", load_start.elapsed());

        let PreparedAtomPhase {
            mutation_key_values,
            atom_results,
            changed_keys,
        } = self
            .prepare_atom_phase(
                &schema_name,
                &mut schema,
                &mut schema_mutations,
                timing_breakdown,
            )
            .await?;
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
            search_batch,
            share_prefixes,
            sibling_updates: Vec::new(),
            sibling_origins: Vec::new(),
            sibling_moves: Vec::new(),
            idempotency_entries,
            changed_keys,
            tracks_retention_age,
            tracks_retention_hash_partitions,
        })
    }
}
