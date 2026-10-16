//! Helpers that prepare a schema delta: schema load, share prefixes, dedupe, persist capacity.

use super::*;

impl MutationManager {
    /// Load the schema with its runtime fields and fold mutations into the record envelope.
    pub(super) async fn load_schema_for_delta(
        &self,
        schema_name: &str,
        schema_mutations: &mut [Mutation],
        storage_prefix: Option<&str>,
    ) -> Result<crate::schema::types::Schema, SchemaError> {
        let mut schema = if self.db_ops.namespaced_store().disk_pack_runtime().is_some() {
            self.db_ops
                .schemas()
                .read_snapshot(schema_name)
                .await?
                .schema
        } else {
            self.schema_manager.get_schema_metadata(schema_name)?
        }
        .ok_or_else(|| SchemaError::InvalidData(format!("Schema '{schema_name}' not found")))?;
        apply_storage_prefix_to_schema(&mut schema, storage_prefix);
        schema.ensure_record_molecule_runtime_field();
        crate::record_molecule::fold_mutations_to_record_envelope(
            &self.db_ops,
            &schema,
            schema_mutations,
        )
        .await?;
        Ok(schema)
    }

    /// Active share-rule prefixes that apply to this schema (personal namespace only).
    pub(super) async fn active_share_prefixes(
        &self,
        schema_name: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<String>, SchemaError> {
        if storage_prefix.is_some() {
            return Ok(Vec::new());
        }
        Ok(crate::sharing::store::list_share_rules_in_ops(&self.db_ops)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "load share rules for schema '{schema_name}': {error}"
                ))
            })?
            .into_iter()
            .filter(|rule| rule.active && rule.scope_matches(schema_name))
            .map(|rule| rule.share_prefix)
            .collect())
    }

    /// Drop unchanged field writes when write dedupe is enabled.
    pub(super) fn dedupe_atom_results(
        &self,
        schema_name: &str,
        schema: &crate::schema::types::Schema,
        schema_mutations: &[Mutation],
        mutation_key_values: &[crate::schema::types::KeyValue],
        atom_results: Vec<(usize, String, crate::atom::Atom)>,
        timing_breakdown: &mut HashMap<&str, std::time::Duration>,
    ) -> Vec<(usize, String, crate::atom::Atom)> {
        if !self.write_dedupe_enabled() {
            return atom_results;
        }
        let dedupe_start = std::time::Instant::now();
        let sent = atom_results.len();
        let kept = Self::drop_unchanged_field_writes(
            schema,
            schema_mutations,
            mutation_key_values,
            atom_results,
        );
        Self::add_timing(timing_breakdown, "  - dedupe_scan", dedupe_start.elapsed());
        if kept.len() != sent {
            tracing::debug!(
                target: "write_dedupe",
                schema = %schema_name,
                sent,
                written = kept.len(),
                skipped = sent - kept.len(),
                "dropped unchanged field writes"
            );
        }
        kept
    }

    /// Reserve defer bytes and the persist lane slot. Returns the defer
    /// reservation, the lane reservation, and whether to wait for the lane.
    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    pub(super) fn reserve_persist_capacity(
        &self,
        schema_name: &str,
        storage_prefix: Option<&str>,
        force_durable: bool,
        deferred_atoms: Option<&[(crate::atom::Atom, Option<crate::atom::AtomPartition>)]>,
        idempotency_entries: &[(String, String)],
        search_batch: Option<&crate::db_operations::search_index::IndexChangeBatch>,
        share_prefixes: &[String],
    ) -> Result<
        (
            Option<crate::memory_budget::DeferReservation>,
            Option<crate::resident::PersistReservation<super::super::molecules::LanePersistJob>>,
            bool,
        ),
        SchemaError,
    > {
        let requires_durable_wait =
            force_durable || crate::fold_db_core::mutation_sync_flush_enabled();
        let acks_resident =
            self.acks_on_resident() && storage_prefix.is_none() && !requires_durable_wait;
        let atom_defer_bytes =
            deferred_atoms.map_or(crate::memory_budget::PER_DEFERRED_TASK_BYTES, |located| {
                crate::memory_budget::estimate_deferred_batch_bytes(
                    located.iter().map(|(atom, _)| atom),
                )
            });
        let idempotency_bytes = idempotency_entries
            .iter()
            .fold(0_u64, |total, (key, value)| {
                total.saturating_add((key.len() + value.len()) as u64)
            });
        let search_bytes = search_batch
            .and_then(|batch| serde_json::to_vec(batch).ok())
            .map_or(0, |body| body.len() as u64);
        let share_prefix_bytes = share_prefixes.iter().fold(0_u64, |total, prefix| {
            total.saturating_add(prefix.len() as u64)
        });
        let defer_bytes = atom_defer_bytes
            .saturating_add(idempotency_bytes)
            .saturating_add(search_bytes)
            .saturating_add(share_prefix_bytes);
        let defer_refusal = self.defer_refusal_kind();
        let write_through = acks_resident
            && (defer_refusal == crate::memory_budget::DeferRefusal::Disabled
                || crate::memory_budget::should_write_through(defer_bytes));
        if write_through {
            self.persist_lanes.metrics().record_write_through();
        }
        let defer_reservation = if acks_resident && !write_through {
            let reservation = self.try_reserve_defer(defer_bytes);
            if reservation.is_none() {
                return Err(SchemaError::PersistQueueFull {
                    schema: schema_name.to_string(),
                    kind: "bytes".into(),
                });
            }
            reservation
        } else {
            None
        };
        // Every schema mutation reserves its FIFO position before resident
        // apply. Fast resident mode returns after enqueue. Other modes wait
        // for this envelope after every schema group enters its lane.
        let wait_for_persist_lane = !acks_resident || defer_reservation.is_none();
        let persist_key = crate::resident::PersistLaneKey::new(
            storage_prefix.unwrap_or("").to_string(),
            schema_name.to_string(),
        );
        let lane_reservation = Some(
            match self.persist_lanes.reserve(persist_key, defer_bytes.max(1)) {
                Ok(reservation) => reservation,
                Err(kind) => {
                    return Err(SchemaError::PersistQueueFull {
                        schema: schema_name.to_string(),
                        kind: match kind {
                            crate::resident::PersistLaneFull::Entries => "entries".into(),
                            crate::resident::PersistLaneFull::Bytes => "bytes".into(),
                            crate::resident::PersistLaneFull::Unhealthy => "unhealthy".into(),
                        },
                    });
                }
            },
        );
        Ok((defer_reservation, lane_reservation, wait_for_persist_lane))
    }
}
