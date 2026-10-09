//! Attribution roots, protein reverse-membership erasure, and guarded storage-slot purge.

use std::collections::{HashMap, HashSet};

use crate::db_operations::AttributionPendingScope;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Mutation, MutationType};
use crate::schema::SchemaError;

use super::attribution::{attribution_events, live_write_attribution_path};
use super::MutationManager;

/// Conservative heap charge for the retained-atom set held by a purge retry.
///
/// `HashSet::capacity` states the element capacity, not the raw bucket count.
/// Charging twice that capacity covers the table load factor and small-table
/// rounding. Each charged bucket includes a `String` header and control data.
/// The string allocation uses its capacity because the retry owns that heap.
pub(super) fn estimate_retained_atom_set_bytes(retained: Option<&HashSet<String>>) -> u64 {
    const RAW_BUCKETS_PER_ELEMENT_CAPACITY: u64 = 2;
    const CONTROL_BYTES_PER_BUCKET: u64 = 8;

    retained.map_or(0, |set| {
        let element_capacity = u64::try_from(set.capacity()).unwrap_or(u64::MAX);
        let raw_buckets = element_capacity.saturating_mul(RAW_BUCKETS_PER_ELEMENT_CAPACITY);
        let string_header_bytes = u64::try_from(std::mem::size_of::<String>()).unwrap_or(u64::MAX);
        let table_bytes = raw_buckets
            .saturating_mul(string_header_bytes.saturating_add(CONTROL_BYTES_PER_BUCKET));

        set.iter().fold(table_bytes, |total, atom| {
            total.saturating_add(u64::try_from(atom.capacity()).unwrap_or(u64::MAX))
        })
    })
}

impl MutationManager {
    /// Persist the durable attribution root for a batch, then append its
    /// source events and clear the pending-scope markers.
    ///
    /// This runs once per mutation batch, from exactly one of two mutually
    /// exclusive call sites: the live request write
    /// (`write_mutations_batch_with_receipt_cloud`) or a captured-mutation
    /// replay (`apply_replayed_mutations`). A given batch takes one path or
    /// the other, never both, so the two call sites are not two attribution
    /// writes for the same operation — they are the shared attribution tail
    /// for two different upstream pipelines. No-op when source events are
    /// disabled or the batch carries no attribution scopes.
    pub(super) async fn record_attribution_scopes(
        &self,
        attribution_scopes: &[AttributionPendingScope],
    ) -> Result<(), SchemaError> {
        if attribution_scopes.is_empty() {
            return Ok(());
        }
        // One durable path row per written object, before the event append's
        // trailing flush — so a caller that observes the response also has a
        // durable attribution root for what it just wrote, not only a source
        // event that a later batch walk has yet to classify.
        for scope in attribution_scopes {
            self.db_ops
                .attribution()
                .put_attribution_path(&live_write_attribution_path(scope))
                .await?;
        }
        let mutation_ids = attribution_scopes
            .iter()
            .map(|scope| scope.mutation_id.clone())
            .collect::<Vec<_>>();
        self.db_ops
            .attribution()
            .append_events_and_clear_pending_scopes(
                attribution_events(attribution_scopes),
                &mutation_ids,
            )
            .await?;
        Ok(())
    }

    /// Add the reverse-key deletes for a protein-backed HashRange membership.
    ///
    /// A HashRange membership has two keyed views of one edge: `holder → item`
    /// and `item → holder`. A protein keeps writes in both views coherent, but
    /// a hard delete has no payload from which the normal fold can derive the
    /// sibling key. Point-read the fixed schema field set before we tombstone
    /// the entry key, then enqueue the sibling key under its own schema. This
    /// reads one fixed-field row and checks the resident schema and protein
    /// metadata. It never traverses a data partition or scans stored records.
    // lint:fn-size-ok verbatim move from write.rs; splitting this function is separate work
    pub(super) async fn expand_protein_hashrange_erasures(
        &self,
        erasures: Vec<Mutation>,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<Mutation>, SchemaError> {
        let schemas = self.schema_manager.get_schemas()?;
        let mut expanded = Vec::with_capacity(erasures.len());
        let mut seen: HashSet<(String, String)> = HashSet::new();

        for erasure in erasures {
            let source_schema = schemas.get(&erasure.schema_name).ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "Schema '{}' not found for reverse membership delete",
                    erasure.schema_name
                ))
            })?;
            let source_key = source_schema.key.as_ref();
            let source_is_hashrange =
                matches!(source_schema.schema_type, DeclarativeSchemaType::HashRange);
            seen.insert((
                erasure.schema_name.clone(),
                erasure.key_value.to_storage_key(),
            ));
            expanded.push(erasure.clone());

            if !source_is_hashrange {
                continue;
            }
            let Some(source_key) = source_key else {
                continue;
            };
            if source_key.hash_field.is_none() {
                continue;
            }

            let source_molecules: HashSet<String> = source_schema
                .runtime_fields
                .values()
                .filter_map(|field| field.common().molecule_uuid().cloned())
                .collect();
            if source_molecules.is_empty() {
                continue;
            }

            let mut proteins = Vec::new();
            for molecule_uuid in source_molecules {
                let Some(protein_uuid) = self
                    .db_ops
                    .atoms()
                    .protein_of_molecule(&molecule_uuid)
                    .await?
                else {
                    continue;
                };
                if let Some(protein) = self.db_ops.atoms().protein_get(&protein_uuid).await? {
                    proteins.push(protein);
                }
            }
            if proteins.is_empty() {
                continue;
            }

            let source_fields: Vec<String> = source_schema.runtime_fields.keys().cloned().collect();
            let row = match self
                .read_current_row_fields(&erasure, &source_fields, storage_prefix)
                .await?
            {
                super::super::cas::CurrentRowFields::Present(row) => row,
                super::super::cas::CurrentRowFields::Absent
                | super::super::cas::CurrentRowFields::Corrupt { .. } => continue,
            };

            for (peer_name, peer_schema) in &schemas {
                if peer_name == &erasure.schema_name
                    || !matches!(peer_schema.schema_type, DeclarativeSchemaType::HashRange)
                {
                    continue;
                }
                let Some(peer_key) = peer_schema.key.as_ref() else {
                    continue;
                };
                let (Some(peer_hash_field), Some(peer_range_field)) = (
                    peer_key.hash_field.as_deref(),
                    peer_key.range_field.as_deref(),
                ) else {
                    continue;
                };
                let peer_molecules: HashSet<&str> = peer_schema
                    .runtime_fields
                    .values()
                    .filter_map(|field| field.common().molecule_uuid().map(String::as_str))
                    .collect();
                let peer_is_member = proteins.iter().any(|protein| {
                    protein.members.iter().any(|member| {
                        peer_molecules.contains(member.molecule_uuid.as_str())
                            && member.hash_field == peer_hash_field
                            && member.range_field.as_deref() == Some(peer_range_field)
                    })
                });
                if !peer_is_member {
                    continue;
                }

                let Some(hash) = row.get(peer_hash_field).and_then(serde_json::Value::as_str)
                else {
                    continue;
                };
                let Some(range) = row
                    .get(peer_range_field)
                    .and_then(serde_json::Value::as_str)
                else {
                    continue;
                };
                let key_value = KeyValue::new(Some(hash.to_string()), Some(range.to_string()));
                if !seen.insert((peer_name.clone(), key_value.to_storage_key())) {
                    continue;
                }

                let mut reverse = Mutation::new(
                    peer_name.clone(),
                    HashMap::new(),
                    key_value,
                    erasure.pub_key.clone(),
                    erasure.mutation_type,
                );
                // The forward row proves the logical edge exists. The sibling
                // can already be absent after an interrupted older writer, so
                // its cleanup stays idempotent even for a loud forward delete.
                reverse.synchronous = erasure.synchronous;
                if erasure.mutation_type == MutationType::Delete {
                    // Both molecule keys describe one signed source Delete.
                    // The derived key has no separate signature, but its
                    // durable winner must retain the source device's order.
                    reverse.uuid = erasure.uuid.clone();
                    reverse.imported_written_at = erasure.imported_written_at;
                    reverse.logical_counter = erasure.logical_counter;
                    reverse.author_clock_writer_id = erasure.author_clock_writer_id.clone();
                }
                expanded.push(reverse);
            }
        }

        Ok(expanded)
    }

    /// Purge exact storage-form slots discovered by an owner scan.
    ///
    /// This is intentionally separate from `MutationType::Purge`: its inputs
    /// are already BlindV1/OpeV1-encoded and must never pass through API-key
    /// validation or encoding. Each bounded chunk holds the same per-schema
    /// exclusive barrier as logical purge across reachability snapshot and
    /// destructive commit.
    ///
    /// # Cost
    ///
    /// The schema-wide `O(F·R)` work — resolving live molecules and collecting
    /// the atoms retained history names — is done **once** here, not once per
    /// chunk. Doing it per chunk made the drain `O(N/CHUNK · F·R)`: measured on
    /// the primary's 943k-tombstone population at 25–35 s per 64-slot chunk,
    /// ~2 records/sec, ~130 hours for the store. `PURGE_BARRIER_CHUNK` bounds
    /// one guarded purge critical section. It was the wrong place to pay the
    /// schema cost. See [`prepare_storage_slot_purge`].
    ///
    /// Missing slots are skipped, not refused: tombstone atoms are
    /// content-addressed, so all fields of one tombstoned record share a single
    /// atom and an earlier chunk legitimately erases what a later chunk still
    /// lists. Refusing there aborted the whole drain with a 500.
    pub async fn purge_storage_slots_guarded(
        &self,
        schema_name: &str,
        targets: &[crate::fold_db_core::purge::StorageSlotPurgeTarget],
    ) -> Result<Vec<crate::fold_db_core::purge::StorageSlotPurgeEvidence>, SchemaError> {
        if targets.is_empty() {
            return Ok(Vec::new());
        }
        let schema = self
            .schema_manager
            .get_schema_metadata(schema_name)?
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "Schema '{schema_name}' not found for storage-slot purge"
                ))
            })?;
        crate::fold_db_core::purge::validate_storage_slot_purge_targets(
            schema_name,
            &schema,
            targets,
        )?;
        let bytes = targets
            .iter()
            .fold(0_u64, |total, target| {
                // The envelope can retain the target, its evidence copy, and
                // one Search change. Include fixed collection overhead too.
                total.saturating_add(target.approx_bytes().saturating_mul(3).saturating_add(256))
            })
            .max(1);
        let lane_key = crate::resident::PersistLaneKey::new("", schema_name);
        let reservation = self
            .persist_lanes
            .reserve(lane_key, bytes)
            .map_err(|kind| SchemaError::PersistQueueFull {
                schema: schema_name.to_string(),
                kind: match kind {
                    crate::resident::PersistLaneFull::Entries => "entries".into(),
                    crate::resident::PersistLaneFull::Bytes => "bytes".into(),
                    crate::resident::PersistLaneFull::Unhealthy => "unhealthy".into(),
                },
            })?;
        let (completion, receiver) = tokio::sync::oneshot::channel();
        let pending_task = self.pending_tasks.begin();
        let job = super::super::molecules::StorageSlotPurgeEnvelope {
            schema_name: schema_name.to_string(),
            targets: targets.to_vec(),
            evidence_checkpoint: Vec::with_capacity(targets.len()),
            durable_complete: false,
            retry_requires_finalize: false,
            search_batch: None,
            completion: Some(completion),
            pending_completion: None,
        };
        let envelope = crate::resident::PersistEnvelope::new(
            super::super::molecules::LanePersistJob::StorageSlotPurge {
                job: Box::new(job),
                _pending_task: pending_task,
            },
            bytes,
        );
        reservation.fill(envelope);
        let completion = receiver.await.map_err(|_| {
            SchemaError::InvalidData(
                "storage-slot purge lane stopped before completion".to_string(),
            )
        })?;
        crate::request_phases::add_purge_totals(&completion.phases);
        Ok(completion.evidence)
    }
}
