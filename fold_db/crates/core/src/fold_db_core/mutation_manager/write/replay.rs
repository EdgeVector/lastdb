//! Captured-mutation replay: apply a remote intent without logging it back,
//! and preserve Puts that lose to a local Delete.

use sha2::{Digest, Sha256};

use crate::schema::types::MutationType;

use super::*;

impl MutationManager {
    /// Replay a captured mutation intent without writing it back to the log.
    #[cfg(feature = "cloud-sync")]
    pub(crate) async fn apply_replayed_mutations(
        &self,
        mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<String>, SchemaError> {
        let has_aggregate = mutations
            .iter()
            .any(|mutation| mutation.aggregate_set.is_some());
        let mutation_ids: Vec<String> = mutations
            .iter()
            .map(|mutation| mutation.uuid.clone())
            .collect();
        if has_aggregate {
            self.preflight_replayed_aggregate_set(&mutations)?;
        }
        let attribution_scopes = if attribution_source_events_enabled() {
            attribution_scopes(&mutations, storage_prefix)?
        } else {
            Vec::new()
        };
        // Clock observation writes durable replay metadata. Keep it inside the
        // same suppression scope as the replay so neither write enters outbox.
        let _kv_suppress = self
            .capture_router()
            .map(|router| router.enter_kv_suppress());
        crate::sync::capture::with_capture_suppressed(async {
            // A replay can add user-plane data. It must cross the same durable
            // source boundary as a request write, but it must never re-enter
            // the cloud mutation log.
            self.db_ops
                .attribution()
                .begin_pending_scopes(&attribution_scopes)
                .await?;
            let author_clock_persist = self.observe_replayed_author_clocks(&mutations)?;
            if let Some((reservation, state)) = author_clock_persist {
                // The replay cursor can advance only after this high-water row
                // and the schema-lane data envelope are durable.
                reservation.submit_and_wait(state).await?;
            }
            let mut remaining = mutations;
            for _ in 0..4 {
                let mut accepted = Vec::with_capacity(remaining.len());
                for mutation in remaining {
                    if let Some(winning_delete) = self
                        .replayed_put_blocked_by_delete(&mutation, storage_prefix)
                        .await?
                    {
                        if has_aggregate {
                            return Err(SchemaError::InvalidData(
                                "a Delete barrier blocks part of a replayed aggregate batch".into(),
                            ));
                        }
                        self.record_replayed_put_lost_to_delete(
                            &mutation,
                            &winning_delete,
                            storage_prefix,
                        )
                        .await?;
                    } else {
                        accepted.push(mutation);
                    }
                }
                if accepted.is_empty() {
                    self.record_attribution_scopes(&attribution_scopes).await?;
                    return Ok(mutation_ids);
                }
                let result = if has_aggregate {
                    self.apply_replayed_aggregate_set(accepted.clone(), storage_prefix)
                        .await
                } else {
                    self.write_with_aggregate_invalidations(
                        accepted.clone(),
                        storage_prefix,
                        WriteOrigin::Replay,
                        None,
                    )
                    .await
                };
                match result {
                    Ok(_) => {
                        self.record_attribution_scopes(&attribution_scopes).await?;
                        return Ok(mutation_ids);
                    }
                    Err(SchemaError::ReplayDeleteBarrierChanged) => remaining = accepted,
                    Err(error) => return Err(error),
                }
            }
            Err(SchemaError::InvalidData(
                "Delete barrier changed too often during mutation replay".into(),
            ))
        })
        .await
    }

    #[cfg(feature = "cloud-sync")]
    pub(super) async fn replayed_put_blocked_by_delete(
        &self,
        mutation: &Mutation,
        storage_prefix: Option<&str>,
    ) -> Result<Option<crate::atom::delete_barrier::DeleteBarrier>, SchemaError> {
        if !matches!(
            mutation.mutation_type,
            MutationType::Create | MutationType::Update
        ) {
            return Ok(None);
        }
        let mut schema = self
            .schema_manager
            .get_schema_metadata(&mutation.schema_name)?
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "Schema '{}' not found for replay Delete barrier check",
                    mutation.schema_name
                ))
            })?;
        apply_storage_prefix_to_schema(&mut schema, storage_prefix);
        let mut resolved = mutation.clone();
        resolved.key_value =
            Self::resolve_mutation_key_value(&mutation.schema_name, &schema, mutation)?;
        let candidates = crate::fold_db_core::purge::plan_normal_delete_barriers(
            &self.db_ops,
            &schema,
            std::slice::from_ref(&resolved),
        )?;
        for candidate in candidates {
            if let Some(barrier) = self
                .db_ops
                .atoms()
                .winning_delete_barrier(&candidate.mk_key)
                .await?
            {
                if barrier.order_key() >= candidate.order_key() {
                    return Ok(Some(barrier));
                }
            }
        }
        Ok(None)
    }

    /// Preserve a rejected source Put before the replay cursor advances.
    /// The source field values remain available even when no local tip uses them.
    #[cfg(feature = "cloud-sync")]
    pub(super) async fn record_replayed_put_lost_to_delete(
        &self,
        mutation: &Mutation,
        winning_delete: &crate::atom::delete_barrier::DeleteBarrier,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        // lint:fn-size-ok moved verbatim from write.rs; one sequential preserve-then-flush step
        use crate::atom::{
            FieldKey, MutationEvent, MutationEventKind, SourceMutationOrder, SuppressedByDelete,
        };

        if mutation.fields_and_values.is_empty() {
            // HashRange normalization can synthesize key-field atoms. Those
            // are not retained source field bodies from this replay envelope.
            return Err(SchemaError::InvalidData(
                "suppressed Put has no source atoms".into(),
            ));
        }

        let _pending_persist_task = self.pending_tasks.begin();
        let mut schema = self
            .schema_manager
            .get_schema_metadata(&mutation.schema_name)?
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "Schema '{}' not found for suppressed Put history",
                    mutation.schema_name
                ))
            })?;
        apply_storage_prefix_to_schema(&mut schema, storage_prefix);
        schema.ensure_record_molecule_runtime_field();
        let mut source_mutations = vec![mutation.clone()];
        let (keys, atoms, located) = self.prepare_atoms_and_key_values(
            &mutation.schema_name,
            &schema,
            &mut source_mutations,
            storage_prefix,
        )?;
        let located = located
            .ok_or_else(|| SchemaError::InvalidData("suppressed Put has no source atoms".into()))?;
        if atoms.is_empty() {
            // An ack lets replay advance its cursor. Keep a source Put with no
            // atom on the log until it can be recorded or rejected upstream.
            return Err(SchemaError::InvalidData(
                "suppressed Put has no source atoms".into(),
            ));
        }

        let written_at = mutation.imported_written_at.ok_or_else(|| {
            SchemaError::InvalidData("suppressed Put lacks the original device write time".into())
        })?;
        let timestamp_nanos = i64::try_from(written_at).map_err(|_| {
            SchemaError::InvalidData("suppressed Put write time exceeds history range".into())
        })?;
        let source_writer = if mutation.author_clock_writer_id.is_empty() {
            mutation.pub_key.as_str()
        } else {
            mutation.author_clock_writer_id.as_str()
        };
        let source_uuid = mutation
            .replayed_source_mutation_uuid
            .as_deref()
            .unwrap_or(mutation.uuid.as_str());
        let field_key = FieldKey::from(keys[0].clone());
        let mut events = Vec::with_capacity(atoms.len());
        for (_, field_name, atom) in atoms {
            let field = schema.runtime_fields.get(&field_name).ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "suppressed Put field '{}' is absent from schema '{}'",
                    field_name, mutation.schema_name
                ))
            })?;
            let molecule_uuid = field.common().molecule_uuid().ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "suppressed Put field '{field_name}' has no molecule"
                ))
            })?;
            let mut identity = Sha256::new();
            for part in [
                source_writer,
                source_uuid,
                field_name.as_str(),
                atom.uuid(),
                keys[0].hash.as_deref().unwrap_or(""),
                keys[0].range.as_deref().unwrap_or(""),
            ] {
                identity.update((part.len() as u64).to_be_bytes());
                identity.update(part.as_bytes());
            }
            identity.update(mutation.logical_counter.to_be_bytes());
            let key = crate::schema::types::field::build_storage_key(
                storage_prefix,
                &format!(
                    "{}:suppressed:{:x}",
                    crate::atom::molecule_key_codec::history_event_key(
                        molecule_uuid,
                        timestamp_nanos,
                    ),
                    identity.finalize()
                ),
            );
            let event = MutationEvent {
                molecule_uuid: molecule_uuid.clone(),
                timestamp: chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(timestamp_nanos),
                field_key: field_key.clone(),
                old_atom_uuid: None,
                new_atom_uuid: atom.uuid().to_string(),
                kind: MutationEventKind::SuppressedPut,
                version: 0,
                is_conflict: true,
                conflict_loser_atom: None,
                writer_pubkey: mutation.pub_key.clone(),
                signature: String::new(),
                provenance: mutation.provenance.clone(),
                source_order: Some(SourceMutationOrder {
                    written_at,
                    logical_counter: mutation.logical_counter,
                    device_id: source_writer.to_string(),
                    mutation_uuid: source_uuid.to_string(),
                }),
                suppressed_by_delete: Some(SuppressedByDelete {
                    mk_key: winning_delete.mk_key.clone(),
                    written_at: winning_delete.written_at,
                    logical_counter: winning_delete.logical_counter,
                    device_id: winning_delete.device_id.clone(),
                    mutation_uuid: winning_delete.mutation_uuid.clone(),
                }),
            };
            events.push((key, event));
        }

        // The source log carries full field bodies. Write them before the
        // history references; a failed write leaves the replay cursor in place.
        self.db_ops
            .atoms()
            .batch_store_atoms_located_borrowed(&located, storage_prefix)
            .await?;
        self.db_ops
            .atoms()
            .batch_store_suppressed_put_events(events, storage_prefix)
            .await?;
        // The download cursor can advance when this callback returns. Flush
        // the atom bodies, history rows, and reference edges first, including
        // when LastStore defers ordinary batch flushes.
        self.db_ops.atoms().flush().await.map_err(|error| {
            SchemaError::InvalidData(format!("flush suppressed Put history: {error}"))
        })
    }
}
