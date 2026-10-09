//! Recording a single op or mutation-intent marker into the outbox.
// lint:file-size-ok moved verbatim from outbox.rs; one method family per file

use super::*;

impl SyncEngine {
    pub(super) async fn append_mutation_log_entry(
        &self,
        entry: &LogEntry,
        targets: &[SyncTarget],
        partitioner: &Option<SyncPartitioner>,
    ) -> Result<Vec<super::super::pin_log::MutationLogTargetPosition>, String> {
        self.pin_log
            .ensure_continuous_mutation_log_for_targets(targets)
            .await
            .map_err(|error| format!("continuous mutation-log ensure failed: {error}"))?;
        self.pin_log
            .append_entry_to_active_pin_logs(entry, targets, partitioner)
            .await
            .map_err(|error| format!("continuous mutation-log append failed: {error}"))
    }

    /// Atom ids a legacy reference-only MutationIntent still needs.
    pub(super) fn mutation_intent_gc_roots(
        op: &LogOp,
    ) -> std::collections::HashMap<Option<String>, Vec<String>> {
        let mut grouped: std::collections::HashMap<Option<String>, HashSet<String>> =
            std::collections::HashMap::new();
        if let LogOp::MutationIntent { mutations } = op {
            for envelope in mutations {
                let roots = grouped.entry(envelope.storage_prefix.clone()).or_default();
                roots.extend(
                    envelope
                        .field_atom_uuids
                        .iter()
                        .filter(|(field, _)| !envelope.fields_and_values.contains_key(*field))
                        .map(|(_, uuid)| uuid.clone()),
                );
            }
        }
        grouped
            .into_iter()
            .filter_map(|(prefix, roots)| {
                if roots.is_empty() {
                    None
                } else {
                    let mut roots = roots.into_iter().collect::<Vec<_>>();
                    roots.sort_unstable();
                    Some((prefix, roots))
                }
            })
            .collect()
    }

    /// Record an operation durably, then admit it to the bounded upload queue
    /// if there is room. This method never drops older unsynced entries unless
    /// they exceed the **absolute** upload byte ceiling (config/env
    /// `max_upload_bytes_per_cycle` / `LASTDB_SYNC_MAX_UPLOAD_BYTES`) — those
    /// are forgotten so a multi-GB BatchPut cannot pin the upload head forever.
    /// Adaptive cycle budget may only defer in-memory admit, never durable forget.
    ///
    /// **Mutation-log-first Phase A:** when [`CaptureMode::MutationLog`] is set,
    /// every staged commit group-commits into the durable mutation log while
    /// Cloud Sync is on — even for LastStore homes with
    /// `legacy_personal_cloud_sync = false`. Local R/W never awaits upload.
    pub(crate) async fn record_op(&self, op: LogOp) -> Result<u64, String> {
        Ok(self.record_op_with_publication(op).await?.frontier)
    }

    /// Move a bounded page of durable Async markers into the mutation log.
    /// The pin-log writer flushes an allocation floor, marker receipts, and
    /// target rows in that order. A retry after marker-delete failure reuses
    /// the receipt's positions instead of minting duplicate cloud operations.
    // lint:fn-size-ok verbatim move from outbox.rs; splitting this function is separate work
    pub(crate) async fn record_mutation_intent_marker_batch(
        &self,
        markers: &[(Vec<u8>, Vec<crate::sync::log::MutationEnvelope>)],
    ) -> Result<Vec<Vec<u8>>, String> {
        const MAX_MARKERS: usize = 256;
        if markers.is_empty() {
            return Ok(Vec::new());
        }
        if markers.len() > MAX_MARKERS {
            return Err(format!(
                "capture marker batch exceeds {MAX_MARKERS} markers"
            ));
        }
        let mut total_bytes = 0usize;
        for (marker_key, envelopes) in markers {
            if marker_key.is_empty() || envelopes.is_empty() {
                return Err("capture marker batch has an empty key or mutation intent".into());
            }
            let bytes = serde_json::to_vec(envelopes)
                .map_err(|error| format!("encode capture marker envelope: {error}"))?;
            total_bytes = total_bytes.saturating_add(bytes.len());
            if total_bytes > CAPTURE_MARKER_BATCH_MAX_ENVELOPE_BYTES {
                return Err(format!(
                    "capture marker batch exceeds {CAPTURE_MARKER_BATCH_MAX_ENVELOPE_BYTES} envelope bytes"
                ));
            }
        }
        if !matches!(self.config.capture_mode, CaptureMode::MutationLog)
            || !self.should_stage_cloud_mutations().await
        {
            return Err("continuous cloud capture is unavailable; durable marker retained".into());
        }

        // The destination set cannot change between frontier allocation and
        // the durable rows/receipts. This is the same lock order as record_op.
        let _target_config = self.target_config_lock.lock().await;
        let targets = self.targets.lock().await.clone();
        let partitioner = self.partitioner.lock().await.clone();
        let generation = self
            .target_config_generation
            .load(std::sync::atomic::Ordering::Acquire);
        let marker_keys = markers
            .iter()
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        let existing_receipts = self
            .pin_log
            .capture_marker_receipt_presence(&marker_keys)
            .await?;
        let activation_guard = self.automatic_gc_pin_log_barrier.lock().await;
        let activation_pending = self
            .automatic_gc_pin_log_activation_pending
            .load(std::sync::atomic::Ordering::Acquire);
        let atom_store = self.automatic_gc_atom_store.lock().await.clone();
        let atom_generation = atom_store.as_ref().map_or(
            0,
            crate::db_operations::AtomStore::automatic_gc_atoms_generation,
        );
        let hold_for_activation = activation_pending && atom_generation == 0;
        if !hold_for_activation {
            drop(activation_guard);
        }
        let mut entries = Vec::with_capacity(markers.len());
        for ((marker_key, envelopes), already_recorded) in markers.iter().zip(existing_receipts) {
            let op = crate::sync::mutation_intent::mutation_intent_op(envelopes.clone());
            let mut entry = if already_recorded {
                // The pin-log method validates the full receipt and repairs
                // its original rows. These unused coordinates do not advance
                // the process sequence or a durable allocation floor.
                LogEntry {
                    seq: 0,
                    timestamp_ms: 0,
                    device_id: self.device_id.clone(),
                    op,
                }
            } else {
                self.make_entry_for_target_snapshot(op, generation, &targets)
                    .await?
            };
            entry.op.strip_sot_field_values();

            // Reference-only historical rows still need GC roots until their
            // pin-log append is durable. New self-contained rows have none.
            let roots = Self::mutation_intent_gc_roots(&entry.op);
            if !roots.is_empty() {
                if let Some(atoms) = &atom_store {
                    for (prefix, atom_uuids) in roots {
                        atoms
                            .protect_automatic_gc_atom_references(&atom_uuids, prefix.as_deref())
                            .await
                            .map_err(|error| {
                                format!("protect marker pin-log atom references: {error}")
                            })?;
                    }
                } else if atom_generation > 0 || activation_pending {
                    return Err(
                        "automatic gc-atoms pin protection has no registered atom store"
                            .to_string(),
                    );
                }
            }
            entries.push((marker_key.clone(), entry));
        }
        let cleared = self
            .pin_log
            .append_capture_marker_batch(&entries, &targets, &partitioner)
            .await?;
        self.set_state(SyncState::Dirty, None).await;
        // Product capture is async: the marker drain is the append the
        // coalesce hold must see. Stamp before the wake so that wake does
        // not read "no append in this process" and seal at once.
        self.note_mutation_log_local_append();
        self.wake.notify_one();
        Ok(cleared)
    }

    /// The direct publication path uses the same marker receipt as the
    /// background drain. The caller holds the drain lock through marker
    /// deletion, so the receipt remains available for this exact read.
    pub(crate) async fn record_mutation_intent_marker_with_publication(
        &self,
        marker_key: Vec<u8>,
        envelopes: Vec<crate::sync::log::MutationEnvelope>,
    ) -> Result<super::super::pin_log::MutationLogAppendReceipt, String> {
        self.record_mutation_intent_marker_batch(&[(marker_key.clone(), envelopes)])
            .await?;
        self.pin_log
            .capture_marker_append_receipt(&marker_key)
            .await
    }

    /// Record one operation and return the exact mutation-log target positions.
    // lint:fn-size-ok verbatim move from outbox.rs; splitting this function is separate work
    pub(crate) async fn record_op_with_publication(
        &self,
        op: LogOp,
    ) -> Result<super::super::pin_log::MutationLogAppendReceipt, String> {
        // Intentional sync-off past grace: do not accumulate cloud mutations.
        // Local DB remains source of truth; never map this to a write failure.
        if !self.should_stage_cloud_mutations().await {
            let _ = self.maybe_clear_staging_for_sync_off_past_grace().await;
            return Ok(super::super::pin_log::MutationLogAppendReceipt {
                writer_id: self.device_id.clone(),
                frontier: *self.seq.lock().await,
                durable_capture_written: false,
                targets: Vec::new(),
            });
        }
        // This lock is the linearization point for the destination set. Keep
        // it until the allocation floor and every selected pin-log row reach
        // the same flushed local batch. `configure_targets` therefore cannot
        // add a target with a future durable HWM between frontier allocation
        // and row persistence.
        let target_config = self.target_config_lock.lock().await;
        let targets_snapshot = self.targets.lock().await.clone();
        let partitioner_snapshot = self.partitioner.lock().await.clone();
        let target_generation = self
            .target_config_generation
            .load(std::sync::atomic::Ordering::Acquire);
        let mut entry = self
            .make_entry_for_target_snapshot(op, target_generation, &targets_snapshot)
            .await?;
        let targets;

        // Continuous mutation-log plane (product default for LastStore Mini).
        // Pin-mode freeze also appends via the same durable store when active.
        if matches!(self.config.capture_mode, CaptureMode::MutationLog) {
            entry.op.strip_sot_field_values();
            // A record that starts before marker activation must finish its
            // append before the pin-log scan starts. Once a generation is
            // active, a legacy reference-only row writes its marker first and
            // uses the same per-atom guards as deletion. New self-contained
            // rows have no roots here and pay no marker IO.
            let activation_guard = self.automatic_gc_pin_log_barrier.lock().await;
            let activation_pending = self
                .automatic_gc_pin_log_activation_pending
                .load(std::sync::atomic::Ordering::Acquire);
            let atom_store = self.automatic_gc_atom_store.lock().await.clone();
            let generation = atom_store.as_ref().map_or(
                0,
                crate::db_operations::AtomStore::automatic_gc_atoms_generation,
            );
            let roots = Self::mutation_intent_gc_roots(&entry.op);
            let hold_for_activation = activation_pending && generation == 0;
            if !hold_for_activation {
                drop(activation_guard);
            }
            if !roots.is_empty() {
                if let Some(atoms) = atom_store {
                    for (prefix, atom_uuids) in roots {
                        atoms
                            .protect_automatic_gc_atom_references(&atom_uuids, prefix.as_deref())
                            .await
                            .map_err(|error| {
                                format!("protect pin-log atom references before append: {error}")
                            })?;
                    }
                } else if generation > 0 || activation_pending {
                    return Err(
                        "automatic gc-atoms pin protection has no registered atom store"
                            .to_string(),
                    );
                } else {
                    // Test and legacy engines without a serving AtomStore keep
                    // their pre-existing behavior while no GC lap is active.
                }
            }
            targets = self
                .append_mutation_log_entry(&entry, &targets_snapshot, &partitioner_snapshot)
                .await?;
        } else {
            // Pin-mode path (legacy capture modes): pin append was fallible; propagate.
            targets = self
                .pin_log
                .append_entry_to_active_pin_logs(&entry, &targets_snapshot, &partitioner_snapshot)
                .await?;
        }

        // The durable pin-log batch is now flushed, so a later target change
        // cannot create a destination hole for this receipt.
        drop(target_config);

        // Legacy personal `{user_hash}/log/{seq}` outbox plane — disabled on
        // LastStore homes so we do not dual-write legacy objects + mutation log.
        if !self.config.legacy_personal_cloud_sync {
            if matches!(self.config.capture_mode, CaptureMode::MutationLog) {
                self.set_state(SyncState::Dirty, None).await;
                self.note_mutation_log_local_append();
                self.wake.notify_one();
            }
            let durable_capture_written = !targets.is_empty();
            return Ok(super::super::pin_log::MutationLogAppendReceipt {
                writer_id: entry.device_id,
                frontier: entry.seq,
                durable_capture_written,
                targets,
            });
        }

        let size = entry.serialized_len();
        // Absolute (config/env) max decides durable forget. Adaptive cycle
        // budget only gates whether the entry joins the in-memory upload queue
        // this cycle — never permanent history loss under RSS pressure.
        let caps = self.active_upload_caps().await;
        let forget_max_bytes = self.absolute_outbox_max_bytes();
        let select_max_bytes = caps.max_upload_bytes;
        // Persist first so a crash mid-cycle does not lose the intent — then
        // immediately drop if over the absolute ceiling so a multi-GB BatchPut
        // cannot pin the upload head forever.
        self.persist_outbox_entry(&entry).await?;
        if forget_max_bytes > 0 && size > forget_max_bytes {
            self.drop_oversize_outbox_entry(
                entry.seq,
                size,
                forget_max_bytes,
                "record_op",
                "dropping oversize op at record time (not admitted to upload queue or durable outbox)",
            )
            .await?;
            // Still mark dirty / wake so a later cycle can drain other work;
            // this particular op will not sync (local data remains in the DB).
            self.set_state(SyncState::Dirty, None).await;
            self.wake.notify_one();
            let durable_capture_written = !targets.is_empty();
            return Ok(super::super::pin_log::MutationLogAppendReceipt {
                writer_id: entry.device_id,
                frontier: entry.seq,
                durable_capture_written,
                targets,
            });
        }
        let queue_cap = upload_queue_cap(caps.max_pending, caps.max_upload_entries);
        // An older row deferred by the adaptive budget is still durable-only.
        // Personal upload keys are this client `entry.seq`, so admitting a later
        // seq now would upload past that head and let a peer's `seq > cursor`
        // listing skip the older row forever. Hold the barrier: the row stays
        // durable and a larger-budget cycle refills it in FIFO order.
        let deferred_head = self
            .adaptive_deferred_head()
            .filter(|head| *head < entry.seq);
        let mut pending = self.pending.lock().await;
        let fits_select = select_max_bytes == 0 || size <= select_max_bytes;
        if let Some(head) = deferred_head {
            tracing::info!(
                target: "fold_db::sync::memory",
                outbox_seq = entry.seq,
                size,
                deferred_head_seq = head,
                select_max_bytes,
                forget_max_bytes,
                "durable outbox retained; older adaptive-deferred row holds the FIFO upload head"
            );
        } else if fits_select && (queue_cap == 0 || pending.len() < queue_cap) {
            pending.push(entry.clone());
        } else if !fits_select {
            self.defer_from_seq_for_cycle_locked(&mut pending, entry.seq);
            tracing::info!(
                target: "fold_db::sync::memory",
                outbox_seq = entry.seq,
                size,
                select_max_bytes,
                forget_max_bytes,
                "durable outbox retained; adaptive budget defers in-memory queue admit"
            );
        } else {
            tracing::warn!(
                upload_queue_len = pending.len(),
                queue_cap,
                outbox_seq = entry.seq,
                "sync upload queue full; durable outbox entry will wait for a later upload cycle"
            );
        }
        drop(pending);
        self.set_state(SyncState::Dirty, None).await;
        // Wake the background sync coordinator so a flush fires near-immediately
        // instead of waiting out the full `sync_interval_ms`. Safe under burst
        // writes: `Notify` holds at most one pending notification, so a rapid
        // sequence of writes coalesces into a single early wake-up, and the
        // subsequent sync cycle drains every pending entry in one batch.
        self.wake.notify_one();
        Ok(super::super::pin_log::MutationLogAppendReceipt {
            writer_id: entry.device_id,
            frontier: entry.seq,
            durable_capture_written: true,
            targets,
        })
    }
}
