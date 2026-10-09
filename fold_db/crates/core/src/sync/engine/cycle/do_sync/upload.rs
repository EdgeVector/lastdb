use super::*;

use crate::sync::engine::upload_policy::UploadPolicySnapshot;

impl SyncEngine {
    /// Select the memory-bounded upload batch for this cycle and record the
    /// cycle's upload stats. Returns the entries and their serialized size.
    pub(super) async fn select_and_record_upload_batch(
        &self,
        backup_blocked: bool,
        upload_caps: &UploadPolicySnapshot,
    ) -> (Vec<LogEntry>, u64) {
        // Upload pending entries, partitioned across targets. Cap what this
        // cycle will seal/PUT so a multi-thousand outbox cannot monopolise RAM
        // (re-enable thrash 2026-07-14: download=0, pending≈3757, swap 3→26GB).
        //
        // Size is measured once here and reused for stats/logs — never call
        // `serialized_len()` again on the selected batch (full JSON re-encode
        // of a multi-MB BatchPut was enough to stall the cycle under swap).
        let (entries, bytes_selected): (Vec<LogEntry>, u64) = if backup_blocked {
            // Selecting a batch here would clone entries out of `pending`
            // into a per-target partition that the proof gate is guaranteed
            // to refuse. This is the allocation the block exists to avoid.
            (Vec::new(), 0)
        } else if self.config.legacy_personal_cloud_sync {
            let pending = self.pending.lock().await;
            let selected = Self::select_upload_batch(
                &pending,
                upload_caps.max_upload_entries,
                upload_caps.max_upload_bytes,
            );
            let bytes_selected: u64 = selected.iter().map(|e| e.serialized_len() as u64).sum();
            tracing::info!(
                target: "fold_db::sync::memory",
                pending_len = pending.len(),
                selected = selected.len(),
                bytes_selected,
                budget_bytes = upload_caps.budget_bytes,
                concurrency = upload_caps.concurrency,
                "select_upload_batch for this cycle"
            );
            (selected, bytes_selected)
        } else {
            (Vec::new(), 0)
        };
        {
            let queued = self.pending.lock().await.len();
            let selected = entries.len();
            let deferred = queued.saturating_sub(selected);
            let truncated_by_entry_cap = upload_caps.max_upload_entries > 0
                && queued > upload_caps.max_upload_entries
                && selected == upload_caps.max_upload_entries;
            let truncated_by_byte_cap =
                upload_caps.max_upload_bytes > 0 && deferred > 0 && !truncated_by_entry_cap;
            *self.last_upload_stats.lock().await = Some(UploadCycleStats {
                entries_queued: queued,
                entries_selected: selected,
                bytes_selected,
                entries_deferred: deferred,
                truncated_by_entry_cap,
                truncated_by_byte_cap,
            });
            if deferred > 0 {
                tracing::info!(
                    target: "fold_db::sync::memory",
                    queued,
                    selected,
                    bytes_selected,
                    deferred,
                    budget_bytes = upload_caps.budget_bytes,
                    "upload cycle capped; remaining outbox deferred to later ticks"
                );
            }
        }
        (entries, bytes_selected)
    }

    /// Partition the selected entries across targets, upload each bucket
    /// under its own crypto, and drain what was uploaded from the outbox.
    pub(super) async fn upload_partitioned_entries(
        &self,
        entries: &[LogEntry],
        bytes_selected: u64,
        targets: &[SyncTarget],
        partitioner: &Option<SyncPartitioner>,
        personal_idxs: &[usize],
        state: &mut CycleState,
    ) -> SyncResult<()> {
        tracing::info!(
            target: "fold_db::sync::memory",
            entries = entries.len(),
            bytes = bytes_selected,
            "beginning upload partition for this cycle"
        );
        // Partition entries across targets by key prefix.
        // Batches with mixed-prefix keys are split into one sub-entry per target
        // so each chunk is sealed under the correct crypto provider.
        let mut buckets: std::collections::HashMap<usize, Vec<LogEntry>> =
            std::collections::HashMap::new();
        for entry in entries {
            for (idx, sub_entry) in Self::partition_entry(partitioner, entry, targets)? {
                buckets.entry(idx).or_default().push(sub_entry);
            }
        }
        tracing::info!(
            target: "fold_db::sync::memory",
            buckets = buckets.len(),
            "upload partition complete; starting per-target upload"
        );

        // Upload each bucket with its target's crypto
        // Track only personal-target uploads, since personal compaction uses
        // these counters for its entry-count backstop and size trigger.
        // Personal is identified by the split above, not by target position:
        // scoped-only configurations can place a scoped target at index 0.
        let mut tally = UploadTally {
            all_succeeded: true,
            auth_error: false,
            single_bucket: buckets.len() == 1,
            personal_uploaded: 0,
            personal_bytes: 0,
        };
        for (target_idx, bucket) in &buckets {
            tracing::info!(
                "uploading {} entries to target {} ('{}')",
                bucket.len(),
                target_idx,
                targets.get(*target_idx).map_or("?", |t| t.label.as_str())
            );
            let target = &targets[*target_idx];

            self.upload_bucket(
                target,
                bucket,
                personal_idxs.contains(target_idx),
                state,
                &mut tally,
            )
            .await;
        }

        // Propagate auth errors so the top-level sync() can refresh and retry
        if tally.auth_error {
            return Err(SyncError::Auth(
                "upload failed due to auth error".to_string(),
            ));
        }

        self.drain_uploaded_entries(entries, state, &tally).await
    }

    /// Upload one target's bucket, proving the prefix decryptable first.
    async fn upload_bucket(
        &self,
        target: &SyncTarget,
        bucket: &[LogEntry],
        is_personal: bool,
        state: &mut CycleState,
        tally: &mut UploadTally,
    ) {
        // Poison gate: before appending to a prefix whose tail we did
        // NOT just decrypt via replay, require a positive proof that the
        // current key still opens the existing cloud data. This closes
        // the cursor-at-head / compacted-tail bypass — a drifted-key
        // node with an intact cursor otherwise uploads wrong-key
        // ciphertext that no correct device can ever unseal.
        if !state.proven_prefixes.contains(&target.prefix) {
            if let Err(e) = self.prove_prefix_decryptable(target).await {
                self.record_cloud_sync_transfer_failure("upload", &target.label, &e)
                    .await;
                tracing::warn!(
                    "upload to '{}' blocked: pre-upload decrypt proof failed: {}",
                    target.label,
                    redact_sync_error_text(&e.to_string())
                );
                state.first_transfer_error = Some(select_more_severe_transfer_error(
                    state.first_transfer_error.take(),
                    e,
                ));
                tally.all_succeeded = false;
                return;
            }
            // Proven now — don't re-prove this prefix for later buckets.
            state.proven_prefixes.insert(target.prefix.clone());
        }

        match self.upload_entries(target, bucket).await {
            Ok(outcome) => {
                state.uploaded += outcome.entries_uploaded;
                if outcome.entries_uploaded != bucket.len() {
                    tally.all_succeeded = false;
                }
                if let Some(e) = outcome.partial_error {
                    self.record_cloud_sync_transfer_failure("upload", &target.label, &e)
                        .await;
                    let redacted_error = redact_sync_error_text(&e.to_string());
                    tracing::warn!(
                        "upload to '{}' stopped after partial progress: {}",
                        target.label,
                        redacted_error
                    );
                    state.first_transfer_error = Some(select_more_severe_transfer_error(
                        state.first_transfer_error.take(),
                        e,
                    ));
                }
                if is_personal {
                    tally.personal_uploaded += outcome.entries_uploaded;
                    // Sum serialized bytes of the entries actually uploaded
                    // (a full bucket success uploads all of them).
                    tally.personal_bytes += bucket
                        .iter()
                        .take(outcome.entries_uploaded)
                        .map(|e| e.serialized_len() as u64)
                        .sum::<u64>();
                }
            }
            Err(ref e) if matches!(e, SyncError::Auth(_)) => {
                self.record_cloud_sync_transfer_failure("upload", &target.label, e)
                    .await;
                tracing::warn!(
                    "upload to '{}' failed (auth): {}",
                    target.label,
                    redact_sync_error_text(&e.to_string())
                );
                tally.auth_error = true;
                tally.all_succeeded = false;
            }
            Err(e) => {
                self.record_cloud_sync_transfer_failure("upload", &target.label, &e)
                    .await;
                tracing::warn!(
                    "upload to '{}' failed: {}",
                    target.label,
                    redact_sync_error_text(&e.to_string())
                );
                state.first_transfer_error = Some(select_more_severe_transfer_error(
                    state.first_transfer_error.take(),
                    e,
                ));
                tally.all_succeeded = false;
            }
        }
    }

    /// Drop uploaded entries from the pending queue and the durable outbox,
    /// and credit personal log growth toward the compaction triggers.
    async fn drain_uploaded_entries(
        &self,
        entries: &[LogEntry],
        state: &CycleState,
        tally: &UploadTally,
    ) -> SyncResult<()> {
        // Clear uploaded entries from pending. Partitioning may split one
        // original entry into multiple target-specific sub-entries, so the
        // drain condition must be per bucket rather than `uploaded >=
        // entries.len()`. If any target failed, keep every original entry
        // so the missing target retries next cycle.
        if tally.all_succeeded {
            let mut pending = self.pending.lock().await;
            let count = entries.len().min(pending.len());
            pending.drain(..count);
            drop(pending);
            self.remove_outbox_entries(&entries[..count])
                .await
                .map_err(SyncError::Storage)?;
            self.schedule_outbox_entries()
                .await
                .map_err(SyncError::Storage)?;
            // Accumulate personal log growth toward the size-based compaction
            // trigger. Only count a fully-drained cycle so we never double-count
            // entries kept for retry.
            if tally.personal_uploaded > 0 {
                *self.bytes_since_snapshot.lock().await += tally.personal_bytes;
                *self.entries_since_snapshot.lock().await += tally.personal_uploaded as u64;
            }
        } else if state.uploaded > 0 && tally.single_bucket {
            let mut pending = self.pending.lock().await;
            let count = state.uploaded.min(entries.len()).min(pending.len());
            pending.drain(..count);
            drop(pending);
            self.remove_outbox_entries(&entries[..count])
                .await
                .map_err(SyncError::Storage)?;
            self.schedule_outbox_entries()
                .await
                .map_err(SyncError::Storage)?;
            // The prefix we just drained is permanently gone from the outbox
            // (not "kept for retry"). Credit personal log growth for those
            // entries so size/entry compaction triggers track real uploaded
            // work. Use the drained `count` (not `tally.personal_uploaded`) so a
            // min() against pending length cannot over-count.
            if tally.personal_uploaded > 0 && count > 0 {
                *self.bytes_since_snapshot.lock().await += tally.personal_bytes;
                *self.entries_since_snapshot.lock().await += count as u64;
            }
            tracing::warn!(
                    "partial upload: {}/{} entries succeeded for a single target; drained that prefix and kept the remainder for retry",
                    count,
                    entries.len()
                );
        } else if state.uploaded > 0 {
            tracing::warn!(
                    "partial upload: {}/{} target-specific entries succeeded across multiple targets; keeping all original entries in pending for safe retry",
                    state.uploaded,
                    entries.len()
                );
        }
        Ok(())
    }
}

/// Outcome of the per-bucket uploads of one cycle.
struct UploadTally {
    all_succeeded: bool,
    auth_error: bool,
    single_bucket: bool,
    personal_uploaded: usize,
    personal_bytes: u64,
}
