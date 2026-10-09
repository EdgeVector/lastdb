use super::*;

impl PinLog {
    /// Delete durable pin-log records that were **dropped from the outgoing
    /// stream and never published**, after tombstoning each one.
    ///
    /// This is deliberately NOT
    /// [`Self::truncate_confirmed_pin_log_records`]. That function deletes only
    /// frontiers cloud has confirmed, and its contract says why: "anything
    /// weaker risks dropping a record no peer can recover." A quarantined
    /// frontier is the exact opposite of a confirmed one — cloud never saw it —
    /// so routing the drop through a parameter named `confirmed_frontiers` made
    /// the call site read as safe while doing the thing that doc comment warns
    /// about. Same delete, opposite justification, and it needs its own name.
    ///
    /// The record still has to go. An unsealable row can never be uploaded (its
    /// atom is gone), and leaving it durable means every later cycle pays to
    /// scan past it forever. What was missing is the receipt.
    ///
    /// **Tombstone first, then delete.** If the tombstone write fails the row is
    /// left in place and retried next cycle: an untraced hole is worse than a
    /// row that costs one scan slot. Skipping the row is already decided by the
    /// caller, so the cycle makes progress either way — only the reclaim waits.
    pub(super) async fn drop_unsealable_pin_log_records(
        &self,
        engine: &SyncEngine,
        target_id: &str,
        target_prefix: &str,
        quarantined: &[(u64, String)],
    ) -> usize {
        if quarantined.is_empty() {
            return 0;
        }
        let store = match self.pin_log_store().await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    target: "fold_db::sync::mutation_log",
                    error = %e,
                    "quarantine drop skipped: cannot open durable pin log"
                );
                return 0;
            }
        };
        let mut removed = 0usize;
        for (frontier, reason) in quarantined {
            if let Err(e) = engine
                .save_upload_quarantine_tombstone(target_prefix, *frontier, reason)
                .await
            {
                tracing::warn!(
                    target: "fold_db::sync::mutation_log",
                    error = %e,
                    frontier = *frontier,
                    "quarantine tombstone failed; keeping the durable row so the hole stays traceable (retried next cycle)"
                );
                continue;
            }
            let key = pin_log_entry_key(target_id, *frontier);
            match store.delete(&key).await {
                Ok(true) => removed += 1,
                Ok(false) => {}
                Err(e) => {
                    tracing::warn!(
                        target: "fold_db::sync::mutation_log",
                        error = %e,
                        frontier = *frontier,
                        "quarantine drop: delete failed (retried next cycle)"
                    );
                }
            }
        }
        self.maybe_compact_pin_log_plane(removed).await;
        removed
    }

    /// Delete durable pin-log records that cloud has **confirmed**.
    ///
    /// Called only after a successful cloud publish, and only for the exact
    /// frontiers that were confirmed — never a range inferred from a
    /// high-water mark. Anything weaker risks dropping a record no peer can
    /// recover.
    ///
    /// Why this exists: capture appends a durable record per commit, and
    /// nothing ever removed them. On the primary the pin log grew
    /// **138 MiB -> 10.45 GiB in about a day** (store 13.70 -> 25.67 GiB),
    /// feeding the memory pressure that was SIGKILLing the daemon. Capture was
    /// on, publish was a stub, and truncation was gated on publish — so the log
    /// had no exit path at all.
    ///
    /// Best-effort: a delete failure is logged and retried next cycle. It must
    /// never fail a publish that already succeeded, and never block local R/W.
    pub(super) async fn truncate_confirmed_pin_log_records(
        &self,
        target_id: &str,
        confirmed_frontiers: &[u64],
    ) -> usize {
        if confirmed_frontiers.is_empty() {
            return 0;
        }
        let store = match self.pin_log_store().await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    target: "fold_db::sync::mutation_log",
                    error = %e,
                    "pin-log truncate skipped: cannot open durable pin log"
                );
                return 0;
            }
        };
        let mut removed = 0usize;
        for frontier in confirmed_frontiers {
            let key = pin_log_entry_key(target_id, *frontier);
            match store.delete(&key).await {
                Ok(true) => removed += 1,
                Ok(false) => {}
                Err(e) => {
                    tracing::warn!(
                        target: "fold_db::sync::mutation_log",
                        error = %e,
                        frontier = *frontier,
                        "pin-log truncate: delete failed (retried next cycle)"
                    );
                }
            }
        }
        self.maybe_compact_pin_log_plane(removed).await;
        removed
    }

    /// Return the bytes the deletes above only *logically* freed.
    ///
    /// `LastStore` is append-structured: `delete` writes a delete line and
    /// leaves the original record in the segment, so a truncation cycle makes
    /// the plane's files **larger**, not smaller. Measured on the primary with
    /// sync fully healthy and truncation firing every cycle, `sync_pin_log`
    /// grew 21,389 -> 21,621 MiB in 91 minutes — about 3.7 GiB/day — while the
    /// whole store held 1.65 GB of real record content.
    ///
    /// Compaction rewrites only the keys still live in the shard index, which
    /// is exactly the set of records cloud has *not* confirmed, and deletes the
    /// superseded segment files. Running it here rather than leaving it to
    /// `lastdb db compact --collection sync_pin_log --execute` is the whole
    /// point: an operator command that nobody runs unattended is not a
    /// retention policy.
    ///
    /// Safety and cost:
    /// - Best-effort. The publish that led here already succeeded; a compaction
    ///   failure is logged and retried once the counter refills. It must never
    ///   fail a publish and never block local reads or writes.
    /// - `compact_collection` locks one shard of one collection at a time, so
    ///   only pin-log writes wait, and only per shard. `main`, `atoms`, and
    ///   `tips` are untouched.
    /// - The plane is capture-skipped (`SYNC_INTERNAL_NAMESPACES`), so this
    ///   rewrite produces no mutation-log records. Compacting a *captured*
    ///   plane is what turned the 2026-08-08 `tips` compaction into 11.58 GiB
    ///   of new pin log; this cannot repeat that.
    ///
    /// # Why a row counter alone is not a retention policy
    ///
    /// `truncated_since_compact` counts rows deleted by **this process**. It is
    /// an in-memory `AtomicU64`, so every restart throws the accumulated budget
    /// away — the same process-local-state bug that made truncate-after-confirm
    /// never survive a restart in the first place. Measured on the primary
    /// 2026-08-17T06:52Z, 43 minutes after a healthy start with `degraded=false`
    /// and `log_lag=0`:
    ///
    /// ```text
    /// lastdb db compact --collection sync_pin_log   (dry run)
    ///   live_keys      1
    ///   bytes_before   21_951_142_027      # 20.4 GiB, 52% of a 39 GiB store
    ///   never_compact  false
    ///   skipped_reason null
    /// grep -ci 'pin.log' lastdbd.err.log  ->  0
    /// ```
    ///
    /// Compaction was permitted, would have collapsed the plane to one record,
    /// and had not been attempted once: 1,359 segment files from nine days
    /// earlier were still on disk. The row trigger never armed because the
    /// primary restarts (safe upgrades, memory guard) more often than a publish
    /// stream refills 20,000 confirmed rows.
    ///
    /// So the second trigger reads the **plane's on-disk size**, which is state,
    /// not session history. A bloated plane inherited from a previous process is
    /// noticed on the first publish cycle after start, and the plane is bounded
    /// by construction rather than by how long this process has been up.
    pub(super) async fn maybe_compact_pin_log_plane(&self, removed: usize) {
        // Packing lock: hold through the rewrite. A boolean pre-check would
        // race a new BackupPublishTarget cut, retiring chunks under an
        // uncommitted manifest. Same shape as the capture-worker compactors.
        let backup_target = self.backup_publish_target.lock().await;
        if backup_target.is_some() {
            tracing::debug!(
                target: "fold_db::sync::mutation_log",
                "skipping pin-log compaction while a backup publish target is held"
            );
            return;
        }
        let rows_due = self.rows_due_for_compaction(removed);
        // Probe even when `removed == 0`: bloat left by an earlier process is
        // exactly the case the row counter cannot see.
        let bytes_due = if rows_due.is_some() {
            None
        } else {
            self.bloated_plane_bytes()
        };
        let (pending, trigger) = match (rows_due, bytes_due) {
            (Some(pending), _) => (pending, "row_budget"),
            (None, Some(bytes)) => {
                tracing::info!(
                    target: "fold_db::sync::mutation_log",
                    plane_bytes = bytes,
                    max_plane_bytes = self.compact_max_plane_bytes,
                    "pin-log plane is over its on-disk cap; compacting without \
                     waiting for the row budget"
                );
                // A size-triggered rewrite reclaims whatever the pending rows
                // would have, so their budget is spent too.
                let pending = self
                    .truncated_since_compact
                    .swap(0, std::sync::atomic::Ordering::SeqCst);
                (pending, "plane_bytes")
            }
            (None, None) => return,
        };

        let options = crate::storage::laststore::CollectionCompactOptions {
            collection: PIN_LOG_NAMESPACE.to_string(),
            dry_run: false,
            seed_committed_history: false,
        };
        match self.store.compact_collection(options).await {
            Ok(report) => {
                if let Some(reason) = report.skipped_reason.as_deref() {
                    tracing::warn!(
                        target: "fold_db::sync::mutation_log",
                        reason,
                        "pin-log compaction refused; plane keeps growing until this is fixed"
                    );
                    return;
                }
                let reclaimed = report
                    .bytes_after
                    .map(|after| report.bytes_before.saturating_sub(after));
                self.raise_size_trigger_floor(report.bytes_after);
                *self.last_compact_trigger.lock().await = Some(trigger.to_string());
                tracing::info!(
                    target: "fold_db::sync::mutation_log",
                    trigger,
                    truncated_rows = pending,
                    live_keys = report.live_keys,
                    bytes_before = report.bytes_before,
                    bytes_after = report.bytes_after,
                    reclaimed_bytes = reclaimed,
                    "pin-log plane compacted after confirmed truncation"
                );
            }
            Err(e) => {
                tracing::warn!(
                    target: "fold_db::sync::mutation_log",
                    error = %e,
                    trigger,
                    truncated_rows = pending,
                    "pin-log compaction failed (retried on a later cycle)"
                );
            }
        }
    }

    /// Has this process truncated enough confirmed rows to pay for a rewrite?
    ///
    /// Returns the pending row count and *claims* it, so two concurrent publish
    /// cycles cannot both start a compaction of the same plane.
    pub(super) fn rows_due_for_compaction(&self, removed: usize) -> Option<u64> {
        if removed == 0 {
            return None;
        }
        let threshold = self.compact_after_rows;
        if threshold == 0 {
            return None;
        }
        let before = self
            .truncated_since_compact
            .fetch_add(removed as u64, std::sync::atomic::Ordering::Relaxed);
        let pending = before.saturating_add(removed as u64);
        if pending < threshold {
            return None;
        }
        // Claim the budget before the caller awaits.
        self.truncated_since_compact
            .compare_exchange(
                pending,
                0,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::Relaxed,
            )
            .ok()
            .map(|_| pending)
    }

    /// Raise the size trigger's floor so a legitimately large live set cannot
    /// turn the cap into a rewrite treadmill.
    ///
    /// The cap assumes the plane's live content is near zero, which is true when
    /// sync is healthy — the live set is the unconfirmed publish backlog. During
    /// a long sync outage it is not: nothing is confirmed, so nothing can be
    /// truncated, and the plane can hold more than the cap in records that must
    /// be kept. Without a floor the size trigger would then fire on every probe
    /// interval and rewrite an over-cap plane every few minutes, reclaiming
    /// nothing.
    ///
    /// Doubling the post-compaction size means the next size-triggered rewrite
    /// waits for the plane to double again — so growth stays bounded while the
    /// rewrite rate falls with the live set's size. The floor never drops below
    /// the cap, and a compaction that genuinely emptied the plane leaves the
    /// floor at the cap.
    pub(super) fn raise_size_trigger_floor(&self, bytes_after: Option<u64>) {
        let Some(after) = bytes_after else {
            return;
        };
        let floor = after.saturating_mul(2).max(self.compact_max_plane_bytes);
        self.size_trigger_floor_bytes
            .store(floor, std::sync::atomic::Ordering::Relaxed);
    }

    /// Plane bytes, if the plane is over its cap and a probe is due.
    ///
    /// Rate-limited because this runs on the publish path: the probe is a stat
    /// walk of one collection directory (no shard loads, no index rebuild — see
    /// [`crate::storage::traits::NamespacedStore::collection_disk_bytes`]), but
    /// a healthy primary publishes every few tens of seconds and there is no
    /// reason to walk 2,600 files that often.
    ///
    /// Returns `None` when the cap is disabled, a probe is not yet due, the
    /// backend cannot measure bytes, or the plane is within its cap. Claiming
    /// the probe slot before measuring means a concurrent cycle sees "not due"
    /// rather than racing into a second compaction.
    pub(super) fn bloated_plane_bytes(&self) -> Option<u64> {
        let cap = self.compact_max_plane_bytes;
        if cap == 0 {
            return None;
        }
        let now_s = crate::clock::unix_secs();
        let last = self
            .last_bloat_probe_unix_s
            .load(std::sync::atomic::Ordering::Relaxed);
        if last != 0 && now_s.saturating_sub(last) < self.compact_bloat_probe_interval_s {
            return None;
        }
        if self
            .last_bloat_probe_unix_s
            .compare_exchange(
                last,
                now_s.max(1),
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_err()
        {
            return None;
        }
        let bytes = self.store.collection_disk_bytes(PIN_LOG_NAMESPACE)?;
        let floor = self
            .size_trigger_floor_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
            .max(cap);
        (bytes > floor).then_some(bytes)
    }

    pub(crate) async fn last_compact_trigger(&self) -> Option<String> {
        self.last_compact_trigger.lock().await.clone()
    }
}

/// Confirmed rows that must be truncated before the pin-log plane is compacted.
///
/// This is the reclaim/throughput knob. Compaction rewrites every live record
/// in the plane, so firing it after a handful of deletes would pay a full
/// rewrite for a few kilobytes; letting it never fire costs ~3.7 GiB/day of
/// dead segment bytes on a busy primary. The default amortises one rewrite
/// across a meaningful truncation batch while still bounding how far the plane
/// can drift from its live content.
///
/// Set `LASTDB_PIN_LOG_COMPACT_AFTER_ROWS=0` to disable self-compaction and
/// leave reclaim entirely to `lastdb db compact --collection sync_pin_log`.
pub(super) fn pin_log_compact_after_rows() -> u64 {
    env_flag::var_or("LASTDB_PIN_LOG_COMPACT_AFTER_ROWS", 20_000u64)
}

/// On-disk bytes above which the pin-log plane is compacted regardless of the
/// row budget — the trigger that actually bounds the plane.
///
/// The row counter is process-local and a restart zeroes it, so on a primary
/// that restarts for safe upgrades and memory-guard events it can arm slower
/// than the plane bloats. On 2026-08-17 that left `sync_pin_log` holding
/// **20.4 GiB behind one live record**, 52% of the store, with compaction
/// permitted and never attempted. A size cap cannot drift that way: it is read
/// off the disk, so an inherited bloated plane is noticed on the first publish
/// cycle after start.
///
/// The primary's measured healthy live set is 5 rows / 620 KiB. A 16 MiB cap
/// leaves ample headroom while reaching the measured 11.6 MiB/hour churn in
/// roughly 1.4 hours, inside a normal daemon session; the previous 512 MiB cap
/// needed about 44 hours. Set
/// `LASTDB_PIN_LOG_COMPACT_MAX_BYTES=0` to disable the size trigger and keep
/// only the row budget.
pub(super) fn pin_log_compact_max_plane_bytes() -> u64 {
    env_flag::var_or("LASTDB_PIN_LOG_COMPACT_MAX_BYTES", 16 * 1024 * 1024)
}

/// Minimum seconds between on-disk bloat probes.
///
/// The probe is a directory stat walk, cheap next to a publish cycle, but it
/// runs on the publish path and there is nothing to gain from walking the same
/// files every cycle. Override with
/// `LASTDB_PIN_LOG_COMPACT_PROBE_INTERVAL_SECS`.
pub(super) fn pin_log_compact_bloat_probe_interval_s() -> u64 {
    env_flag::var_or("LASTDB_PIN_LOG_COMPACT_PROBE_INTERVAL_SECS", 300u64)
}

// lint:file-size-ok moved verbatim from pin_log.rs; cohesive unit, split further in a later pass
