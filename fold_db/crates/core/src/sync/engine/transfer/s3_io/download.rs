//! S3 download and replay path.

use super::super::super::*;
use crate::sync::error::{SyncError, SyncResult};
use crate::sync::org_sync::SyncTarget;
use futures::stream::StreamExt;

mod dispatch;
mod replay;

use replay::{ensure_contiguous_org_seqs, DownloadProgress};

impl SyncEngine {
    /// Attach the pin location (target + seq) to an apply failure that stopped
    /// the download cursor.
    ///
    /// Replay already logs where it stopped, but a log line is not a field: the
    /// structured `SyncStatus.replay_blocker` is what `/api/status`, the UI, and
    /// the health routines read, and it is populated by matching the error
    /// variant. So an apply failure carrying no location armed nothing, and the
    /// only record of *which* seq was pinned lived in the daemon log (Tom's
    /// primary, 2026-08-16: a catalog-guard refusal pinned one seq for four
    /// hours, blocking every upload behind it, with `replay_blocker: null` the
    /// entire time).
    ///
    /// Two classes are deliberately passed through unwrapped:
    ///
    /// - [`SyncError::Network`] — `record_sync_failure` maps it to
    ///   `SyncState::Offline`, which is the more useful verdict. A node that
    ///   cannot reach the network is offline, not pinned on an entry.
    /// - [`SyncError::CorruptEntry`] / [`SyncError::KeyProofFailed`] — already
    ///   typed, already carry their location, and already classify into a
    ///   blocker whose `action` names a specific remedy. Re-wrapping them would
    ///   trade a diagnosis for a bare fact.
    pub(crate) fn pin_replay_error(target_label: &str, seq: u64, err: SyncError) -> SyncError {
        match err {
            SyncError::Network(_)
            | SyncError::CorruptEntry { .. }
            | SyncError::KeyProofFailed { .. }
            | SyncError::ReplayApplyFailed { .. } => err,
            other => SyncError::ReplayApplyFailed {
                target: target_label.to_string(),
                seq,
                reason: other.to_string(),
                diagnosis: crate::sync::ReplayApplyDiagnosis::new(
                    crate::sync::ReplayOperation::Unknown,
                    crate::sync::ReplayCause::Unknown,
                ),
            },
        }
    }

    /// Personal (empty prefix) is the single-writer plane: a delayed echo of
    /// this node's own uploaded log must not re-apply, or a newer local write
    /// can be reverted. A *different* `device_id` on that same empty prefix is
    /// multi-device pull and still applies. Org targets (non-empty prefix)
    /// always apply, including the author's own entries. Restore/bootstrap is
    /// a separate path and still replays self-authored entries.
    fn is_personal_self_echo(&self, target: &SyncTarget, entry_device_id: &str) -> bool {
        target.prefix.is_empty() && entry_device_id == self.device_id
    }

    pub(crate) async fn checkpoint_download_cursor_after_replay(
        &self,
        target: &SyncTarget,
        seq: u64,
    ) {
        let mut cursors = self.download_cursors.lock().await;
        cursors.insert(target.cursor_key(), seq);
        drop(cursors);
        self.save_download_cursor(&target.cursor_key(), seq).await;
    }

    pub(crate) async fn new_log_seqs_after_cursor(
        &self,
        target: &SyncTarget,
        cursor: u64,
    ) -> SyncResult<Vec<u64>> {
        if !Self::supports_personal_log_index(target) {
            return self
                .list_log_seqs_and_refresh_personal_index(target, cursor)
                .await;
        }

        if self.should_reconcile_personal_log_index() {
            tracing::debug!(
                target = %target.label,
                cursor,
                "periodically reconciling personal log index from the full object list"
            );
            return self
                .list_log_seqs_and_refresh_personal_index(target, cursor)
                .await;
        }

        match self.read_personal_log_index(target).await {
            Ok(Some(index)) => Ok(index.seqs_after(cursor)),
            Ok(None) => {
                tracing::info!(
                    target = %target.label,
                    cursor,
                    "personal log index missing; rebuilding from one full log list"
                );
                self.list_log_seqs_and_refresh_personal_index(target, cursor)
                    .await
            }
            // A DECRYPT failure of the index is the loudest wrong-key signal
            // there is: the index ciphertext exists but our current sync key
            // cannot open it. Do NOT swallow it and rebuild+overwrite the index
            // under the (wrong) local key — that is exactly how a drifted node
            // poisons the shared `log_index.enc`. Surface it as a structured
            // key-mismatch so the caller blocks uploads and arms the blocker.
            Err(SyncError::Crypto(reason)) => Err(SyncError::KeyProofFailed {
                target: target.label.clone(),
                reason: format!(
                    "personal log index did not decrypt with the current sync key: {reason}"
                ),
            }),
            Err(e) => {
                tracing::warn!(
                    target = %target.label,
                    cursor,
                    error = %e,
                    "failed to read personal log index (non-crypto); rebuilding from one full log list"
                );
                self.list_log_seqs_and_refresh_personal_index(target, cursor)
                    .await
            }
        }
    }
    /// Download entries from a target, refreshing auth once on 401.
    /// Download new entries from a sync target.
    ///
    /// Lists `/{prefix}/log/{seq}.enc`, downloads, unseals with the target's
    /// crypto, and replays.
    ///
    /// ### Why we use a personal log index (no S3 `start_after`)
    ///
    /// S3 `start_after` orders keys **lexicographically**, not numerically.
    /// Our keys use unpadded decimal seqs (`log/52.enc`, `log/100.enc`), so
    /// `log/100.enc` lex-sorts *before* `log/52.enc` (because `'1' < '5'`).
    /// Using the local numeric cursor as a lex `start_after` would silently
    /// hide every key whose seq crosses a digit-length boundary under the
    /// cursor's lex prefix — e.g., cursor=52 hides seqs 100..104 permanently.
    /// That is alpha BLOCKER 30a7b: Alice uploads 104 entries, Bob's second
    /// poll sees `start_after=log/52.enc` and misses seqs 100..104.
    ///
    /// Personal log seqs are also sparse timestamp values, so "fetch
    /// cursor+1..head" is not a usable range. Instead we maintain a compact,
    /// encrypted `snapshots/log_index.enc` containing the sorted log seqs.
    /// Steady-state download reads that single object, filters `seq > cursor`,
    /// and presigns only those exact keys. If the index is missing/corrupt
    /// (first run after deploy, manual repair, or old account), we do one full
    /// numeric list and rewrite the index so later idle cycles issue no
    /// full-prefix list.
    pub(crate) async fn download_entries(&self, target: &SyncTarget) -> SyncResult<u64> {
        let cursor = {
            let cursors = self.download_cursors.lock().await;
            cursors.get(&target.cursor_key()).copied().unwrap_or(0)
        };

        let mut new_seqs = self.new_log_seqs_after_cursor(target, cursor).await?;
        new_seqs.sort_unstable();

        ensure_contiguous_org_seqs(target, cursor, &new_seqs)?;

        if new_seqs.is_empty() {
            self.record_empty_download_cycle(target, cursor).await;
            return Ok(0);
        }

        // --- Memory caps (incident 2026-07-14) ---------------------------------
        // Five-whys short form:
        // 1. Kernel watchdog panic — swap exhausted, watchdogd starved.
        // 2. lastdbd footprint 0.5GB→74GB in ~2 min (powerstats).
        // 3. Stacked in download_entries / fetch+unseal during catch-up.
        // 4. One cycle tried to walk an unbounded post-cursor backlog with
        //    concurrent full-body GETs and no byte budget.
        // 5. "Concurrency for speed" assumed entry bodies were small and that
        //    buffered(n) kept memory O(n×avg_entry) with n small — false for
        //    embedding BatchPuts / multi-MB payloads / multi-hour catch-up.
        // Fix: truncate the per-cycle window, refuse poison-size objects, stop
        // when the byte budget is spent, and publish stats for lastdb status.
        let entries_listed = new_seqs.len();
        let entry_cap = self.config.max_download_entries_per_cycle;
        let mut truncated_by_entry_cap = false;
        if entry_cap > 0 && new_seqs.len() > entry_cap {
            new_seqs.truncate(entry_cap);
            truncated_by_entry_cap = true;
        }
        let entries_deferred_from_cap = entries_listed.saturating_sub(new_seqs.len());
        let byte_budget = self.config.max_download_bytes_per_cycle;

        tracing::info!(
            target: "fold_db::sync::memory",
            target_label = %target.label,
            cursor,
            entries_listed,
            entries_this_cycle = new_seqs.len(),
            entries_deferred = entries_deferred_from_cap,
            entry_cap,
            byte_budget,
            sync_concurrency = self.config.sync_concurrency,
            "download: starting cycle"
        );

        let mut progress = DownloadProgress {
            max_contiguous_seq: cursor,
            ..DownloadProgress::default()
        };
        let truncated_by_byte_cap = self
            .replay_download_chunks(target, &new_seqs, byte_budget, &mut progress)
            .await?;

        self.reload_after_download(target, &progress).await;

        let entries_deferred = entries_deferred_from_cap
            + if truncated_by_byte_cap {
                new_seqs
                    .iter()
                    .filter(|s| **s > progress.max_contiguous_seq)
                    .count()
            } else {
                0
            };
        let stats = progress.into_cycle_stats(
            target,
            entries_listed,
            entries_deferred,
            truncated_by_entry_cap,
            truncated_by_byte_cap,
        );
        let total_replayed = stats.entries_replayed;
        stats.log_cycle_complete();
        *self.last_download_stats.lock().await = Some(stats);

        Ok(total_replayed)
    }

    /// Publish the stats of a download cycle that found no entries past the cursor.
    async fn record_empty_download_cycle(&self, target: &SyncTarget, cursor: u64) {
        tracing::info!(
            "download '{}': 0 new entries (cursor={})",
            target.label,
            cursor
        );
        *self.last_download_stats.lock().await = Some(DownloadCycleStats {
            target: target.label.clone(),
            entries_listed: 0,
            ..DownloadCycleStats::default()
        });
    }

    /// Run the schema and embedding reloaders for the kinds this cycle replayed.
    async fn reload_after_download(&self, target: &SyncTarget, progress: &DownloadProgress) {
        if progress.schemas_replayed {
            self.invoke_reloader(&self.schema_reloader, "schema", &target.label)
                .await;
        }
        if progress.embeddings_replayed {
            self.invoke_reloader(&self.embedding_reloader, "embedding", &target.label)
                .await;
        }
    }

    /// Presign, fetch and replay `new_seqs` chunk by chunk, strictly in seq
    /// order. Returns true when the per-cycle byte budget cut the walk short.
    async fn replay_download_chunks(
        &self,
        target: &SyncTarget,
        new_seqs: &[u64],
        byte_budget: usize,
        progress: &mut DownloadProgress,
    ) -> SyncResult<bool> {
        let mut truncated_by_byte_cap = false;

        // Chunk the presign call at MAX_PRESIGN_BATCH — storage_service caps
        // `seq_numbers` per request and a fresh node can easily list >1000
        // new objects on first sync. Chunks are processed in seq order so the
        // contiguous-cursor invariant still holds: if chunk N fails partway,
        // chunks 1..N-1 fully replayed and `max_contiguous_seq` reflects that.
        'chunks: for chunk in new_seqs.chunks(MAX_PRESIGN_BATCH) {
            if truncated_by_byte_cap {
                break;
            }
            let urls = self.auth.presign_download(target, chunk).await?;
            if urls.len() != chunk.len() {
                return Err(SyncError::Auth(format!(
                    "presign_download '{}': expected {} urls, got {}",
                    target.label,
                    chunk.len(),
                    urls.len()
                )));
            }
            // Fetch + unseal the chunk's objects concurrently (network + crypto
            // are the per-entry bottleneck), but cap the in-flight count and
            // preserve seq order via `buffered` so REPLAY below stays strictly
            // sequential. Parallelising the fetch must not relax the
            // contiguous-cursor invariant: the replay loop consumes results in
            // seq order and aborts on the first failure, so the cursor never
            // advances past an unreplayed seq (see alpha BLOCKER 4439b).
            let cap = self.config.sync_concurrency.max(1);
            // Iterate OWNED `(seq, url)` pairs — `chunk.iter().copied()` yields
            // `u64` by value and `zip(urls)` consumes the Vec, so the map closure
            // takes no borrowed iterator item. (Borrowing the iterator item here
            // would trip the higher-ranked-lifetime bound when this future is
            // later spawned by the sync coordinator.)
            let mut fetch_stream = futures::stream::iter(chunk.iter().copied().zip(urls).map(
                |(seq, url)| async move {
                    self.fetch_and_unseal_entry_detailed("download", target, seq, url)
                        .await
                },
            ))
            .buffered(cap);

            // Replay strictly in seq order. `buffered` yields results in input
            // (seq) order, so a `?` here aborts the whole download at the first
            // failed seq — `max_contiguous_seq` reflects only entries replayed
            // before it, and each successful replay checkpoints the cursor
            // immediately.
            while let Some(res) = fetch_stream.next().await {
                let fetched = res?;
                if self.apply_fetched_entry(target, fetched, progress).await?
                    && byte_budget > 0
                    && progress.bytes_downloaded as usize >= byte_budget
                {
                    truncated_by_byte_cap = true;
                    tracing::info!(
                        target: "fold_db::sync::memory",
                        target_label = %target.label,
                        bytes_downloaded = progress.bytes_downloaded,
                        byte_budget,
                        cursor_now = progress.max_contiguous_seq,
                        "download: byte budget reached; deferring remainder to next cycle"
                    );
                    // Drop the remaining fetch stream — in-flight GETs cancel.
                    break 'chunks;
                }
            }
        }
        Ok(truncated_by_byte_cap)
    }
}
