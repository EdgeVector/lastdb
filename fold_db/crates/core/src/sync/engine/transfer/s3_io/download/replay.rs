//! Per-entry replay and bookkeeping for one download cycle.

use super::super::fetch::FetchedEntry;
use super::*;
use crate::sync::engine::pin_log::PinLogRecord;
use crate::sync::log::LogEntry;

/// Running totals for one `download_entries` cycle.
#[derive(Default)]
pub(super) struct DownloadProgress {
    pub total_replayed: u64,
    /// Cursor advances contiguously: if any seq in a batch fails we stop
    /// before it so the next cycle re-downloads it. Silent drops (where the
    /// cursor skips past a failed entry) caused alpha BLOCKER 4439b.
    pub max_contiguous_seq: u64,
    pub schemas_replayed: bool,
    pub embeddings_replayed: bool,
    pub entries_skipped_self: u64,
    pub bytes_downloaded: u64,
    pub entries_skipped_oversize: u64,
    pub entries_attempted: usize,
}

/// Loud-failure invariant: for ORG prefixes, the set of new seqs must
/// be contiguous (no holes between the smallest and largest) because
/// org seqs are server-allocated in atomic blocks. A hole there means
/// S3 returned a partial view of the log — the exact silent-drop
/// pattern 30a7b produced — and would cause `max_contiguous_seq` to
/// advance past a seq that later appears.
///
/// PERSONAL prefixes (empty prefix) use client-assigned nanosecond
/// timestamps, so the seq space is inherently sparse: a device writes
/// at T1, then again at T1+100000, and the listing has "gaps" of
/// hundreds of thousands of unused seq values between them. The
/// contiguity check can't distinguish natural sparseness from S3
/// dropping a seq, so it's skipped for personal and we trust the
/// listing's completeness (typical path) plus cursor-replay semantics
/// (missed seqs re-appear in future listings because cursor only
/// advances to seqs we saw). See `designs/unified_sync.md` for the
/// personal-vs-org split rationale.
pub(super) fn ensure_contiguous_org_seqs(
    target: &SyncTarget,
    cursor: u64,
    new_seqs: &[u64],
) -> SyncResult<()> {
    if target.prefix.is_empty() {
        return Ok(());
    }
    let (Some(&first), Some(&last)) = (new_seqs.first(), new_seqs.last()) else {
        return Ok(());
    };
    let expected = (last - first + 1) as usize;
    if new_seqs.len() == expected {
        return Ok(());
    }
    let set: std::collections::BTreeSet<u64> = new_seqs.iter().copied().collect();
    let missing: Vec<u64> = (first..=last).filter(|s| !set.contains(s)).collect();
    tracing::error!(
        "sync list '{}' returned non-contiguous seqs after cursor={}: first={} last={} count={} expected={} missing_sample={:?}",
        target.label,
        cursor,
        first,
        last,
        new_seqs.len(),
        expected,
        missing.iter().take(16).collect::<Vec<_>>()
    );
    Err(SyncError::S3(format!(
        "non-contiguous log listing for '{}': cursor={} got {} seqs in range {}..={} (expected {})",
        target.label,
        cursor,
        new_seqs.len(),
        first,
        last,
        expected
    )))
}

impl SyncEngine {
    /// Replay one fetched object and advance the cursor.
    ///
    /// Returns `false` when the caller must skip its byte-budget check for
    /// this object (oversize skip or self-authored echo).
    pub(super) async fn apply_fetched_entry(
        &self,
        target: &SyncTarget,
        fetched: FetchedEntry,
        progress: &mut DownloadProgress,
    ) -> SyncResult<bool> {
        progress.entries_attempted += 1;
        progress.bytes_downloaded = progress
            .bytes_downloaded
            .saturating_add(fetched.ciphertext_bytes);
        if fetched.skipped_oversize {
            progress.entries_skipped_oversize += 1;
            self.advance_cursor(target, fetched.seq, progress).await;
            return Ok(false);
        }
        // Continuous mutation-log segments land on the legacy flat
        // `log/{seq}.enc` key. Apply every unsealed PinLogRecord
        // entry before claiming cursor progress — never advance past
        // unapplied mutation-log data.
        if let Some(records) = fetched.mutation_log_records {
            self.apply_mutation_log_records(target, fetched.seq, records, progress)
                .await?;
            self.advance_cursor(target, fetched.seq, progress).await;
        } else if let Some(entry) = fetched.entry {
            if self.is_personal_self_echo(target, &entry.device_id) {
                progress.entries_skipped_self += 1;
                tracing::debug!(
                    "skip self-authored log entry '{}' seq={}",
                    target.label,
                    fetched.seq,
                );
                self.advance_cursor(target, fetched.seq, progress).await;
                return Ok(false);
            }
            tracing::info!(
                "replay '{}' seq={}: {}",
                target.label,
                fetched.seq,
                entry.op.describe()
            );
            self.replay_and_pin(target, fetched.seq, &entry, "apply failed", progress)
                .await?;
            self.advance_cursor(target, fetched.seq, progress).await;
        } else {
            // Quarantine / empty-skip paths: no payload to apply.
            self.advance_cursor(target, fetched.seq, progress).await;
        }
        Ok(true)
    }

    async fn apply_mutation_log_records(
        &self,
        target: &SyncTarget,
        seq: u64,
        records: Vec<PinLogRecord>,
        progress: &mut DownloadProgress,
    ) -> SyncResult<()> {
        for record in records {
            let entry = &record.entry;
            if self.is_personal_self_echo(target, &entry.device_id) {
                progress.entries_skipped_self += 1;
                tracing::debug!(
                    "skip self-authored mutation-log entry '{}' seq={} frontier_after={}",
                    target.label,
                    seq,
                    record.frontier_after,
                );
                continue;
            }
            tracing::info!(
                "replay mutation-log '{}' seq={} frontier_after={}: {}",
                target.label,
                seq,
                record.frontier_after,
                entry.op.describe()
            );
            self.replay_and_pin(target, seq, entry, "mutation-log apply failed", progress)
                .await?;
        }
        Ok(())
    }

    /// Replay one entry; on failure attach the pin location so the structured
    /// `replay_blocker` is populated.
    async fn replay_and_pin(
        &self,
        target: &SyncTarget,
        seq: u64,
        entry: &LogEntry,
        failure_label: &str,
        progress: &mut DownloadProgress,
    ) -> SyncResult<()> {
        // `schema_states` shares the schema reloader: replaying a pure
        // state flip on an org schema (approve/block) must still refresh
        // the in-memory state cache, otherwise `/api/schemas` lags the
        // on-disk truth until the next schemas-namespace write lands.
        match entry.op.namespace() {
            "schemas" | "schema_states" => progress.schemas_replayed = true,
            "native_index" => progress.embeddings_replayed = true,
            _ => {}
        }
        let cursor_floor = progress.max_contiguous_seq;
        self.replay_entry(entry, Some(target)).await.map_err(|e| {
            tracing::error!(
                "sync replay aborted: {} in '{}' seq={}: {}; cursor will NOT advance past seq={}",
                failure_label,
                target.label,
                seq,
                e,
                cursor_floor
            );
            Self::pin_replay_error(&target.label, seq, e)
                .with_replay_operation(entry.seq, (&entry.op).into())
        })?;
        progress.total_replayed += 1;
        Ok(())
    }

    async fn advance_cursor(&self, target: &SyncTarget, seq: u64, progress: &mut DownloadProgress) {
        progress.max_contiguous_seq = seq;
        self.checkpoint_download_cursor_after_replay(target, seq)
            .await;
    }
}

impl DownloadProgress {
    pub(super) fn into_cycle_stats(
        self,
        target: &SyncTarget,
        entries_listed: usize,
        entries_deferred: usize,
        truncated_by_entry_cap: bool,
        truncated_by_byte_cap: bool,
    ) -> DownloadCycleStats {
        let entries_health_deferred = if target.prefix.is_empty()
            && self.entries_skipped_self > 0
            && self.total_replayed == 0
        {
            0
        } else {
            entries_deferred
        };
        DownloadCycleStats {
            target: target.label.clone(),
            entries_listed,
            entries_attempted: self.entries_attempted,
            entries_replayed: self.total_replayed,
            entries_skipped_self: self.entries_skipped_self,
            bytes_downloaded: self.bytes_downloaded,
            entries_skipped_oversize: self.entries_skipped_oversize,
            entries_deferred,
            entries_health_deferred,
            truncated_by_entry_cap,
            truncated_by_byte_cap,
        }
    }
}

impl DownloadCycleStats {
    pub(super) fn log_cycle_complete(&self) {
        tracing::info!(
            target: "fold_db::sync::memory",
            target_label = %self.target,
            entries_listed = self.entries_listed,
            entries_attempted = self.entries_attempted,
            entries_replayed = self.entries_replayed,
            entries_skipped_self = self.entries_skipped_self,
            bytes_downloaded = self.bytes_downloaded,
            entries_skipped_oversize = self.entries_skipped_oversize,
            entries_deferred = self.entries_deferred,
            entries_health_deferred = self.entries_health_deferred,
            truncated_by_entry_cap = self.truncated_by_entry_cap,
            truncated_by_byte_cap = self.truncated_by_byte_cap,
            "download: cycle complete"
        );
    }
}
