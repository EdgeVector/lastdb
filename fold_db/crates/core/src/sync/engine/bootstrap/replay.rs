//! Replay leg of a target bootstrap: photograph tail and flat log tail.

use super::*;
use crate::sync::s3::PresignedUrl;
use crate::sync::snapshot_log::Frontier;

/// Counters that the photograph tail and the flat log replay both advance.
pub(super) struct BootstrapReplayState {
    pub(super) schemas_replayed: bool,
    pub(super) embeddings_replayed: bool,
    pub(super) entries_replayed: usize,
    pub(super) photograph_tail_last_seq: u64,
}

impl SyncEngine {
    /// Reload schemas, then replay the writer log tail after a named photograph.
    pub(super) async fn replay_photograph_tail(
        &self,
        target: &SyncTarget,
        frontier: &Frontier,
        snapshot_present: bool,
        snapshot_last_seq: u64,
        state: &mut BootstrapReplayState,
    ) -> SyncResult<()> {
        // S can contain schema rows required by a MutationIntent in the
        // writer tail. Reload SchemaCore after S lands and before replay;
        // bootstrap_all's final reload happens too late for that apply.
        let schema_reload_started = Instant::now();
        if snapshot_present {
            self.invoke_reloader(
                &self.schema_reloader,
                "schema",
                "named photograph before writer tail",
            )
            .await;
        }
        let schema_reload_ms = elapsed_ms(schema_reload_started);
        let tail_replay_started = Instant::now();
        let report = self
            .restore_mutation_log_after_photograph(target, frontier)
            .await?;
        let tail_replay_ms = elapsed_ms(tail_replay_started);
        state.entries_replayed = state
            .entries_replayed
            .saturating_add(report.records_applied);
        state.photograph_tail_last_seq = report
            .frontier_after
            .values()
            .copied()
            .max()
            .unwrap_or(snapshot_last_seq)
            .max(snapshot_last_seq);
        if report.records_applied > 0 {
            // Mutation intents can touch schemas or native indexes.
            // Reload both caches after a photograph tail rather than
            // infer namespaces from the aggregate replay report.
            state.schemas_replayed = true;
            state.embeddings_replayed = true;
        }
        tracing::info!(
            target: "fold_db::sync::photograph",
            target_label = %target.label,
            snapshot_frontier = frontier.max_through(),
            segments_applied = report.segments_applied,
            records_applied = report.records_applied,
            schema_reload_ms,
            tail_replay_ms,
            "loaded named photograph and applied writer log tail"
        );
        Ok(())
    }

    /// Replay the flat log tail strictly in seq order.
    pub(super) async fn replay_bootstrap_log_tail(
        &self,
        target: &SyncTarget,
        log_seqs: &[u64],
        legacy_personal_root: bool,
        state: &mut BootstrapReplayState,
    ) -> SyncResult<()> {
        if !log_seqs.is_empty() {
            tracing::info!(
                "replaying {} log entries for '{}' (seq {}..={})",
                log_seqs.len(),
                target.label,
                log_seqs[0],
                log_seqs[log_seqs.len() - 1]
            );

            // Bootstrap may replay the entire log tail — hundreds of thousands
            // of entries on a long-lived device. Done serially (one
            // download→unseal→replay per iteration) the network legs dominate:
            // a ~1 GiB log takes hours. Fan the download+unseal out with a
            // bounded-concurrency pipeline (`bootstrap_concurrency`, default
            // higher than the steady-state `sync_concurrency` since this is a
            // one-time large fetch), while keeping REPLAY strictly sequential
            // in seq order. `buffered(cap)` yields decrypted entries in input
            // (seq) order and caps the in-flight set, so memory stays bounded
            // and the contiguous-cursor invariant holds: a `?` in the replay
            // loop aborts at the first failed seq and `last_seq` only advances
            // because we return `Err` on any failure (see download_entries).
            let cap = self.config.bootstrap_concurrency.max(1);

            // Chunk the presign call at MAX_PRESIGN_BATCH (storage_service caps
            // `seq_numbers` per request and a bootstrap can list far more than
            // that). Chunks are processed in seq order, so the contiguous
            // replay invariant holds across chunk boundaries.
            for chunk in log_seqs.chunks(MAX_PRESIGN_BATCH) {
                let urls = if legacy_personal_root {
                    self.auth.presign_download_legacy_personal(chunk).await?
                } else {
                    self.auth.presign_download(target, chunk).await?
                };
                if urls.len() != chunk.len() {
                    return Err(SyncError::Auth(format!(
                        "presign_download '{}': expected {} urls, got {}",
                        target.label,
                        chunk.len(),
                        urls.len()
                    )));
                }

                self.fetch_and_replay_log_chunk(target, chunk, urls, cap, state)
                    .await?;

                // The helper aborts on the first replay error (returning Err
                // above), so reaching here means every seq in this chunk
                // replayed contiguously — count them all.
                state.entries_replayed += chunk.len();
            }
        }

        Ok(())
    }

    /// Fetch one presigned chunk concurrently and replay it in seq order.
    async fn fetch_and_replay_log_chunk(
        &self,
        target: &SyncTarget,
        chunk: &[u64],
        urls: Vec<PresignedUrl>,
        cap: usize,
        state: &mut BootstrapReplayState,
    ) -> SyncResult<()> {
        // Iterate OWNED `(seq, url)` pairs so the fetch closure borrows
        // nothing from the iterator (mirrors download_entries — avoids
        // the higher-ranked-lifetime bound when buffered). FETCH
        // (download + unseal) runs up to `cap` concurrently;
        // `ordered_concurrent_fetch` re-orders results into seq order
        // and REPLAY (consume) runs strictly sequentially, so the
        // contiguous-cursor invariant holds (`last_seq`, advanced after
        // the loop, never moves past a failed seq because any error
        // aborts the whole bootstrap).
        let schemas_seen = &mut state.schemas_replayed;
        let embeddings_seen = &mut state.embeddings_replayed;
        let items = chunk.iter().copied().zip(urls);
        ordered_concurrent_fetch(
            items,
            cap,
            |(seq, url)| async move {
                // Per-object size cap applies to bootstrap too so a
                // poison multi-GB log object cannot pin process RSS.
                self.fetch_and_unseal_entry_detailed("bootstrap download", target, seq, url)
                    .await
                    .map(|f| (f.seq, f.entry, f.mutation_log_records, f.skipped_oversize))
            },
            |(seq, entry, mutation_log_records, skipped_oversize): (
                u64,
                Option<LogEntry>,
                Option<Vec<crate::sync::engine::pin_log::PinLogRecord>>,
                bool,
            )| {
                if skipped_oversize {
                    tracing::warn!(
                        target: "fold_db::sync::memory",
                        target_label = %target.label,
                        seq,
                        "bootstrap: skipped oversize log object"
                    );
                }
                // Replay strictly in seq order (consume is awaited
                // sequentially). `schema_states` shares the schema
                // reloader: see `download_entries` for rationale.
                // Mutation-log segments on the flat key must be applied
                // before bootstrap treats the seq as handled.
                note_replayed_entries(
                    target,
                    seq,
                    mutation_log_records.as_deref(),
                    entry.as_ref(),
                    schemas_seen,
                    embeddings_seen,
                );
                if mutation_log_records.is_some() || entry.is_some() {
                    futures::future::Either::Left(async move {
                        if let Some(records) = mutation_log_records {
                            for record in records {
                                self.replay_entry(&record.entry, Some(target)).await?;
                            }
                            Ok(())
                        } else if let Some(entry) = entry {
                            self.replay_entry(&entry, Some(target)).await
                        } else {
                            Ok(())
                        }
                    })
                } else {
                    futures::future::Either::Right(futures::future::ready(Ok(())))
                }
            },
        )
        .await?;
        Ok(())
    }
}

/// Record which caches a replayed entry dirties and log it.
fn note_replayed_entries(
    target: &SyncTarget,
    seq: u64,
    mutation_log_records: Option<&[crate::sync::engine::pin_log::PinLogRecord]>,
    entry: Option<&LogEntry>,
    schemas_seen: &mut bool,
    embeddings_seen: &mut bool,
) {
    if let Some(records) = mutation_log_records {
        for record in records {
            match record.entry.op.namespace() {
                "schemas" | "schema_states" => *schemas_seen = true,
                "native_index" => *embeddings_seen = true,
                _ => {}
            }
            tracing::info!(
                "bootstrap mutation-log replay '{}' seq={} frontier_after={}: {}",
                target.label,
                seq,
                record.frontier_after,
                record.entry.op.describe()
            );
        }
    } else if let Some(entry) = entry {
        match entry.op.namespace() {
            "schemas" | "schema_states" => *schemas_seen = true,
            "native_index" => *embeddings_seen = true,
            _ => {}
        }
        tracing::info!(
            "bootstrap replay '{}' seq={}: {}",
            target.label,
            seq,
            entry.op.describe()
        );
    }
}
