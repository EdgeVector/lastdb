//! Cloud bootstrap (snapshot + ordered log replay).

use super::super::error::{SyncError, SyncResult};
use super::super::log::LogEntry;
use super::super::snapshot::Snapshot;
use super::*;
use crate::hex::sha256_hex;
use std::time::Instant;

impl SyncEngine {
    // =========================================================================

    /// Bootstrap a single sync target by index into `self.targets`.
    ///
    /// Loads this target's named photograph S from `latest` when present, then
    /// replays the log tail after F. Legacy `latest.enc` remains a read fallback.
    /// The personal
    /// target restores the full snapshot; non-personal targets restore only keys
    /// that belong to their prefix so org/share bootstrap does not clear
    /// unrelated local data.
    ///
    /// Returns a `BootstrapOutcome` describing what was replayed. If
    /// `latest.enc` does not exist (new prefix), returns an outcome with
    /// `last_seq = 0` and no entries replayed — not an error.
    ///
    /// This method does NOT invoke schema/embedding reloaders. Callers that
    /// need cache refresh should use `bootstrap_all` (or
    /// `bootstrap_targets_from`), which handle reloader dispatch.
    pub async fn bootstrap_target(&self, idx: usize) -> SyncResult<BootstrapOutcome> {
        let target = {
            let targets = self.targets.lock().await;
            if idx >= targets.len() {
                return Err(SyncError::Storage(format!(
                    "bootstrap_target: index {} out of range (have {} targets)",
                    idx,
                    targets.len()
                )));
            }
            targets[idx].clone()
        };

        tracing::info!(
            "bootstrapping target '{}' (idx={}, prefix='{}')",
            target.label,
            idx,
            target.prefix
        );

        let photograph_latest = match self.auth.photograph_latest_get_for_target(&target).await {
            Ok(response) => response.latest,
            Err(e) if photograph_latest_action_is_unsupported(&e) => {
                tracing::warn!(
                    target: "fold_db::sync::photograph",
                    target_label = %target.label,
                    error = %e,
                    "photograph latest read failed; using legacy latest.enc fallback"
                );
                None
            }
            Err(e) => return Err(e),
        };
        let photograph_frontier = photograph_latest
            .as_ref()
            .map(|latest| latest.frontier.clone());
        let mut snapshot_download_ms = 0u64;
        let mut snapshot_data = if let Some(latest) = photograph_latest.as_ref() {
            let snapshot_name = format!("{}.enc", latest.snapshot_id);
            let snapshot_url = self
                .auth
                .presign_snapshot_download_for_target(&target, &snapshot_name)
                .await?;
            let download_started = Instant::now();
            let data = self.s3.download(&snapshot_url).await?;
            snapshot_download_ms =
                snapshot_download_ms.saturating_add(elapsed_ms(download_started));
            let Some(data) = data else {
                return Err(SyncError::Storage(format!(
                    "latest names photograph {} but snapshots/{snapshot_name} is missing",
                    latest.snapshot_id
                )));
            };
            let actual = sha256_hex(&data);
            if actual != latest.snapshot_id {
                return Err(SyncError::Crypto(format!(
                    "photograph id mismatch: latest={} downloaded={actual}",
                    latest.snapshot_id
                )));
            }
            Some(data)
        } else {
            let snapshot_url = self
                .auth
                .presign_snapshot_download_for_target(&target, "latest.enc")
                .await?;
            let download_started = Instant::now();
            let data = self.s3.download(&snapshot_url).await?;
            snapshot_download_ms =
                snapshot_download_ms.saturating_add(elapsed_ms(download_started));
            data
        };
        let mut snapshot_present = snapshot_data.is_some();
        let mut log_objects = None;
        let mut legacy_personal_root = false;

        if !snapshot_present {
            let db_hash_log_objects = self.auth.list_log_objects(&target).await?;
            if idx == 0 && target.prefix.is_empty() && db_hash_log_objects.is_empty() {
                let legacy_snapshot_url = self
                    .auth
                    .presign_snapshot_download_legacy_personal("latest.enc")
                    .await?;
                let download_started = Instant::now();
                let legacy_snapshot_data = self.s3.download(&legacy_snapshot_url).await?;
                snapshot_download_ms =
                    snapshot_download_ms.saturating_add(elapsed_ms(download_started));
                let legacy_log_objects = self.auth.list_objects_legacy_personal("log/").await?;
                if legacy_snapshot_data.is_some() || !legacy_log_objects.is_empty() {
                    tracing::info!(
                        target: "fold_db::sync::bootstrap",
                        "db_hash root is empty; bootstrapping personal target from legacy principal root"
                    );
                    snapshot_data = legacy_snapshot_data;
                    snapshot_present = snapshot_data.is_some();
                    log_objects = Some(legacy_log_objects);
                    legacy_personal_root = true;
                } else {
                    log_objects = Some(db_hash_log_objects);
                }
            } else {
                log_objects = Some(db_hash_log_objects);
            }
        }

        let snapshot_last_seq = if let Some(data) = snapshot_data {
            let unseal_started = Instant::now();
            let snapshot = Snapshot::unseal(&data, &target.crypto).await?;
            let unseal_decode_ms = elapsed_ms(unseal_started);
            let last_seq = snapshot.last_seq;
            if let Some(frontier) = photograph_frontier.as_ref() {
                let named_through = frontier.max_through();
                if last_seq != named_through {
                    return Err(SyncError::Storage(format!(
                        "photograph frontier mismatch: snapshot last_seq={last_seq}, latest F={named_through}"
                    )));
                }
            }
            tracing::info!(
                "restoring snapshot for '{}': {} namespaces, last_seq={}",
                target.label,
                snapshot.namespaces.len(),
                last_seq
            );
            let restore_report = if target.prefix.is_empty() {
                // AtRestEnc checkpoints must land on the raw store so ENC:
                // envelopes are not double-sealed by EncryptingNamespacedStore.
                // Logical (historical) snapshots restore through the encrypting
                // seam so puts re-seal under local at-rest crypto.
                if snapshot.is_at_rest_enc() {
                    tracing::info!(
                        target: "fold_db::sync",
                        "bootstrap: restoring at_rest_enc snapshot into raw store"
                    );
                    snapshot.restore(self.cursor_store.as_ref()).await?
                } else {
                    snapshot.restore(self.store.as_ref()).await?
                }
            } else {
                let restore_scopes = self
                    .target_restore_scopes
                    .lock()
                    .await
                    .get(&target.prefix)
                    .cloned()
                    .unwrap_or_else(|| vec![target.prefix.clone()]);
                snapshot
                    .restore_scoped_to_prefixes(self.store.as_ref(), &restore_scopes)
                    .await?
            };
            if target.prefix.is_empty() {
                self.invoke_photograph_restore_barrier()
                    .await
                    .map_err(|error| {
                        SyncError::Storage(format!(
                            "refresh serving store after photograph restore: {error}"
                        ))
                    })?;
            }
            tracing::info!(
                target: "fold_db::sync::photograph",
                target_label = %target.label,
                snapshot_bytes = data.len(),
                namespaces = restore_report.namespaces,
                entries = restore_report.entries,
                batches = restore_report.batches,
                download_ms = snapshot_download_ms,
                unseal_decode_ms,
                materialize_ms = restore_report.materialize_ms,
                final_flush_ms = restore_report.final_flush_ms,
                restore_total_ms = restore_report.total_ms(),
                ingress_total_ms = snapshot_download_ms.saturating_add(unseal_decode_ms),
                "photograph restore phases complete"
            );
            if target.prefix.is_empty() {
                let pack_id = format!("{last_seq}.enc");
                match self
                    .warm_thumb_cache_for_snapshot(&snapshot, &pack_id)
                    .await
                {
                    Ok(0) => {}
                    Ok(count) => tracing::info!(
                        target: "fold_db::sync",
                        pack_id = %pack_id,
                        thumbnails = count,
                        "bootstrap warmed thumbnail cache"
                    ),
                    Err(e) => tracing::warn!(
                        target: "fold_db::sync",
                        pack_id = %pack_id,
                        error = %e,
                        "bootstrap thumbnail cache warm failed (non-fatal)"
                    ),
                }
            }
            last_seq
        } else {
            0
        };

        let log_objects = match log_objects {
            Some(log_objects) => log_objects,
            None => self.auth.list_log_objects(&target).await?,
        };

        let mut log_seqs: Vec<u64> = log_objects
            .iter()
            .filter_map(|obj| {
                let (writer, seq) = parse_mutation_log_object_key(&obj.key)?;
                // A named photograph uses the writer-scoped mutation-log
                // reader below. Keep this loop for legacy flat objects only.
                if photograph_frontier.is_some() && writer.is_some() {
                    None
                } else {
                    Some(seq)
                }
            })
            .filter(|seq| *seq > snapshot_last_seq)
            .collect();
        log_seqs.sort();

        // GUARD: a missing snapshot on a target whose log prefix already holds
        // history would replay only the tail and restore a silently hollow
        // store (the "missing snapshot" migration trap). Fail loudly with a
        // structured error naming the gap unless the caller opted into a fresh
        // start. A brand-new account (no snapshot, empty log prefix) passes
        // through untouched. Guard on the full listing count (not the
        // post-snapshot filter) so history is detected even at seq boundaries;
        // when the snapshot is absent `snapshot_last_seq` is 0, so `log_seqs`
        // already spans the whole prefix.
        guard_missing_snapshot(
            &target.label,
            snapshot_present,
            log_seqs.len(),
            self.config.accept_fresh_bootstrap,
        )?;

        if !snapshot_present {
            if log_seqs.is_empty() {
                tracing::info!(
                    "no snapshot and no log history for '{}' — bootstrapping a fresh empty store",
                    target.label
                );
            } else {
                // Reachable only with accept_fresh set (the guard errors
                // otherwise): the operator explicitly chose a fresh start.
                tracing::warn!(
                    "no snapshot found for '{}' but accept_fresh is set — starting fresh despite {} existing log entr{} in the cloud prefix",
                    target.label,
                    log_seqs.len(),
                    if log_seqs.len() == 1 { "y" } else { "ies" }
                );
            }
        }

        let mut schemas_replayed = false;
        let mut embeddings_replayed = false;
        let mut entries_replayed: usize = 0;
        let mut photograph_tail_last_seq = snapshot_last_seq;

        if let Some(frontier) = photograph_frontier.as_ref() {
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
                .restore_mutation_log_after_photograph(&target, frontier)
                .await?;
            let tail_replay_ms = elapsed_ms(tail_replay_started);
            entries_replayed = entries_replayed.saturating_add(report.records_applied);
            photograph_tail_last_seq = report
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
                schemas_replayed = true;
                embeddings_replayed = true;
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
        }

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
                    self.auth.presign_download(&target, chunk).await?
                };
                if urls.len() != chunk.len() {
                    return Err(SyncError::Auth(format!(
                        "presign_download '{}': expected {} urls, got {}",
                        target.label,
                        chunk.len(),
                        urls.len()
                    )));
                }

                // Iterate OWNED `(seq, url)` pairs so the fetch closure borrows
                // nothing from the iterator (mirrors download_entries — avoids
                // the higher-ranked-lifetime bound when buffered). FETCH
                // (download + unseal) runs up to `cap` concurrently;
                // `ordered_concurrent_fetch` re-orders results into seq order
                // and REPLAY (consume) runs strictly sequentially, so the
                // contiguous-cursor invariant holds (`last_seq`, advanced after
                // the loop, never moves past a failed seq because any error
                // aborts the whole bootstrap).
                let target = &target;
                let schemas_seen = &mut schemas_replayed;
                let embeddings_seen = &mut embeddings_replayed;
                let items = chunk.iter().copied().zip(urls);
                ordered_concurrent_fetch(
                    items,
                    cap,
                    |(seq, url)| async move {
                        // Per-object size cap applies to bootstrap too so a
                        // poison multi-GB log object cannot pin process RSS.
                        self.fetch_and_unseal_entry_detailed("bootstrap download", target, seq, url)
                            .await
                            .map(|f| {
                                (
                                    f.seq,
                                    f.entry,
                                    f.mutation_log_records,
                                    f.skipped_oversize,
                                )
                            })
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
                        if let Some(records) = &mutation_log_records {
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
                        } else if let Some(entry) = &entry {
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

                // The helper aborts on the first replay error (returning Err
                // above), so reaching here means every seq in this chunk
                // replayed contiguously — count them all.
                entries_replayed += chunk.len();
            }
        }

        // Bootstrap replayed every listed seq contiguously (we return Err on
        // any failure), so `last_seq` is safe to advance to the max listed.
        let last_seq = log_seqs
            .last()
            .copied()
            .unwrap_or(snapshot_last_seq)
            .max(photograph_tail_last_seq);

        // Advance the local sequence counter only from the personal target.
        // Org targets write to their own R2 prefix and must not rewind the
        // personal counter used for upload sequencing.
        if idx == 0 {
            *self.seq.lock().await = last_seq;
        }

        // Always seed this target's download cursor to the bootstrap head,
        // including the snapshot-only path where last_seq == snapshot_last_seq
        // and no log tail was replayed. Compaction bounds stamp/delete by
        // min(requested, cursor); leaving cursor at the default 0 after a
        // successful restore of snapshot N would let compact(N) stamp
        // latest.enc at last_seq=0 and overwrite a good checkpoint with a
        // hollow one (see teardown-sync-bootstrap-cursor-not-seeded-snapshot-only).
        // last_seq is already max(snapshot_last_seq, max(replayed_tail)).
        {
            let mut cursors = self.download_cursors.lock().await;
            cursors.insert(target.prefix.clone(), last_seq);
        }
        self.save_download_cursor(&target.prefix, last_seq).await;

        let org_skips = self.org_scoped_replay_skips();
        if org_skips > 0 {
            tracing::warn!(
                target: "fold_db::sync::bootstrap",
                target = %target.label,
                org_scoped_keys_skipped = org_skips,
                "bootstrap of '{}' complete at seq {} ({} entries replayed); \
                 SKIPPED {org_skips} org-scoped storage key(s) (consented drop after org-crypto strip)",
                target.label,
                last_seq,
                entries_replayed
            );
        } else {
            tracing::info!(
                "bootstrap of '{}' complete at seq {} ({} entries replayed)",
                target.label,
                last_seq,
                entries_replayed
            );
        }

        Ok(BootstrapOutcome {
            last_seq,
            entries_replayed,
            schemas_replayed,
            embeddings_replayed,
        })
    }

    /// Bootstrap all configured sync targets (personal + orgs).
    ///
    /// Restores personal first, then restores non-personal targets with bounded
    /// concurrency. Fails fast: if any target errors, aborts and returns `Err`
    /// with context identifying which target failed. Partial success is not
    /// useful in the restore case.
    ///
    /// After all targets succeed, invokes the schema reloader ONCE if any
    /// outcome reported schema replays, and the embedding reloader ONCE if
    /// any outcome reported embedding replays. This avoids redundant
    /// SchemaCore/EmbeddingIndex refreshes when many targets restore in
    /// sequence.
    ///
    /// Callers are responsible for configuring org targets (via
    /// `configure_targets`) before invoking this method.
    pub async fn bootstrap_all(&self) -> SyncResult<Vec<BootstrapOutcome>> {
        // Snapshot target count and release the lock before iterating so
        // per-target calls can reacquire it.
        let target_count = self.targets.lock().await.len();
        tracing::info!("bootstrap_all: starting restore of {target_count} target(s)");

        let mut outcomes: Vec<BootstrapOutcome> = Vec::with_capacity(target_count);
        if target_count > 0 {
            outcomes.push(self.bootstrap_target(0).await.map_err(|e| {
                SyncError::Storage(format!("bootstrap_all: target idx=0 failed: {e}"))
            })?);
        }
        outcomes.extend(self.bootstrap_targets_from_inner(1).await?);

        self.reload_after_bootstrap_outcomes(&outcomes, "bootstrap_all")
            .await;

        tracing::info!(
            "bootstrap_all: completed {} target(s) successfully",
            outcomes.len()
        );
        Ok(outcomes)
    }

    /// Bootstrap targets starting at `start_idx`, bounded-concurrent.
    ///
    /// This is used by restore flows that already bootstrapped personal data
    /// once, then discover org memberships from the restored store and only need
    /// to fetch non-personal targets in phase 2.
    pub async fn bootstrap_targets_from(
        &self,
        start_idx: usize,
    ) -> SyncResult<Vec<BootstrapOutcome>> {
        let outcomes = self.bootstrap_targets_from_inner(start_idx).await?;
        self.reload_after_bootstrap_outcomes(&outcomes, "bootstrap_targets_from")
            .await;
        Ok(outcomes)
    }

    /// Bootstrap one configured target after a new grant or registration.
    pub async fn bootstrap_target_by_prefix(
        &self,
        prefix: &str,
    ) -> SyncResult<Option<BootstrapOutcome>> {
        let idx = self
            .targets
            .lock()
            .await
            .iter()
            .position(|target| target.prefix == prefix);
        let Some(idx) = idx else {
            return Ok(None);
        };
        let outcome = self.bootstrap_target(idx).await?;
        self.reload_after_bootstrap_outcomes(std::slice::from_ref(&outcome), "bootstrap_target")
            .await;
        Ok(Some(outcome))
    }

    pub(crate) async fn bootstrap_targets_from_inner(
        &self,
        start_idx: usize,
    ) -> SyncResult<Vec<BootstrapOutcome>> {
        let target_count = self.targets.lock().await.len();
        if start_idx >= target_count {
            return Ok(Vec::new());
        }

        let cap = self.config.bootstrap_target_concurrency.max(1);
        let mut results = Vec::with_capacity(target_count - start_idx);
        let mut stream = futures::stream::iter(start_idx..target_count)
            .map(|idx| async move {
                self.bootstrap_target(idx)
                    .await
                    .map(|outcome| (idx, outcome))
                    .map_err(|e| {
                        SyncError::Storage(format!(
                            "bootstrap_targets_from: target idx={idx} failed: {e}"
                        ))
                    })
            })
            .buffer_unordered(cap);

        while let Some(result) = stream.next().await {
            results.push(result?);
        }
        results.sort_by_key(|(idx, _)| *idx);
        Ok(results.into_iter().map(|(_, outcome)| outcome).collect())
    }

    pub(crate) async fn reload_after_bootstrap_outcomes(
        &self,
        outcomes: &[BootstrapOutcome],
        context: &str,
    ) {
        if outcomes.iter().any(|o| o.schemas_replayed) {
            self.invoke_reloader(&self.schema_reloader, "schema", context)
                .await;
        }
        if outcomes.iter().any(|o| o.embeddings_replayed) {
            self.invoke_reloader(&self.embedding_reloader, "embedding", context)
                .await;
        }
    }
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

pub(super) fn photograph_latest_action_is_unsupported(error: &SyncError) -> bool {
    let message = error.to_string();
    message.contains("Unknown presign action: photograph_latest_get")
        || message.contains("unexpected action photograph_latest_get")
}
