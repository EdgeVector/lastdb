//! Cloud bootstrap (snapshot + ordered log replay).

use super::super::error::{SyncError, SyncResult};
use super::super::log::LogEntry;
use super::super::snapshot::Snapshot;
use super::*;
use crate::hex::sha256_hex;
use std::time::Instant;

mod replay;
mod snapshot;
use replay::BootstrapReplayState;
use snapshot::BootstrapSnapshotFetch;

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

        if target.read_binding.is_some() {
            return self.bootstrap_foreign_share(&target).await;
        }

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
        let BootstrapSnapshotFetch {
            snapshot_data,
            snapshot_download_ms,
            snapshot_present,
            log_objects,
            legacy_personal_root,
        } = self
            .fetch_bootstrap_snapshot(
                idx,
                &target,
                photograph_latest
                    .as_ref()
                    .map(|latest| latest.snapshot_id.as_str()),
            )
            .await?;

        let snapshot_last_seq = match snapshot_data {
            Some(data) => {
                self.restore_bootstrap_snapshot(
                    &target,
                    &data,
                    photograph_frontier.as_ref(),
                    snapshot_download_ms,
                )
                .await?
            }
            None => 0,
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

        let mut state = BootstrapReplayState {
            schemas_replayed: false,
            embeddings_replayed: false,
            entries_replayed: 0,
            photograph_tail_last_seq: snapshot_last_seq,
        };

        if let Some(frontier) = photograph_frontier.as_ref() {
            self.replay_photograph_tail(
                &target,
                frontier,
                snapshot_present,
                snapshot_last_seq,
                &mut state,
            )
            .await?;
        }

        self.replay_bootstrap_log_tail(&target, &log_seqs, legacy_personal_root, &mut state)
            .await?;

        // Bootstrap replayed every listed seq contiguously (we return Err on
        // any failure), so `last_seq` is safe to advance to the max listed.
        let last_seq = log_seqs
            .last()
            .copied()
            .unwrap_or(snapshot_last_seq)
            .max(state.photograph_tail_last_seq);

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
            cursors.insert(target.cursor_key(), last_seq);
        }
        self.save_download_cursor(&target.cursor_key(), last_seq)
            .await;

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
                state.entries_replayed
            );
        } else {
            tracing::info!(
                "bootstrap of '{}' complete at seq {} ({} entries replayed)",
                target.label,
                last_seq,
                state.entries_replayed
            );
        }

        Ok(BootstrapOutcome {
            last_seq,
            entries_replayed: state.entries_replayed,
            schemas_replayed: state.schemas_replayed,
            embeddings_replayed: state.embeddings_replayed,
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
