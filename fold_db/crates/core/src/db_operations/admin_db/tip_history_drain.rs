//! Tip-history drain and its durable checkpoint.

use super::*;

impl AtomStore {
    /// One bounded, resumable pass that drains existing `tv:` tip-version
    /// chains on live heads without collection compaction.
    ///
    /// New writes are history-free by default (opt-in tip history). Legacy
    /// chains still pin body atoms until something clears them. Full-store
    /// `gc-atoms --prune-live-history` does that, but it is an unbounded
    /// operator verb and pairs poorly with cloud-backup re-staging after a
    /// collection compact. This path reuses [`Self::apply_tip_chain_prune`]'s
    /// CAS/revalidation guard and pages through `mk:` so each call is bounded
    /// and an interrupted pass resumes from `next_after_key`.
    ///
    /// Does **not** delete orphan body atoms or compact any collection —
    /// only clears `prev_tip_id` and deletes the `tv:` nodes for heads that
    /// still match the planning observation.
    // lint:fn-size-ok verbatim move from admin_db.rs; splitting this function is separate work
    pub async fn drain_tip_history_chains(
        &self,
        options: TipHistoryDrainOptions,
    ) -> Result<TipHistoryDrainReport, SchemaError> {
        use crate::atom::molecule_key_codec;
        use crate::db_operations::atom_store::PerKeyRecord;

        const SCAN_PAGE: usize = 512;
        const PASS_TIME_BUDGET: std::time::Duration = std::time::Duration::from_secs(120);

        let max_keys = options.max_keys.max(1);
        let max_prunes = options.max_prunes.unwrap_or(max_keys).max(1);
        let dry_run = options.dry_run;
        let storage_prefix = options.storage_prefix.as_deref();
        let pass_started = std::time::Instant::now();
        let pass_started_at = Utc::now().to_rfc3339();

        let mk_prefix = build_storage_key(storage_prefix, "mk:");
        let mk_end = FilterUtils::create_prefix_end(&mk_prefix);
        let mut report = TipHistoryDrainReport {
            dry_run,
            pass_started_at,
            ..Default::default()
        };

        let mut page_start = options
            .after_key
            .clone()
            .unwrap_or_else(|| mk_prefix.clone());
        let mut skip_head = options.after_key.clone();
        let mut range_exhausted = false;

        while !range_exhausted {
            let rows = self
                .raw()
                .inner()
                .scan_range_paged(page_start.as_bytes(), mk_end.as_bytes(), SCAN_PAGE)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("tip-history drain scan mk: {e}")))?;
            if rows.len() < SCAN_PAGE {
                range_exhausted = true;
            }
            if rows.is_empty() {
                break;
            }

            let head = skip_head.take();
            let mut stop_after_page = false;
            for (raw_key, raw_value) in rows {
                let mk_key = String::from_utf8_lossy(&raw_key).into_owned();
                page_start = mk_key.clone();
                if head.as_deref() == Some(mk_key.as_str()) {
                    continue;
                }

                if report.keys_scanned as usize >= max_keys
                    || report.tips_chain_cleared as usize >= max_prunes
                    || (report.keys_scanned > 0 && pass_started.elapsed() >= PASS_TIME_BUDGET)
                {
                    report.more_remaining = true;
                    stop_after_page = true;
                    break;
                }

                report.keys_scanned += 1;
                report.next_after_key = Some(mk_key.clone());

                let Ok(rec) = serde_json::from_slice::<PerKeyRecord>(&raw_value) else {
                    report.tips_skipped_unreadable += 1;
                    continue;
                };
                if rec.entry.prev_tip_id.is_empty() {
                    continue;
                }
                report.tips_with_chain += 1;

                // Walk the chain for bytes/keys (same walk as full-store prune).
                let mut vid = rec.entry.prev_tip_id.clone();
                let mut guard = 0u32;
                let mut tv_keys: Vec<String> = Vec::new();
                let mut chain_bytes = 0u64;
                while !vid.is_empty() && guard < 1_000_000 {
                    guard += 1;
                    let bare = molecule_key_codec::tip_version_key(&vid);
                    let full_key = build_storage_key(storage_prefix, &bare);
                    let Ok(Some(bytes)) = self.raw().inner().get(full_key.as_bytes()).await else {
                        break;
                    };
                    chain_bytes += full_key.len() as u64 + bytes.len() as u64;
                    tv_keys.push(full_key);
                    let Ok(entry) = serde_json::from_slice::<crate::atom::AtomEntry>(&bytes) else {
                        break;
                    };
                    vid = entry.prev_tip_id;
                }
                if tv_keys.is_empty() {
                    // Dangling prev_tip_id with no resolvable tv: nodes — still
                    // clear the head link so the tip is history-free.
                }

                if dry_run {
                    report.tips_chain_cleared += 1;
                    report.tip_versions_pruned += tv_keys.len() as u64;
                    report.tip_version_bytes_approx += chain_bytes;
                    continue;
                }

                if !self.apply_tip_chain_prune(&mk_key, &rec, &tv_keys).await? {
                    report.tips_skipped_changed += 1;
                    continue;
                }
                report.tips_chain_cleared += 1;
                report.tip_versions_pruned += tv_keys.len() as u64;
                report.tip_version_bytes_approx += chain_bytes;
            }

            skip_head = Some(page_start.clone());
            if stop_after_page {
                break;
            }
        }

        if !dry_run && report.tips_chain_cleared > 0 {
            let _ = self.flush().await;
        }
        if range_exhausted && !report.more_remaining {
            report.next_after_key = None;
        }
        Ok(report)
    }

    /// Load the durable tip-history drain checkpoint (empty if absent).
    pub async fn tip_history_drain_checkpoint(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<TipHistoryDrainCheckpoint, SchemaError> {
        let key = build_storage_key(storage_prefix, TIP_HISTORY_DRAIN_CHECKPOINT_KEY);
        let raw: Option<TipHistoryDrainCheckpoint> =
            self.raw().get_item(&key).await.map_err(|e| {
                SchemaError::InvalidData(format!("load tip-history drain checkpoint: {e}"))
            })?;
        Ok(raw.unwrap_or_default())
    }

    /// Persist the tip-history drain checkpoint.
    pub async fn put_tip_history_drain_checkpoint(
        &self,
        storage_prefix: Option<&str>,
        checkpoint: &TipHistoryDrainCheckpoint,
    ) -> Result<(), SchemaError> {
        let key = build_storage_key(storage_prefix, TIP_HISTORY_DRAIN_CHECKPOINT_KEY);
        self.raw().put_item(&key, checkpoint).await.map_err(|e| {
            SchemaError::InvalidData(format!("store tip-history drain checkpoint: {e}"))
        })?;
        Ok(())
    }

    /// Run one automatic drain pass from the durable checkpoint and advance it.
    ///
    /// Resume semantics:
    /// - If the prior sweep completed, start from the beginning of `mk:`.
    /// - Otherwise continue from `after_key`.
    /// - On `more_remaining`, store the new cursor; on completion, mark
    ///   `sweep_complete` and clear the cursor so the next cycle restarts.
    ///
    /// Dry-run does not mutate the checkpoint or the data plane.
    pub async fn drain_tip_history_chains_from_checkpoint(
        &self,
        options: TipHistoryDrainOptions,
    ) -> Result<(TipHistoryDrainReport, TipHistoryDrainCheckpoint), SchemaError> {
        let storage_prefix = options.storage_prefix.clone();
        let prefix_ref = storage_prefix.as_deref();
        let mut checkpoint = self.tip_history_drain_checkpoint(prefix_ref).await?;
        let resume_after = if checkpoint.sweep_complete {
            None
        } else {
            checkpoint.after_key.clone()
        };
        let pass_options = TipHistoryDrainOptions {
            after_key: options.after_key.or(resume_after),
            ..options
        };
        let report = self.drain_tip_history_chains(pass_options).await?;
        if report.dry_run {
            return Ok((report, checkpoint));
        }

        checkpoint.passes_completed = checkpoint.passes_completed.saturating_add(1);
        checkpoint.tips_chain_cleared_total = checkpoint
            .tips_chain_cleared_total
            .saturating_add(report.tips_chain_cleared);
        checkpoint.tip_versions_pruned_total = checkpoint
            .tip_versions_pruned_total
            .saturating_add(report.tip_versions_pruned);
        checkpoint.last_pass_at = Some(report.pass_started_at.clone());
        if report.more_remaining {
            checkpoint.after_key = report.next_after_key.clone();
            checkpoint.sweep_complete = false;
        } else {
            checkpoint.after_key = None;
            checkpoint.sweep_complete = true;
        }
        self.put_tip_history_drain_checkpoint(prefix_ref, &checkpoint)
            .await?;
        let _ = self.flush().await;
        Ok((report, checkpoint))
    }
}
