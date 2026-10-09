//! Superseded-version retention and its durable checkpoint.

use super::*;

impl AtomStore {
    /// One bounded, resumable pass that drops **expired** `tv:` nodes on live
    /// heads while keeping versions written inside the 7-day window.
    ///
    /// This is not [`Self::drain_tip_history_chains`]: drain clears the whole
    /// chain. Retention keeps recent `as_of` history. Tombstoned heads are
    /// skipped so deleted-record semantics stay timer-free.
    ///
    /// Undatable `tv:` nodes (`written_at == 0`) are kept. The first dated
    /// node older than the cutoff, and every node after it, is expired.
    // lint:fn-size-ok verbatim move from admin_db.rs; splitting this function is separate work
    pub async fn retain_superseded_versions(
        &self,
        options: SupersededVersionRetentionOptions,
    ) -> Result<SupersededVersionRetentionReport, SchemaError> {
        use crate::atom::molecule_key_codec;
        use crate::db_operations::atom_store::PerKeyRecord;

        const SCAN_PAGE: usize = 512;
        const PASS_TIME_BUDGET: std::time::Duration = std::time::Duration::from_secs(120);

        let max_keys = options.max_keys.max(1);
        let max_prunes = options.max_prunes.unwrap_or(max_keys).max(1);
        let dry_run = options.dry_run;
        let storage_prefix = options.storage_prefix.as_deref();
        let retention_seconds = options
            .retention_seconds
            .filter(|secs| *secs > 0)
            .unwrap_or(molecule_key_codec::SUPERSEDED_VERSION_RETENTION_SECS);
        let now_ns = unix_nanos();
        let cutoff_ns = now_ns.saturating_sub(retention_seconds.saturating_mul(1_000_000_000));
        let pass_started = std::time::Instant::now();
        let pass_started_at = Utc::now().to_rfc3339();

        let mk_prefix = build_storage_key(storage_prefix, "mk:");
        let mk_end = FilterUtils::create_prefix_end(&mk_prefix);
        let mut report = SupersededVersionRetentionReport {
            dry_run,
            retention_seconds,
            cutoff_written_at_ns: cutoff_ns,
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
                .map_err(|e| {
                    SchemaError::InvalidData(format!("superseded-version retain scan mk: {e}"))
                })?;
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
                    || report.tips_truncated as usize >= max_prunes
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
                if rec.meta.as_ref().is_some_and(|m| m.tombstoned) {
                    report.tips_skipped_tombstoned += 1;
                    continue;
                }
                if rec.entry.prev_tip_id.is_empty() {
                    continue;
                }
                report.tips_with_chain += 1;

                let mut vid = rec.entry.prev_tip_id.clone();
                let mut guard = 0u32;
                let mut nodes: Vec<(String, crate::atom::AtomEntry, u64)> = Vec::new();
                while !vid.is_empty() && guard < 1_000_000 {
                    guard += 1;
                    let bare = molecule_key_codec::tip_version_key(&vid);
                    let full_key = build_storage_key(storage_prefix, &bare);
                    let Ok(Some(bytes)) = self.raw().inner().get(full_key.as_bytes()).await else {
                        break;
                    };
                    let chain_bytes = full_key.len() as u64 + bytes.len() as u64;
                    let Ok(entry) = serde_json::from_slice::<crate::atom::AtomEntry>(&bytes) else {
                        break;
                    };
                    let next = entry.prev_tip_id.clone();
                    nodes.push((full_key, entry, chain_bytes));
                    vid = next;
                }
                if nodes.is_empty() {
                    continue;
                }

                let Some(expired_at) = first_expired_tv_index(&nodes, cutoff_ns) else {
                    continue;
                };
                let kept = &nodes[..expired_at];
                let expired = &nodes[expired_at..];
                let expired_keys: Vec<String> = expired.iter().map(|n| n.0.clone()).collect();
                let expired_bytes: u64 = expired.iter().map(|n| n.2).sum();

                if dry_run {
                    report.tips_truncated += 1;
                    report.tip_versions_pruned += expired.len() as u64;
                    report.tip_versions_kept += kept.len() as u64;
                    report.tip_version_bytes_approx += expired_bytes;
                    continue;
                }

                let last_kept = kept.last();
                let applied = self
                    .apply_expired_chain_truncate(
                        &mk_key,
                        &rec,
                        last_kept.map(|n| n.0.as_str()),
                        last_kept.map(|n| &n.1),
                        &expired_keys,
                    )
                    .await?;
                if !applied {
                    report.tips_skipped_changed += 1;
                    continue;
                }
                report.tips_truncated += 1;
                report.tip_versions_pruned += expired.len() as u64;
                report.tip_versions_kept += kept.len() as u64;
                report.tip_version_bytes_approx += expired_bytes;
            }

            skip_head = Some(page_start.clone());
            if stop_after_page {
                break;
            }
        }

        if !dry_run && report.tips_truncated > 0 {
            let _ = self.flush().await;
        }
        if range_exhausted && !report.more_remaining {
            report.next_after_key = None;
        }
        report.settled =
            !report.more_remaining && report.tips_truncated == 0 && report.tip_versions_pruned == 0;
        Ok(report)
    }

    /// CAS-gated truncate: drop expired `tv:` nodes and clear `prev_tip_id` on
    /// the last kept node (or the live head when the whole chain is expired).
    ///
    /// The delete and the rewrite share one re-read. Splitting them leaves a
    /// head whose `prev_tip_id` points at a `tv:` row that no longer exists.
    pub(super) async fn apply_expired_chain_truncate(
        &self,
        mk_key: &str,
        observed: &super::super::atom_store::PerKeyRecord,
        last_kept_tv_key: Option<&str>,
        last_kept_observed: Option<&crate::atom::AtomEntry>,
        expired_tv_keys: &[String],
    ) -> Result<bool, SchemaError> {
        let Some(last_kept_key) = last_kept_tv_key else {
            return self
                .apply_tip_chain_prune(mk_key, observed, expired_tv_keys)
                .await;
        };

        use super::super::atom_store::PerKeyRecord;
        const CHUNK: usize = 2000;

        let current: Option<PerKeyRecord> = self
            .raw()
            .get_item(mk_key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("retain re-read mk tip: {e}")))?;
        if !head_is_unchanged(current.as_ref(), observed) {
            return Ok(false);
        }

        let kept_now: Option<crate::atom::AtomEntry> = self
            .raw()
            .get_item(last_kept_key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("retain re-read kept tv: {e}")))?;
        let Some(kept_now) = kept_now else {
            return Ok(false);
        };
        if let Some(observed_kept) = last_kept_observed {
            if kept_now.prev_tip_id != observed_kept.prev_tip_id
                || kept_now.atom_uuid != observed_kept.atom_uuid
                || kept_now.written_at != observed_kept.written_at
            {
                return Ok(false);
            }
        }

        let (molecule_uuid, disk_hash, disk_range) = Self::molecule_slot_from_storage_key(mk_key)
            .ok_or_else(|| {
            SchemaError::InvalidData(format!("retain cannot decode mk slot: {mk_key}"))
        })?;

        for chunk in expired_tv_keys.chunks(CHUNK) {
            let mut derived_keys = self
                .tip_version_backref_delete_keys_for_tv_keys(chunk)
                .await?;
            derived_keys.extend(
                self.atom_ref_tip_version_delete_keys(
                    molecule_uuid,
                    &disk_hash,
                    &disk_range,
                    chunk,
                )
                .await?,
            );
            let mut mutations: Vec<KvMutation> = chunk
                .iter()
                .map(|key| KvMutation::delete(key.as_bytes().to_vec()))
                .collect();
            mutations.extend(derived_keys.into_iter().map(KvMutation::delete));
            self.raw()
                .inner()
                .batch_mutate(mutations)
                .await
                .map_err(|e| {
                    SchemaError::InvalidData(format!("retain delete expired tip versions: {e}"))
                })?;
        }

        let mut rewritten = kept_now;
        rewritten.prev_tip_id.clear();
        self.raw()
            .put_item(last_kept_key, &rewritten)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("retain rewrite kept tv: {e}")))?;
        Ok(true)
    }

    /// Load the durable superseded-version retention checkpoint.
    pub async fn superseded_version_retention_checkpoint(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<SupersededVersionRetentionCheckpoint, SchemaError> {
        let key = build_storage_key(storage_prefix, SUPERSEDED_VERSION_RETENTION_CHECKPOINT_KEY);
        let raw: Option<SupersededVersionRetentionCheckpoint> =
            self.raw().get_item(&key).await.map_err(|e| {
                SchemaError::InvalidData(format!("load superseded-version retain checkpoint: {e}"))
            })?;
        Ok(raw.unwrap_or_default())
    }

    /// Persist the superseded-version retention checkpoint.
    pub async fn put_superseded_version_retention_checkpoint(
        &self,
        storage_prefix: Option<&str>,
        checkpoint: &SupersededVersionRetentionCheckpoint,
    ) -> Result<(), SchemaError> {
        let key = build_storage_key(storage_prefix, SUPERSEDED_VERSION_RETENTION_CHECKPOINT_KEY);
        self.raw().put_item(&key, checkpoint).await.map_err(|e| {
            SchemaError::InvalidData(format!("store superseded-version retain checkpoint: {e}"))
        })?;
        Ok(())
    }

    /// Run one retention pass from the durable checkpoint and advance it.
    /// Dry-run does not mutate the checkpoint or the data plane.
    pub async fn retain_superseded_versions_from_checkpoint(
        &self,
        options: SupersededVersionRetentionOptions,
    ) -> Result<
        (
            SupersededVersionRetentionReport,
            SupersededVersionRetentionCheckpoint,
        ),
        SchemaError,
    > {
        let storage_prefix = options.storage_prefix.clone();
        let prefix_ref = storage_prefix.as_deref();
        let mut checkpoint = self
            .superseded_version_retention_checkpoint(prefix_ref)
            .await?;
        let resume_after = if checkpoint.sweep_complete {
            None
        } else {
            checkpoint.after_key.clone()
        };
        let pass_options = SupersededVersionRetentionOptions {
            after_key: options.after_key.or(resume_after),
            ..options
        };
        let report = self.retain_superseded_versions(pass_options).await?;
        if report.dry_run {
            return Ok((report, checkpoint));
        }

        checkpoint.passes_completed = checkpoint.passes_completed.saturating_add(1);
        checkpoint.tips_truncated_total = checkpoint
            .tips_truncated_total
            .saturating_add(report.tips_truncated);
        checkpoint.tip_versions_pruned_total = checkpoint
            .tip_versions_pruned_total
            .saturating_add(report.tip_versions_pruned);
        checkpoint.last_pass_at = Some(report.pass_started_at.clone());
        if report.more_remaining {
            checkpoint.after_key = report.next_after_key.clone();
            checkpoint.sweep_complete = false;
            checkpoint.settled = false;
        } else {
            checkpoint.after_key = None;
            checkpoint.sweep_complete = true;
            checkpoint.settled = report.settled;
        }
        self.put_superseded_version_retention_checkpoint(prefix_ref, &checkpoint)
            .await?;
        let _ = self.flush().await;
        Ok((report, checkpoint))
    }
}
