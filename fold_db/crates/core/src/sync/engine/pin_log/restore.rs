use super::*;

/// Read the same personal writer floor as the engine, without constructing a
/// live sync engine. An offline resume plan uses this on a stopped copy.
pub(crate) async fn primary_resume_durable_local_frontier(
    store: &dyn crate::storage::traits::NamespacedStore,
    writer_id: &str,
) -> Result<u64, String> {
    let pin_log = store
        .open_namespace(PIN_LOG_NAMESPACE)
        .await
        .map_err(|error| format!("open durable pin log: {error}"))?;
    let stored = match pin_log
        .get(PIN_LOG_APPENDED_F_KEY)
        .await
        .map_err(|error| format!("read durable pin-log allocation floor: {error}"))?
    {
        Some(raw) => {
            let bytes: [u8; 8] = raw.try_into().map_err(|raw: Vec<u8>| {
                format!(
                    "invalid durable pin-log allocation floor: {} bytes",
                    raw.len()
                )
            })?;
            u64::from_be_bytes(bytes)
        }
        None => 0,
    };
    let rows = pin_log
        .max_key_u64_after_marker(PIN_LOG_ENTRY_PREFIX.as_bytes(), b":entry:")
        .await
        .map_err(|error| format!("read durable pin-log rows: {error}"))?
        .unwrap_or(0);
    let published = match pin_log
        .get(&pin_log_published_f_key("personal"))
        .await
        .map_err(|error| format!("read durable personal published frontier: {error}"))?
    {
        Some(raw) => serde_json::from_slice::<BTreeMap<String, u64>>(&raw)
            .map_err(|error| format!("decode durable personal published frontier: {error}"))?
            .get(writer_id)
            .copied()
            .unwrap_or(0),
        None => 0,
    };
    Ok(stored.max(rows).max(published))
}

/// How a restored S0 treats the cloud mutation tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackupRestoreMode {
    ReplayTail,
    S0Only,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BackupRestoreFrontier {
    pub(super) version: u32,
    pub(super) by_writer: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) mode: Option<BackupRestoreMode>,
}

impl BackupRestoreFrontier {
    pub(super) fn validate(&self) -> SyncResult<BackupRestoreMode> {
        if self.by_writer.keys().any(String::is_empty) {
            return Err(SyncError::Storage("invalid backup writer frontier".into()));
        }
        match (self.version, self.mode) {
            (1, None) => Ok(BackupRestoreMode::ReplayTail),
            (2, Some(BackupRestoreMode::S0Only)) => Ok(BackupRestoreMode::S0Only),
            _ => Err(SyncError::Storage("invalid backup writer frontier".into())),
        }
    }
}
impl SyncEngine {
    pub(super) async fn publication_positions_confirmed(
        &self,
        positions: &[MutationLogTargetPosition],
    ) -> bool {
        if positions.is_empty() {
            return false;
        }
        let state = self.pin_log.state.lock().await;
        positions.iter().all(|position| {
            state
                .get(&position.target_id)
                .and_then(|runtime| runtime.published_f_by_writer.get(&position.writer_id))
                .is_some_and(|published| *published >= position.frontier)
        })
    }

    /// Wait for cloud confirmation of every exact target/writer frontier.
    ///
    /// A scalar max never enters this predicate. A newer frontier from a
    /// different writer or target cannot satisfy the receipt.
    pub(crate) async fn wait_for_mutation_publication(
        &self,
        positions: &[MutationLogTargetPosition],
        timeout: std::time::Duration,
    ) -> MutationPublicationWait {
        let started = std::time::Instant::now();
        loop {
            if self.publication_positions_confirmed(positions).await {
                return MutationPublicationWait::Published;
            }
            let elapsed = started.elapsed();
            if elapsed >= timeout {
                return MutationPublicationWait::Pending;
            }
            let remaining = timeout.saturating_sub(elapsed);
            tokio::time::sleep(remaining.min(std::time::Duration::from_millis(25))).await;
        }
    }

    /// List + download continuous mutation-log segments from the personal cloud
    /// prefix that are not wholly covered by `incorporated_frontier`.
    ///
    /// Classic `LogEntry` objects that still share the flat `log/` namespace are
    /// skipped (unseal-as-segment fails). Fail-closed only when a listed object
    /// that *is* a mutation-log segment cannot be authenticated/decoded.
    pub async fn download_mutation_log_segments_above(
        &self,
        incorporated_frontier: &Frontier,
    ) -> SyncResult<Vec<MutationLogSegment>> {
        let target = {
            let targets = self.targets.lock().await;
            targets.first().cloned().ok_or_else(|| {
                SyncError::Storage(
                    "download mutation-log segments: no personal sync target".to_string(),
                )
            })?
        };
        self.download_mutation_log_segments_above_target(&target, incorporated_frontier)
            .await
    }

    pub(crate) async fn download_mutation_log_segments_above_target(
        &self,
        target: &SyncTarget,
        incorporated_frontier: &Frontier,
    ) -> SyncResult<Vec<MutationLogSegment>> {
        self.download_mutation_log_segments_with_progress(target, incorporated_frontier, None, None)
            .await
    }

    /// Peer-apply download: like
    /// [`Self::download_mutation_log_segments_above_target`], but the listing
    /// jumps over this node's own `log/{device_id}/` range. Peer apply drops
    /// those segments as self-echo anyway; paging through them is what made
    /// one `do_sync` spend 7-24 minutes listing on the primary (2026-10-05).
    pub(crate) async fn download_peer_mutation_log_segments(
        &self,
        target: &SyncTarget,
        incorporated_frontier: &Frontier,
    ) -> SyncResult<Vec<MutationLogSegment>> {
        let skip_writer = (!self.device_id.is_empty()).then_some(self.device_id.as_str());
        self.download_mutation_log_segments_with_progress(
            target,
            incorporated_frontier,
            None,
            skip_writer,
        )
        .await
    }

    pub(super) async fn download_mutation_log_segments_with_progress(
        &self,
        target: &SyncTarget,
        incorporated_frontier: &Frontier,
        progress: Option<&RestoreProgress>,
        skip_writer: Option<&str>,
    ) -> SyncResult<Vec<MutationLogSegment>> {
        // lint:fn-size-ok moved verbatim from pin_log.rs; splitting this function is separate work.
        progress::phase(progress, RestorePhase::TailDownload);
        let list_started = std::time::Instant::now();
        let objects = match skip_writer {
            Some(writer) => {
                let (objects, stats) = progress::measure(
                    progress,
                    TransferOperation::Authorization,
                    self.auth.list_log_objects_skipping_writer(target, writer),
                )
                .await?;
                tracing::info!(
                    target: "fold_db::sync::mutation_log",
                    target_label = %target.label,
                    listed = objects.len(),
                    pages = stats.pages,
                    skipped_own_keys = stats.skipped_keys,
                    jumped_own_range = stats.jumped,
                    list_ms = list_started.elapsed().as_millis() as u64,
                    "peer-apply log listing"
                );
                objects
            }
            None => {
                progress::measure(
                    progress,
                    TransferOperation::Authorization,
                    self.auth.list_log_objects(target),
                )
                .await?
            }
        };
        let (candidates, flat_classic_skipped) = select_peer_apply_candidates(
            objects.iter().map(|obj| obj.key.as_str()),
            incorporated_frontier,
        );
        if flat_classic_skipped > 0 {
            tracing::debug!(
                target: "fold_db::sync::mutation_log",
                flat_classic_skipped,
                candidates = candidates.len(),
                "peer-apply skipped classic flat log objects without downloading them"
            );
        }

        let candidates = candidates
            .into_iter()
            .map(|(writer, through_id, object_key)| {
                let segment = mutation_log_segment_id_from_object_key(&object_key, through_id)?;
                Ok((writer, object_key, segment))
            })
            .collect::<Result<Vec<_>, String>>()
            .map_err(SyncError::Storage)?;
        progress::update(progress, |p| p.tail_objects_total = Some(candidates.len()));
        let candidate_count = candidates.len();
        let download_started = std::time::Instant::now();
        let mut downloaded = 0usize;
        let mut segments = Vec::with_capacity(candidates.len());
        // Reuse the ordinary presign batch size so restore does not invent a
        // second fan-out policy for the same cloud plane. Keep typed and
        // legacy requests separate. A share scope accepts server-derived typed
        // keys, while its legacy path still uses flat sequence keys.
        const BATCH: usize = 32;
        for typed in [false, true] {
            let selected = candidates
                .iter()
                .filter(|(_, _, segment)| segment.schema_name.is_some() == typed)
                .collect::<Vec<_>>();
            for chunk in selected.chunks(BATCH) {
                let segment_ids = chunk
                    .iter()
                    .map(|(_, _, segment)| segment.clone())
                    .collect::<Vec<_>>();
                let urls = progress::measure(progress, TransferOperation::Authorization, async {
                    if typed {
                        self.auth
                            .presign_download_segments(target, &segment_ids)
                            .await
                    } else {
                        let seqs = segment_ids
                            .iter()
                            .map(|segment| segment.through_id)
                            .collect::<Vec<_>>();
                        let object_keys = segment_ids
                            .iter()
                            .map(|segment| segment.object_key.clone())
                            .collect::<Vec<_>>();
                        self.auth
                            .presign_download_object_keys(target, &seqs, &object_keys)
                            .await
                    }
                })
                .await?;
                if urls.len() != chunk.len() {
                    return Err(SyncError::Auth(format!(
                        "expected {} presigned mutation-log download URLs, got {}",
                        chunk.len(),
                        urls.len()
                    )));
                }
                for (((writer_hint, object_key, _), segment_id), url) in
                    chunk.iter().copied().zip(segment_ids).zip(urls)
                {
                    let Some(bytes) = progress::measure(
                        progress,
                        TransferOperation::Download,
                        self.s3.download(&url),
                    )
                    .await?
                    else {
                        return Err(SyncError::Storage(format!(
                            "mutation-log segment {object_key} missing during restore download"
                        )));
                    };
                    downloaded = downloaded.saturating_add(1);
                    progress::update(progress, |p| {
                        p.response_body_bytes =
                            p.response_body_bytes.saturating_add(bytes.len() as u64);
                        p.tail_objects_downloaded += 1;
                    });
                    let writer = segment_id.writer_id.as_deref().unwrap_or(writer_hint);
                    if incorporated_frontier.covers_log(Some(writer), segment_id.through_id) {
                        continue;
                    }
                    segments.push(MutationLogSegment {
                        segment: segment_id,
                        payload: bytes,
                    });
                }
            }
        }
        if skip_writer.is_some() {
            tracing::info!(
                target: "fold_db::sync::mutation_log",
                target_label = %target.label,
                candidates = candidate_count,
                downloaded,
                kept = segments.len(),
                download_ms = download_started.elapsed().as_millis() as u64,
                "peer-apply log download"
            );
        }
        segments.sort_by(|a, b| {
            a.segment
                .writer_id
                .cmp(&b.segment.writer_id)
                .then(a.segment.through_id.cmp(&b.segment.through_id))
        });
        Ok(segments)
    }

    /// Production restore phase 2: after LastStore S0 is installed on this
    /// engine's store, download continuous mutation-log segments above F and
    /// apply them via [`restore_mutation_log_after_s0`].
    pub async fn restore_mutation_log_after_s0(
        &self,
        incorporated_frontier: &Frontier,
    ) -> SyncResult<MutationLogReplayReport> {
        self.restore_mutation_log_after_s0_with_progress(incorporated_frontier, None)
            .await
    }

    pub async fn restore_mutation_log_after_s0_with_progress(
        &self,
        incorporated_frontier: &Frontier,
        progress: Option<&RestoreProgress>,
    ) -> SyncResult<MutationLogReplayReport> {
        let target = self.targets.lock().await.first().cloned().ok_or_else(|| {
            SyncError::Storage(
                "download mutation-log segments: no personal sync target".to_string(),
            )
        })?;
        let segments = self
            .download_mutation_log_segments_with_progress(
                &target,
                incorporated_frontier,
                progress,
                None,
            )
            .await?;
        replay_mutation_log_segments_with_progress(
            self,
            &segments,
            incorporated_frontier,
            progress,
            &target.crypto,
        )
        .await
    }

    /// Pin a lower bound before the LastStore snapshot enumerates any file.
    /// Published F alone is not a snapshot boundary: it can advance while
    /// mutable chunks are enumerated, after the corresponding atom chunk cut.
    /// Read F first, drain the accepted writes it covers, then persist this
    /// separate marker. Later confirmations never modify the pinned marker.
    pub(crate) async fn prepare_backup_restore_frontier(&self) -> SyncResult<()> {
        let backup_only = self
            .backup_only_mode
            .load(std::sync::atomic::Ordering::Acquire);
        let resume_frontier = *self.primary_resume_frontier.lock().await;
        if resume_frontier.is_some() && !backup_only {
            return Err(SyncError::Storage(
                "primary resume frontier requires backup-only mode".into(),
            ));
        }
        let marker = BackupRestoreFrontier {
            version: if backup_only && resume_frontier.is_none() {
                2
            } else {
                1
            },
            by_writer: match resume_frontier {
                Some(frontier) => BTreeMap::from([(self.device_id.clone(), frontier)]),
                None => self
                    .pin_log
                    .read_published_f_strict("personal")
                    .await
                    .map_err(SyncError::Storage)?,
            },
            mode: (backup_only && resume_frontier.is_none()).then_some(BackupRestoreMode::S0Only),
        };
        marker.validate()?;
        let barrier = self.photograph_cut_barrier.lock().await.clone();
        match barrier {
            Some(barrier) => barrier().await.map_err(SyncError::Storage)?,
            None if marker.by_writer.is_empty() => {}
            None => {
                return Err(SyncError::Storage(
                    "backup writer frontier requires a persistence barrier".into(),
                ))
            }
        }
        let store = self.backup_restore_frontier_store().await?;
        // Every new cut needs an explicit marker. A missing legacy marker
        // reads as an empty frontier, but remote recovery cannot tell that
        // history from a missing or incomplete snapshot.
        let raw = serde_json::to_vec(&marker).map_err(|error| {
            SyncError::Storage(format!("encode backup writer frontier: {error}"))
        })?;
        store
            .put(BACKUP_RESTORE_F_KEY, raw)
            .await
            .map_err(|error| {
                SyncError::Storage(format!("persist backup writer frontier: {error}"))
            })?;
        store
            .flush()
            .await
            .map_err(|error| SyncError::Storage(format!("flush backup writer frontier: {error}")))
    }

    /// Read only the cut marker from the freshly restored S0 store.
    /// Never substitute a storage CSN, the current owner's published F, or the
    /// ordinary published-F row restored from a later mutable-chunk cut.
    /// Legacy snapshots without this marker conservatively replay all writers.
    pub async fn restored_backup_mutation_frontier(&self) -> SyncResult<Frontier> {
        self.restored_backup_mode_and_frontier()
            .await
            .map(|(frontier, _)| frontier)
    }

    /// The cut marker inside S0 determines whether tail replay is safe.
    /// A missing legacy marker means an empty frontier with normal replay.
    pub async fn restored_backup_mode_and_frontier(
        &self,
    ) -> SyncResult<(Frontier, BackupRestoreMode)> {
        self.restored_backup_marker_and_frontier()
            .await
            .map(|(frontier, mode)| (frontier, mode.unwrap_or(BackupRestoreMode::ReplayTail)))
    }

    /// Preserve the difference between an absent legacy marker and an explicit
    /// replay marker. Remote S0 recovery may use an authenticated rescue
    /// descriptor only when the stored marker is absent.
    pub async fn restored_backup_marker_and_frontier(
        &self,
    ) -> SyncResult<(Frontier, Option<BackupRestoreMode>)> {
        let store = self.backup_restore_frontier_store().await?;
        let Some(raw) = store
            .get(BACKUP_RESTORE_F_KEY)
            .await
            .map_err(|error| SyncError::Storage(format!("read backup writer frontier: {error}")))?
        else {
            return Ok((Frontier::from_writer_hwm(BTreeMap::new()), None));
        };
        let marker: BackupRestoreFrontier = serde_json::from_slice(&raw).map_err(|error| {
            SyncError::Storage(format!("decode backup writer frontier: {error}"))
        })?;
        let mode = marker.validate()?;
        Ok((Frontier::from_writer_hwm(marker.by_writer), Some(mode)))
    }

    pub(super) async fn backup_restore_frontier_store(
        &self,
    ) -> SyncResult<Arc<dyn crate::storage::traits::KvStore>> {
        if let Some(source) = &self.laststore_backup_source {
            return source
                .open_namespace(PIN_LOG_NAMESPACE)
                .await
                .map_err(|error| {
                    SyncError::Storage(format!("open backup frontier namespace: {error}"))
                });
        }
        self.pin_log
            .pin_log_store()
            .await
            .map_err(SyncError::Storage)
    }

    pub(crate) async fn restore_mutation_log_after_photograph(
        &self,
        target: &SyncTarget,
        incorporated_frontier: &Frontier,
    ) -> SyncResult<MutationLogReplayReport> {
        let segments = self
            .download_mutation_log_segments_above_target(target, incorporated_frontier)
            .await?;
        replay_mutation_log_segments_with_crypto(
            self,
            &segments,
            incorporated_frontier,
            &target.crypto,
        )
        .await
    }

    /// Regular `do_sync` peer apply: download writer-scoped mutation-log
    /// segments not covered by this node's published vector F, skip the local
    /// writer (self-echo), apply via [`restore_mutation_log_after_s0`], then
    /// persist applied HWMs so the next cycle does not re-fetch them.
    ///
    /// Compact photograph S is not required. LastStore S0 restore stays on
    /// [`Self::restore_mutation_log_after_s0`]; this path is the live Mini.
    ///
    /// Walks every configured target. Catalog membership for a named org DB is
    /// published on the org-hash head, not the personal prefix. Listing only
    /// `targets.first()` left a granted member Mini with zero org segments
    /// and `catalog_membership_denied` on the shared schema.
    pub async fn run_mutation_log_peer_apply_cycle(&self) -> SyncResult<MutationLogReplayReport> {
        let mut incorporated = self.pin_log.incorporated_frontier().await;
        let targets = self.targets.lock().await.clone();
        let mut combined = MutationLogReplayReport::default();
        let mut first_err: Option<SyncError> = None;
        let mut auth_err: Option<SyncError> = None;
        for target in &targets {
            match self
                .peer_apply_mutation_log_on_target(target, &incorporated)
                .await
            {
                Ok(report) => {
                    combined.segments_considered = combined
                        .segments_considered
                        .saturating_add(report.segments_considered);
                    combined.segments_applied = combined
                        .segments_applied
                        .saturating_add(report.segments_applied);
                    combined.records_applied = combined
                        .records_applied
                        .saturating_add(report.records_applied);
                    combined.records_skipped_at_or_below_frontier = combined
                        .records_skipped_at_or_below_frontier
                        .saturating_add(report.records_skipped_at_or_below_frontier);
                    for (writer, through) in report.frontier_after {
                        let entry = combined.frontier_after.entry(writer).or_insert(0);
                        if through > *entry {
                            *entry = through;
                        }
                    }
                    if !combined.frontier_after.is_empty() {
                        incorporated = Frontier::from_writer_hwm(combined.frontier_after.clone());
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        target: "fold_db::sync::mutation_log",
                        target_label = %target.label,
                        error = %redact_sync_error_text(&e.to_string()),
                        "mutation-log peer apply failed for target (continuing remaining targets)"
                    );
                    if matches!(e, SyncError::Auth(_)) {
                        if auth_err.is_none() {
                            auth_err = Some(e);
                        }
                    } else if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }
        // Count before the frontier persist: the segments were applied to the
        // store either way, and a failed HWM persist is a re-apply risk, not a
        // reason to under-report what this process already replayed.
        if combined.segments_applied > 0 {
            self.mutation_log_peer_segments_applied.fetch_add(
                combined.segments_applied as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        if combined.records_applied > 0 {
            self.mutation_log_peer_records_applied.fetch_add(
                combined.records_applied as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        if !combined.frontier_after.is_empty() {
            if let Err(e) = self
                .pin_log
                .incorporate_applied_frontier(&combined.frontier_after)
                .await
            {
                tracing::warn!(
                    target: "fold_db::sync::mutation_log",
                    error = %e,
                    "durable incorporated-F persist failed; peer segments may be re-applied after restart"
                );
            }
        }
        // Auth must surface even after another target listed/applied segments.
        // Returning Ok here used to stamp last_peer_apply_ms and skip the
        // token-refresh retry for the whole drain interval.
        if let Some(e) = super::cycle::peer_apply_cycle_terminal_error(
            combined.segments_considered,
            auth_err,
            first_err,
        ) {
            return Err(e);
        }
        Ok(combined)
    }

    pub(super) async fn peer_apply_mutation_log_on_target(
        &self,
        target: &SyncTarget,
        incorporated: &Frontier,
    ) -> SyncResult<MutationLogReplayReport> {
        let mut segments = self
            .download_peer_mutation_log_segments(target, incorporated)
            .await?;
        if !self.device_id.is_empty() {
            segments.retain(|segment| {
                segment.segment.writer_id.as_deref() != Some(self.device_id.as_str())
            });
        }
        replay_mutation_log_segments_with_crypto(self, &segments, incorporated, &target.crypto)
            .await
    }
}

// lint:file-size-ok moved verbatim from pin_log.rs; cohesive unit, split further in a later pass
