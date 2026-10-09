use super::*;

impl PinLog {
    pub async fn pin_log_statuses(&self) -> Vec<PinLogTargetStatus> {
        let mut statuses: Vec<_> = self
            .state
            .lock()
            .await
            .values()
            .map(PinLogRuntime::status)
            .collect();
        statuses.sort_by(|a, b| a.target_id.cmp(&b.target_id));
        statuses
    }

    /// Incorporated vector F for peer mutation-log apply.
    ///
    /// Merges in-memory `published_f_by_writer` with the durable map so a
    /// restart still covers already-uploaded (and already-applied) writers.
    /// A missing writer is not covered — that is what lets a second Mini
    /// fetch `log/{peer}/{F}.enc`. Scalar F must not be used here: it would
    /// skip a peer stream whose through_id is <= this node's published max.
    pub(crate) async fn incorporated_frontier(&self) -> Frontier {
        let mut by_writer = BTreeMap::new();
        let target_ids = {
            let state = self.state.lock().await;
            for runtime in state.values() {
                for (writer, through) in &runtime.published_f_by_writer {
                    let entry = by_writer.entry(writer.clone()).or_insert(0);
                    *entry = (*entry).max(*through);
                }
            }
            let mut ids: Vec<String> = state.keys().cloned().collect();
            if !ids.iter().any(|id| id == "personal") {
                ids.push("personal".to_string());
            }
            ids
        };
        for target_id in target_ids {
            for (writer, through) in self.read_published_f(&target_id).await {
                let entry = by_writer.entry(writer).or_insert(0);
                *entry = (*entry).max(through);
            }
        }
        Frontier::from_writer_hwm(by_writer)
    }

    /// Max-merge applied peer HWMs into published F so the next cycle does
    /// not re-download those writer-scoped objects. Does not lower any writer.
    pub(crate) async fn incorporate_applied_frontier(
        &self,
        applied: &BTreeMap<String, u64>,
    ) -> Result<(), String> {
        if applied.is_empty() {
            return Ok(());
        }
        let published_at_ms = unix_millis();
        let target_id = "personal";
        // A waiter reads the runtime map. Persist the max-merged HWM first so
        // peer apply cannot satisfy an exact receipt from volatile state that
        // disappears after a failed flush.
        self.persist_published_f(target_id, applied).await?;
        {
            let mut state = self.state.lock().await;
            if let Some(runtime) = state.get_mut(target_id) {
                for (writer, through) in applied {
                    runtime.advance_published_f(writer, *through, published_at_ms);
                }
            }
        }
        Ok(())
    }

    /// Read the durable per-writer published high-water mark for `target_id`.
    ///
    /// Absent means "this home has never completed an upload cycle", which is
    /// the same conservative starting point as an empty in-memory map: every
    /// record reads as pending, so nothing is deleted on a guess.
    ///
    /// A decode failure is downgraded to "unknown" rather than propagated. The
    /// only consequence of losing this value is re-uploading records that are
    /// already in cloud; the only consequence of *trusting a bad one* is
    /// deleting a record no peer can recover. Failing safe here is not
    /// optional.
    pub(super) async fn read_published_f(&self, target_id: &str) -> BTreeMap<String, u64> {
        let store = match self.pin_log_store().await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    target: "fold_db::sync::pin_log",
                    error = %e,
                    "durable published-F read skipped: cannot open durable pin log"
                );
                return BTreeMap::new();
            }
        };
        let key = pin_log_published_f_key(target_id);
        let raw = match store.get(&key).await {
            Ok(Some(raw)) => raw,
            Ok(None) => return BTreeMap::new(),
            Err(e) => {
                tracing::warn!(
                    target: "fold_db::sync::pin_log",
                    target_id = %target_id,
                    error = %e,
                    "durable published-F read failed; treating every record as pending"
                );
                return BTreeMap::new();
            }
        };
        match serde_json::from_slice::<BTreeMap<String, u64>>(&raw) {
            Ok(map) => map,
            Err(e) => {
                tracing::warn!(
                    target: "fold_db::sync::pin_log",
                    target_id = %target_id,
                    error = %e,
                    "durable published-F is undecodable; treating every record as pending"
                );
                BTreeMap::new()
            }
        }
    }

    /// Read a published-F map when the caller intends to advance it.
    ///
    /// A failed or undecodable read must not become an empty map followed by a
    /// successful overwrite. That sequence can regress another writer's
    /// durable confirmation and later reuse its frontier.
    pub(crate) async fn read_published_f_strict(
        &self,
        target_id: &str,
    ) -> Result<BTreeMap<String, u64>, String> {
        let store = self.pin_log_store().await?;
        let Some(raw) = store
            .get(&pin_log_published_f_key(target_id))
            .await
            .map_err(|e| format!("read durable pin-log published-F: {e}"))?
        else {
            return Ok(BTreeMap::new());
        };
        serde_json::from_slice(&raw).map_err(|e| format!("decode durable pin-log published-F: {e}"))
    }

    /// Strict startup floor for this writer's next mutation-log frontier.
    ///
    /// Unlike the conservative status/truncation reader above, this path must
    /// fail when the durable HWM is unreadable. Treating an unknown HWM as zero
    /// can mint a reused frontier that an old cloud confirmation immediately
    /// and falsely satisfies.
    pub(crate) async fn durable_frontier_floor_for_writer(
        &self,
        writer_id: &str,
        targets: &[SyncTarget],
        validate_all_pin_rows: bool,
    ) -> Result<u64, String> {
        let target_ids = targets
            .iter()
            .map(|target| target_id_for_prefix(&target.prefix))
            .collect::<BTreeSet<_>>();
        let store = self
            .pin_log_store()
            .await
            .map_err(|error| format!("open durable pin log for writer frontier seed: {error}"))?;

        // The home-wide point row is the steady-state path. The first frontier
        // allocation in each process also checks every historical pin-row key.
        // That bounded, keys-only fold detects rows written by an older binary
        // during a rollback interval because that binary cannot advance the new
        // point row. Later target generations skip the fold and use point reads.
        let stored_frontier = match store
            .get(PIN_LOG_APPENDED_F_KEY)
            .await
            .map_err(|error| format!("read durable pin-log allocation floor: {error}"))?
        {
            Some(raw) => {
                let bytes: [u8; 8] = raw.try_into().map_err(|raw: Vec<u8>| {
                    format!(
                        "decode durable pin-log allocation floor: expected 8 bytes, got {}",
                        raw.len()
                    )
                })?;
                u64::from_be_bytes(bytes)
            }
            None => 0,
        };
        let mut frontier = stored_frontier;
        if validate_all_pin_rows || stored_frontier == 0 {
            frontier = frontier.max(
                store
                    .max_key_u64_after_marker(PIN_LOG_ENTRY_PREFIX.as_bytes(), b":entry:")
                    .await
                    .map_err(|error| {
                        format!("fold legacy durable pin-log allocation floor: {error}")
                    })?
                    .unwrap_or(0),
            );
        }
        for target_id in target_ids {
            // A removed and re-added target can retain a cloud HWM above this
            // home's floor. Reconfiguration reads each current target's one
            // durable HWM point row before the next frontier is minted.
            let key = pin_log_published_f_key(&target_id);
            let Some(raw) = store.get(&key).await.map_err(|error| {
                format!("read durable published-F for target '{target_id}': {error}")
            })?
            else {
                continue;
            };
            let published =
                serde_json::from_slice::<BTreeMap<String, u64>>(&raw).map_err(|error| {
                    format!("decode durable published-F for target '{target_id}': {error}")
                })?;
            frontier = frontier.max(published.get(writer_id).copied().unwrap_or(0));
        }
        Ok(frontier)
    }

    /// Reserve all old cloud sequence numbers before a primary-authoritative
    /// cut. The append lock prevents a local pin-row batch from lowering the
    /// point floor between the read and the durable max-merge.
    pub(crate) async fn raise_durable_frontier_floor_for_writer(
        &self,
        writer_id: &str,
        targets: &[SyncTarget],
        minimum: u64,
    ) -> Result<u64, String> {
        let _append = self.append_lock.lock().await;
        let current = self
            .durable_frontier_floor_for_writer(writer_id, targets, true)
            .await?;
        let frontier = current.max(minimum);
        if frontier == u64::MAX {
            return Err("primary resume cannot reserve the final writer sequence".into());
        }
        let store = self.pin_log_store().await?;
        store
            .put(PIN_LOG_APPENDED_F_KEY, frontier.to_be_bytes().to_vec())
            .await
            .map_err(|error| format!("reserve primary resume writer frontier: {error}"))?;
        store
            .flush()
            .await
            .map_err(|error| format!("flush primary resume writer frontier: {error}"))?;
        Ok(frontier)
    }

    /// Persist the per-writer published high-water mark for `target_id`.
    ///
    /// The flushed HWM is the authorization barrier. A cloud upload cycle must
    /// persist it before it advances a runtime/plane frontier or truncates any
    /// confirmed pin row. A failure returns an error and keeps every row. The
    /// whole-map max merge is serialized so concurrent peer-apply and upload
    /// updates cannot regress another writer's durable confirmation.
    pub(super) async fn persist_published_f(
        &self,
        target_id: &str,
        by_writer: &BTreeMap<String, u64>,
    ) -> Result<(), String> {
        let _published_f = self.published_f_lock.lock().await;
        let mut merged = self.read_published_f_strict(target_id).await?;
        for (writer_id, through) in by_writer {
            let durable = merged.entry(writer_id.clone()).or_insert(0);
            *durable = (*durable).max(*through);
        }
        let store = self.pin_log_store().await?;
        let bytes = serde_json::to_vec(&merged)
            .map_err(|e| format!("encode durable pin-log published-F: {e}"))?;
        store
            .put(&pin_log_published_f_key(target_id), bytes)
            .await
            .map_err(|e| format!("persist durable pin-log published-F: {e}"))?;
        store
            .flush()
            .await
            .map_err(|e| format!("flush durable pin-log published-F: {e}"))
    }
}

// lint:file-size-ok moved verbatim from pin_log.rs; cohesive unit, split further in a later pass
