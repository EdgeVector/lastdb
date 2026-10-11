use super::*;

impl PinLog {
    pub(super) async fn persist_pin_log_records(
        &self,
        records: &[PinLogRecord],
    ) -> Result<(), String> {
        self.persist_pin_log_records_with_receipts(records, Vec::new())
            .await
    }

    pub(super) async fn persist_pin_log_records_with_receipts(
        &self,
        records: &[PinLogRecord],
        receipts: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), String> {
        // lint:fn-size-ok moved verbatim from pin_log.rs; splitting this function is separate work.
        if records.is_empty() && receipts.is_empty() {
            return Ok(());
        }
        let _append = self.append_lock.lock().await;
        let store = self.pin_log_store().await?;
        let mut allocation_floor = None;
        if let Some(mut frontier) = records.iter().map(|record| record.frontier_after).max() {
            if let Some(raw) = store
                .get(PIN_LOG_APPENDED_F_KEY)
                .await
                .map_err(|e| format!("read durable pin-log allocation floor: {e}"))?
            {
                let bytes: [u8; 8] = raw.try_into().map_err(|raw: Vec<u8>| {
                    format!(
                        "decode durable pin-log allocation floor: expected 8 bytes, got {}",
                        raw.len()
                    )
                })?;
                frontier = frontier.max(u64::from_be_bytes(bytes));
            }
            allocation_floor = Some((
                PIN_LOG_APPENDED_F_KEY.to_vec(),
                frontier.to_be_bytes().to_vec(),
            ));
        }
        let mut row_items = Vec::with_capacity(records.len());
        let mut stats = Vec::with_capacity(records.len());
        for record in records {
            let bytes = serde_json::to_vec(record)
                .map_err(|e| format!("encode durable pin log entry: {e}"))?;
            stats.push((
                record.target_id.clone(),
                record.target_label.clone(),
                record.target_prefix.clone(),
                record.frontier_after,
                record.timestamp_ms,
                bytes.len() as u64,
            ));
            row_items.push((
                pin_log_entry_key(&record.target_id, record.frontier_after),
                bytes,
            ));
        }
        if receipts.is_empty() {
            // The ordinary append and receipt-repair path retains one group
            // flush. Any repaired receipt already has a durable floor.
            let mut items = Vec::with_capacity(row_items.len() + 1);
            if let Some(floor) = allocation_floor {
                items.push(floor);
            }
            items.extend(row_items);
            store
                .batch_put(items)
                .await
                .map_err(|e| format!("persist durable pin log entries: {e}"))?;
            store
                .flush()
                .await
                .map_err(|e| format!("flush durable pin log group: {e}"))?;
        } else {
            // LastStore has no cross-group WAL crash atomicity. Confirm the
            // allocation floor before a receipt can survive a crash; confirm
            // every receipt before any target row can survive a crash. A
            // retry repairs a missing row from the receipt. Three flushes
            // cover the whole batch, regardless of its marker count.
            let floor = allocation_floor
                .ok_or_else(|| "capture marker receipts have no allocation floor".to_string())?;
            store
                .put(&floor.0, floor.1)
                .await
                .map_err(|e| format!("reserve capture marker allocation floor: {e}"))?;
            store
                .flush()
                .await
                .map_err(|e| format!("flush capture marker allocation floor: {e}"))?;
            store
                .batch_put(receipts)
                .await
                .map_err(|e| format!("persist capture marker receipts: {e}"))?;
            store
                .flush()
                .await
                .map_err(|e| format!("flush capture marker receipts: {e}"))?;
            store
                .batch_put(row_items)
                .await
                .map_err(|e| format!("persist capture marker target rows: {e}"))?;
            store
                .flush()
                .await
                .map_err(|e| format!("flush capture marker target rows: {e}"))?;
        }

        let mut state = self.state.lock().await;
        for (target_id, target_label, target_prefix, frontier, ts, bytes) in stats {
            let runtime = state.entry(target_id.clone()).or_insert_with(|| {
                PinLogRuntime::new(
                    target_id,
                    target_label.clone(),
                    target_prefix.clone(),
                    0,
                    0,
                    false,
                )
            });
            runtime.target_label = target_label;
            runtime.target_prefix = target_prefix;
            runtime.last_durable_frontier = runtime.last_durable_frontier.max(frontier);
            runtime.entry_count += 1;
            runtime.byte_count += bytes;
            runtime.last_durable_at_ms = Some(ts);
        }
        Ok(())
    }

    /// Read **at most `want`** pending pin-log records for `target`, paging the
    /// durable scan so the plane's size never becomes the cycle's memory cost.
    ///
    /// # Why this exists
    ///
    /// [`Self::read_pin_log_records_for_target`] materialises the whole target
    /// prefix — every key *and every value* — before anything filters it. The
    /// upload cycle then applied its `max_segments` cap to the decoded result,
    /// so the cap bounded how much the cycle *published* and bounded nothing
    /// about what it *read*.
    ///
    /// On Tom's primary the `sync_pin_log` plane reached **12.33 GiB** (capture
    /// on, publish failing, so nothing was ever truncated). Every boot with
    /// `cloud_sync.json` present therefore drove RSS from 0 to 13–15 GiB inside
    /// ~3 minutes and the memory guard SIGKILLed the daemon — three consecutive
    /// cycles on 2026-08-08, after which cloud sync was switched off and the
    /// brain was left with no off-machine backup at all.
    ///
    /// This is the same defect [`KvStore::scan_prefix_keys`] was added for in
    /// 2026-07: `outbox_meta` called `scan_prefix` and pinned every outbox
    /// payload in RAM. That fix landed one caller; this is the other one.
    ///
    /// # Bounds
    ///
    /// Memory is `O(page × record size)`, independent of the plane. Time is
    /// bounded by `row_budget`: a long run of already-published-but-not-yet-
    /// deleted records at the front of the log cannot make one cycle walk the
    /// whole plane. Stopping early is reported, never inferred — see
    /// [`PendingPinLogPage::scan_complete`].
    ///
    /// `want == 0` means "no batch cap" (the test/local-plane callers): the
    /// scan still pages, so the double buffering of raw rows plus decoded
    /// records is gone there too.
    pub(super) async fn read_pending_pin_log_records_paged(
        &self,
        target: &SyncTarget,
        // `Sync` so the returned future stays `Send` — this runs inside the
        // spawned sync-coordinator task.
        is_pending: &(dyn Fn(&PinLogRecord) -> bool + Sync),
        want: usize,
        pending_byte_budget: usize,
        row_budget: usize,
    ) -> Result<PendingPinLogPage, String> {
        // lint:fn-size-ok moved verbatim from pin_log.rs; splitting this function is separate work.
        let target_id = target_id_for_prefix(&target.prefix);
        let prefix = pin_log_target_prefix(&target_id).into_bytes();
        let end = prefix_upper_bound(&prefix).ok_or_else(|| {
            format!(
                "pin log prefix {} has no finite range upper bound",
                String::from_utf8_lossy(&prefix)
            )
        })?;
        let store = self.pin_log_store().await?;
        let page_size = pin_log_scan_page_size();
        let want = if want == 0 { usize::MAX } else { want };

        let mut page = PendingPinLogPage {
            records: Vec::new(),
            not_pending_frontiers: Vec::new(),
            rows_scanned: 0,
            scan_complete: true,
            row_budget_exhausted: false,
        };
        let mut cursor = prefix.clone();
        let mut pending_bytes = 0usize;

        loop {
            let rows = store
                .scan_range_paged(&cursor, &end, page_size)
                .await
                .map_err(|e| format!("scan durable pin log: {e}"))?;
            if rows.is_empty() {
                return Ok(page);
            }
            // A short page means the range is exhausted, which is also the only
            // situation in which a decode failure is a legitimately truncated
            // trailing append rather than corruption with live records behind it.
            let final_page = rows.len() < page_size;
            let last_idx = rows.len() - 1;
            let mut next_cursor = None;

            for (idx, (key, value)) in rows.into_iter().enumerate() {
                page.rows_scanned += 1;
                next_cursor = Some(key_after(&key));
                match serde_json::from_slice::<PinLogRecord>(&value) {
                    Ok(record) => {
                        if is_pending(&record) {
                            if pending_byte_budget > 0
                                && pending_bytes > 0
                                && pending_bytes.saturating_add(value.len()) > pending_byte_budget
                            {
                                page.scan_complete = false;
                                return Ok(page);
                            }
                            pending_bytes = pending_bytes.saturating_add(value.len());
                            page.records.push(record);
                            if page.records.len() >= want {
                                page.scan_complete = false;
                                return Ok(page);
                            }
                        } else {
                            page.not_pending_frontiers.push(record.frontier_after);
                        }
                    }
                    Err(e) if final_page && idx == last_idx => {
                        tracing::warn!(
                            target: "fold_db::sync::pin_log",
                            key = %String::from_utf8_lossy(&key),
                            error = %e,
                            "ignoring corrupt trailing durable pin log record"
                        );
                    }
                    Err(e) => {
                        return Err(format!(
                            "decode durable pin log record {}: {e}",
                            String::from_utf8_lossy(&key)
                        ));
                    }
                }
                if page.rows_scanned >= row_budget {
                    page.scan_complete = false;
                    page.row_budget_exhausted = true;
                    return Ok(page);
                }
            }

            if final_page {
                return Ok(page);
            }
            match next_cursor {
                Some(next) => cursor = next,
                // Defensive: a non-final page always sets the cursor. Returning
                // here rather than looping keeps a backend that violated the
                // ordering contract from spinning forever on the same page.
                None => return Ok(page),
            }
        }
    }

    pub(super) async fn sync_target_by_prefix(
        &self,
        target_prefix: &str,
    ) -> Result<SyncTarget, String> {
        self.targets
            .lock()
            .await
            .iter()
            .find(|target| target.prefix == target_prefix)
            .cloned()
            .ok_or_else(|| format!("sync target prefix '{target_prefix}' is not configured"))
    }

    /// Every atom uuid a still-durable pin-log record references.
    ///
    /// `strip_sot_field_values` drops a record's inline bodies once every
    /// field carries an id, so an unpublished record is only as durable as the
    /// atoms it names. `gc-atoms` builds its reference set from `mk:` (current
    /// head only), `tv:`, `history:`, `conflict:` and `ref:` — none of which
    /// can see this plane. One ordinary update moves the head off a captured
    /// atom, and the next GC pass reads that body as an orphan and frees it.
    /// The record is then unsealable forever, and the upload path quarantines
    /// and drops it — so a write that was acked locally never reaches the
    /// cloud, with no tombstone and no retry.
    ///
    /// Measured on the primary as `quarantined` 21 → 111 in bursts that line
    /// up with GC passes, dominated by the fields a board rewrites most
    /// (`position`, `created_at`, `branch`). Record
    /// `papercut-lastdb-capture-mints-atom-ids-for-fields-the-write-path-never-stores`.
    ///
    /// Scans every target's entries rather than one target's: an atom is
    /// content addressed by `(schema, value)`, so the body one target still
    /// needs can be the body another target already published.
    pub(crate) async fn pending_pin_log_atom_uuids(
        &self,
    ) -> Result<std::collections::HashSet<String>, String> {
        let mut refs = std::collections::HashSet::new();
        let mut after_key = None;
        loop {
            let page = self
                .pending_pin_log_atom_uuids_page(after_key.as_deref(), 4096)
                .await?;
            refs.extend(page.atom_uuids);
            if page.scan_complete {
                return Ok(refs);
            }
            after_key = page.next_after_key;
        }
    }

    /// Read one strict, bounded page of atom references from the durable log.
    ///
    /// Only a field without an inline body depends on its atom. New
    /// self-contained records therefore add no GC roots, while legacy
    /// reference-only rows remain protected until upload removes them.
    pub(crate) async fn pending_pin_log_atom_uuids_page(
        &self,
        after_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<crate::db_operations::AutomaticGcAtomsPinLogReferencePage, String> {
        let store = self.pin_log_store().await?;
        let prefix = PIN_LOG_ENTRY_PREFIX.as_bytes();
        let start = after_key.map_or_else(|| prefix.to_vec(), key_after);
        let end = prefix_upper_bound(prefix)
            .ok_or_else(|| "pin-log entry prefix has no upper bound".to_string())?;
        let limit = limit.max(1);
        let mut rows = store
            .scan_range_paged(&start, &end, limit.saturating_add(1))
            .await
            .map_err(|e| format!("scan pin log page for atom references: {e}"))?;
        let scan_complete = rows.len() <= limit;
        if !scan_complete {
            rows.truncate(limit);
        }
        let last_idx = rows.len().saturating_sub(1);
        let mut refs = std::collections::HashSet::new();
        for (idx, (key, value)) in rows.iter().enumerate() {
            match serde_json::from_slice::<PinLogRecord>(value) {
                Ok(record) => {
                    if let crate::sync::log::LogOp::MutationIntent { mutations } = &record.entry.op
                    {
                        for envelope in mutations {
                            refs.extend(
                                envelope
                                    .field_atom_uuids
                                    .iter()
                                    .filter(|(field, _)| {
                                        !envelope.fields_and_values.contains_key(*field)
                                    })
                                    .map(|(_, uuid)| uuid.clone()),
                            );
                        }
                    }
                }
                Err(error) if scan_complete && idx == last_idx => {
                    tracing::warn!(
                        target: "fold_db::sync::pin_log",
                        key = %String::from_utf8_lossy(key),
                        %error,
                        "ignoring corrupt trailing durable pin log record while collecting atom references"
                    );
                }
                Err(error) => {
                    return Err(format!(
                        "decode durable pin log record {} for atom references: {error}",
                        String::from_utf8_lossy(key)
                    ));
                }
            }
        }
        Ok(crate::db_operations::AutomaticGcAtomsPinLogReferencePage {
            atom_uuids: refs,
            next_after_key: (!scan_complete)
                .then(|| rows.last().map(|(key, _)| key.clone()))
                .flatten(),
            rows_scanned: rows.len() as u64,
            scan_complete,
        })
    }

    pub(super) async fn pin_log_store(
        &self,
    ) -> Result<std::sync::Arc<dyn crate::storage::traits::KvStore>, String> {
        self.store
            .open_namespace(PIN_LOG_NAMESPACE)
            .await
            .map_err(|e| format!("open durable pin log: {e}"))
    }
}

// lint:file-size-ok moved verbatim from pin_log.rs; cohesive unit, split further in a later pass
