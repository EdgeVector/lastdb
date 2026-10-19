//! Tombstone-flag audit pass over `mk:` records.

use super::super::super::atom_store::PerKeyRecord;
use super::*;

impl AtomStore {
    /// Audit `KeyMetadata.tombstoned` against atom content. A `stamp` request
    /// is refused: only the atom counter planner writes tip records.
    ///
    /// `molecules` restricts the walk to the given molecule uuids and labels the
    /// per-molecule rows; `None` audits every `mk:` record in the store.
    ///
    /// **Cost.** Deciding a row needs its atom body, so this reads every body
    /// under the selected molecules — a full scan by construction, which is why
    /// it is an owner tool and not something a read path calls. Bodies are
    /// fetched in bounded batches through the same located-atom ladder the read
    /// path uses, so a partition-prefixed body resolves without a hint.
    ///
    /// **Resumability** is by idempotence rather than a checkpoint: stamping
    /// only ever sets a flag that content already implies, writes land in
    /// chunks, and a re-run re-audits and stamps whatever a previous run did not
    /// reach. Interrupting it can only leave less work done, never wrong data.
    ///
    /// **Bounding.** `max_keys` caps how many selected `mk:` records one call
    /// decides; `after_key` resumes past the last key a previous call reported.
    /// Without a cap, a whole-store pass on a real home runs for minutes and
    /// exceeds the control socket's read deadline — the client then reports a
    /// failure for work the daemon is still doing, which for a `stamp` pass
    /// leaves the operator unable to say whether anything was written. A bounded
    /// call always answers.
    ///
    /// A `key > after_key` cursor is only a cursor if the walk is ordered. Use
    /// range paging over the ordered storage keyspace: it keeps the cursor
    /// meaningful without materializing and sorting the whole `mk:` prefix on
    /// every resumed pass.
    pub async fn audit_key_tombstone_flags(
        &self,
        molecules: Option<&HashMap<String, String>>,
        stamp: bool,
        max_keys: Option<usize>,
        after_key: Option<&str>,
        storage_prefix: Option<&str>,
    ) -> Result<TombstoneFlagBackfillReport, SchemaError> {
        if stamp {
            return Err(SchemaError::InvalidData(
                "tombstone-flag-audit stamping was removed: only the atom counter planner writes tip records"
                    .into(),
            ));
        }
        // Started before the scan: the scan is part of what the deadline is
        // measuring, so it has to be part of what the budget measures.
        let pass_started = std::time::Instant::now();
        let mk_prefix = build_storage_key(storage_prefix, "mk:");
        let mk_end = FilterUtils::create_prefix_end(&mk_prefix);

        let mut state = TombstoneAuditState {
            report: TombstoneFlagBackfillReport {
                dry_run: !stamp,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut page_start = after_key.map_or_else(|| mk_prefix.clone(), str::to_owned);
        let mut skip_head = after_key.map(str::to_owned);
        let mut range_exhausted = false;

        while !range_exhausted {
            let rows = self
                .scan_mk_page(&page_start, &mk_end, "range scan mk tombstone audit")
                .await?;
            if rows.len() < SCAN_PAGE {
                range_exhausted = true;
            }
            if rows.is_empty() {
                break;
            }

            let head = skip_head.take();
            let mut stop_after_page = false;
            for (k, v) in rows {
                let key = String::from_utf8_lossy(&k).into_owned();
                page_start = key.clone();
                if head.as_deref() == Some(key.as_str()) {
                    continue;
                }
                let Some(molecule) = key
                    .strip_prefix(mk_prefix.as_str())
                    .and_then(|rest| rest.split_once(':'))
                    .map(|(molecule, _)| molecule.to_string())
                else {
                    continue;
                };
                if molecules.is_some_and(|m| !m.contains_key(&molecule)) {
                    continue;
                }
                let over_count =
                    max_keys.is_some_and(|max| state.report.audit.keys_scanned as usize >= max);
                // Same cursor discipline as the count cap: yield before the
                // socket deadline and let the caller resume. Requiring at least
                // one decided row guarantees `next_after_key` advances, so a
                // pass that is slow enough to blow its whole budget on the scan
                // still makes progress rather than resuming forever at the same
                // cursor.
                let over_time = state.report.audit.keys_scanned > 0
                    && pass_started.elapsed() >= PASS_TIME_BUDGET;
                if over_count || over_time {
                    state.report.more_remaining = true;
                    stop_after_page = true;
                    break;
                }
                self.audit_tombstone_row(
                    &mut state,
                    (key, molecule, &v),
                    molecules,
                    storage_prefix,
                )
                .await?;
            }
            skip_head = Some(page_start.clone());
            if stop_after_page {
                break;
            }
        }
        self.finish_tombstone_audit(state, storage_prefix).await
    }
    /// Count one `mk:` row into the audit and queue it for classification when
    /// its flag says live. Classifies and drains writes once a batch fills.
    async fn audit_tombstone_row(
        &self,
        state: &mut TombstoneAuditState,
        (key, molecule, value): (String, String, &[u8]),
        molecules: Option<&HashMap<String, String>>,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        state.last_key = Some(key.clone());
        state.report.audit.keys_scanned += 1;
        let stat = state
            .per_molecule
            .entry(molecule.clone())
            .or_insert_with(|| MoleculeTombstoneFlagStat {
                molecule: molecule.clone(),
                label: molecules.and_then(|m| m.get(&molecule).cloned()),
                ..Default::default()
            });
        stat.keys += 1;

        let Ok(rec) = serde_json::from_slice::<PerKeyRecord>(value) else {
            state.report.audit.keys_unreadable += 1;
            return Ok(());
        };
        if rec.meta.as_ref().is_some_and(|m| m.tombstoned) {
            state.report.audit.meta_tombstoned += 1;
            stat.meta_tombstoned += 1;
            return Ok(());
        }
        state.pending.push((key, molecule, rec));
        if state.pending.len() >= FETCH_CHUNK {
            self.classify_pending_tombstones(state, storage_prefix)
                .await?;
        }
        Ok(())
    }

    async fn classify_pending_tombstones(
        &self,
        state: &mut TombstoneAuditState,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        self.classify_tombstone_candidates(
            &mut state.pending,
            &mut state.report,
            &mut state.per_molecule,
            storage_prefix,
        )
        .await
    }

    /// Classify the last partial batch and sort the
    /// per-molecule rows into the report.
    async fn finish_tombstone_audit(
        &self,
        mut state: TombstoneAuditState,
        storage_prefix: Option<&str>,
    ) -> Result<TombstoneFlagBackfillReport, SchemaError> {
        self.classify_pending_tombstones(&mut state, storage_prefix)
            .await?;

        let mut rows: Vec<MoleculeTombstoneFlagStat> = state.per_molecule.into_values().collect();
        rows.sort_by(|a, b| {
            b.content_tombstoned_meta_false
                .cmp(&a.content_tombstoned_meta_false)
                .then_with(|| b.keys.cmp(&a.keys))
                .then_with(|| a.molecule.cmp(&b.molecule))
        });
        let mut report = state.report;
        report.audit.per_molecule = rows;
        report.next_after_key = state.last_key;
        Ok(report)
    }
}
