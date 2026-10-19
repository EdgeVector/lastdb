//! Legacy key-fork audit pass.

use super::*;

impl AtomStore {
    /// Audit one bounded page of legacy/plain HashKey-encoding tips. A drain
    /// (`dry_run = false`) is refused: only the atom counter planner writes tip
    /// records.
    pub async fn audit_legacy_key_forks(
        &self,
        dry_run: bool,
        max_keys: usize,
        after_key: Option<&str>,
        storage_prefix: Option<&str>,
    ) -> Result<LegacyKeyForkAudit, SchemaError> {
        if !dry_run {
            return Err(SchemaError::InvalidData(
                "legacy key-fork drain was removed: only the atom counter planner writes tip records"
                    .into(),
            ));
        }
        let pass_started = std::time::Instant::now();
        let mk_prefix = build_storage_key(storage_prefix, molecule_key_codec::MK_PREFIX);
        let mk_end = FilterUtils::create_prefix_end(&mk_prefix);
        let mut state = KeyForkAuditState {
            report: LegacyKeyForkAudit {
                dry_run,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut page_start = after_key.map_or_else(|| mk_prefix.clone(), str::to_owned);
        let mut skip_head = after_key.map(str::to_owned);
        let mut range_exhausted = false;

        while !range_exhausted {
            let rows = self
                .scan_mk_page(&page_start, &mk_end, "scan legacy key forks")
                .await?;
            if rows.len() < SCAN_PAGE {
                range_exhausted = true;
            }
            if rows.is_empty() {
                break;
            }
            let head = skip_head.take();
            let mut stop_after_page = false;
            for (raw_key, raw_value) in rows {
                let key = String::from_utf8_lossy(&raw_key).into_owned();
                page_start = key.clone();
                if head.as_deref() == Some(key.as_str()) {
                    continue;
                }
                if state.report.keys_scanned as usize >= max_keys
                    || (state.report.keys_scanned > 0 && pass_started.elapsed() >= PASS_TIME_BUDGET)
                {
                    state.report.more_remaining = true;
                    stop_after_page = true;
                    break;
                }
                let bytes = (raw_key.len() + raw_value.len()) as u64;
                self.audit_key_fork_row(&mut state, key, &mk_prefix, bytes, storage_prefix)
                    .await?;
            }
            skip_head = Some(page_start.clone());
            if stop_after_page {
                break;
            }
        }
        self.classify_legacy_key_fork_candidates(
            &mut state.pending,
            &mut state.report,
            &mut state.per_molecule,
            storage_prefix,
        )
        .await?;
        let mut report = state.report;
        if !dry_run && report.legacy_tips_deleted > 0 {
            let _ = self.flush().await;
        }
        let mut rows: Vec<_> = state.per_molecule.into_values().collect();
        rows.sort_by(|a, b| {
            b.forked
                .cmp(&a.forked)
                .then_with(|| a.molecule.cmp(&b.molecule))
        });
        report.per_molecule = rows;
        if range_exhausted && !report.more_remaining {
            report.next_after_key = None;
        }
        Ok(report)
    }

    /// Count one `mk:` row and queue it as a fork candidate when it is in the
    /// legacy key form. Classifies the batch once it fills.
    async fn audit_key_fork_row(
        &self,
        state: &mut KeyForkAuditState,
        key: String,
        mk_prefix: &str,
        bytes: u64,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let report = &mut state.report;
        report.keys_scanned += 1;
        report.next_after_key = Some(key.clone());

        let Some(suffix) = key.strip_prefix(mk_prefix) else {
            report.keys_unreadable += 1;
            return Ok(());
        };
        let base_key = format!("{}{}", molecule_key_codec::MK_PREFIX, suffix);
        let Some(molecule) = suffix.split_once(':').map(|(m, _)| m.to_string()) else {
            report.keys_unreadable += 1;
            return Ok(());
        };
        let stat = state
            .per_molecule
            .entry(molecule.clone())
            .or_insert_with(|| LegacyKeyForkMoleculeStat {
                molecule: molecule.clone(),
                ..Default::default()
            });
        stat.keys += 1;
        let Some((hash, range)) = molecule_key_codec::decode_hash_range(&base_key, &molecule)
        else {
            report.keys_unreadable += 1;
            return Ok(());
        };
        if hash.is_empty() || self.key_codec().looks_like_storage_hash(&hash) {
            report.current_form += 1;
            stat.current_form += 1;
            return Ok(());
        }
        report.legacy_form += 1;
        stat.legacy_form += 1;
        let twin_hash = self.storage_hash(&molecule, &hash)?;
        let twin_base = molecule_key_codec::hash_range_record_key(&molecule, &twin_hash, &range);
        state.pending.push(LegacyKeyForkCandidate {
            twin_key: build_storage_key(storage_prefix, &twin_base),
            molecule,
            bytes,
        });
        if state.pending.len() >= FETCH_CHUNK {
            self.classify_legacy_key_fork_candidates(
                &mut state.pending,
                &mut state.report,
                &mut state.per_molecule,
                storage_prefix,
            )
            .await?;
        }
        Ok(())
    }
}
