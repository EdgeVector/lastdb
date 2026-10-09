// lint:file-size-ok verbatim move out of the 9.8k-line admin_db.rs; one admin theme per file, split further when next touched
//! Tombstone-flag audit/backfill and legacy key-fork audit.

use super::*;

impl AtomStore {
    /// Audit `KeyMetadata.tombstoned` against atom content, optionally stamping
    /// the flag onto legacy records whose content already says tombstone.
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
    // lint:fn-size-ok verbatim move from admin_db.rs; splitting this function is separate work
    pub async fn audit_key_tombstone_flags(
        &self,
        molecules: Option<&HashMap<String, String>>,
        stamp: bool,
        max_keys: Option<usize>,
        after_key: Option<&str>,
        storage_prefix: Option<&str>,
    ) -> Result<TombstoneFlagBackfillReport, SchemaError> {
        use super::super::atom_store::PerKeyRecord;

        /// Atom bodies per batched fetch. Bounds peak memory on a store whose
        /// live set is far larger than the tombstoned one.
        const FETCH_CHUNK: usize = 512;
        /// `mk:` rewrites per batched put.
        const PUT_CHUNK: usize = 1000;
        /// Raw `mk:` rows to fetch per storage page. This bounds one resumed
        /// pass's transient scan allocation independently of store size.
        const SCAN_PAGE: usize = 2048;
        /// Wall-clock budget for one pass, well under the 600s control-socket
        /// read deadline.
        ///
        /// A key-count cap alone cannot bound a pass. The per-row cost here is
        /// an atom body read, which varies by orders of magnitude across homes,
        /// so any count large enough to be useful on a small store is unbounded
        /// in time on a large one. That is exactly how a 200,000-key cap let a
        /// ~150,000-key workload run twelve minutes and trip the deadline the
        /// cap was added to avoid. The bound the caller needs is the one the
        /// socket enforces, so bound on that.
        const PASS_TIME_BUDGET: std::time::Duration = std::time::Duration::from_secs(240);

        // Started before the scan: the scan is part of what the deadline is
        // measuring, so it has to be part of what the budget measures.
        let pass_started = std::time::Instant::now();
        let mk_prefix = build_storage_key(storage_prefix, "mk:");
        let mk_end = FilterUtils::create_prefix_end(&mk_prefix);

        let mut report = TombstoneFlagBackfillReport {
            dry_run: !stamp,
            ..Default::default()
        };
        let mut per_molecule: HashMap<String, MoleculeTombstoneFlagStat> = HashMap::new();
        // (storage key, molecule, record) for rows the flag calls live.
        let mut pending: Vec<(String, String, PerKeyRecord)> = Vec::new();
        let mut to_stamp: Vec<(String, Value)> = Vec::new();
        let mut last_key: Option<String> = None;
        let mut page_start = after_key.map_or_else(|| mk_prefix.clone(), str::to_owned);
        let mut skip_head = after_key.map(str::to_owned);
        let mut range_exhausted = false;

        while !range_exhausted {
            let rows = self
                .raw()
                .inner()
                .scan_range_paged(page_start.as_bytes(), mk_end.as_bytes(), SCAN_PAGE)
                .await
                .map_err(|e| {
                    SchemaError::InvalidData(format!("range scan mk tombstone audit: {e}"))
                })?;
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
                if max_keys.is_some_and(|max| report.audit.keys_scanned as usize >= max) {
                    report.more_remaining = true;
                    stop_after_page = true;
                    break;
                }
                // Same cursor discipline as the count cap: yield before the
                // socket deadline and let the caller resume. Requiring at least
                // one decided row guarantees `next_after_key` advances, so a
                // pass that is slow enough to blow its whole budget on the scan
                // still makes progress rather than resuming forever at the same
                // cursor.
                if report.audit.keys_scanned > 0 && pass_started.elapsed() >= PASS_TIME_BUDGET {
                    report.more_remaining = true;
                    stop_after_page = true;
                    break;
                }
                last_key = Some(key.clone());

                report.audit.keys_scanned += 1;
                let stat = per_molecule.entry(molecule.clone()).or_insert_with(|| {
                    MoleculeTombstoneFlagStat {
                        molecule: molecule.clone(),
                        label: molecules.and_then(|m| m.get(&molecule).cloned()),
                        ..Default::default()
                    }
                });
                stat.keys += 1;

                let Ok(rec) = serde_json::from_slice::<PerKeyRecord>(&v) else {
                    report.audit.keys_unreadable += 1;
                    continue;
                };
                if rec.meta.as_ref().is_some_and(|m| m.tombstoned) {
                    report.audit.meta_tombstoned += 1;
                    stat.meta_tombstoned += 1;
                    continue;
                }
                pending.push((key, molecule, rec));
                if pending.len() >= FETCH_CHUNK {
                    self.classify_tombstone_candidates(
                        &mut pending,
                        &mut report,
                        &mut per_molecule,
                        stamp.then_some(&mut to_stamp),
                        storage_prefix,
                    )
                    .await?;
                    // Drain writes as they accumulate rather than saving them
                    // all for after the loop. Stamping is the expensive half,
                    // and a budget that only covers the decision loop does not
                    // bound the call: the first real run broke out at its 240s
                    // budget and then spent another six minutes writing,
                    // tripping the very deadline the bound exists to stay under.
                    self.drain_tombstone_stamps(&mut to_stamp, &mut report, PUT_CHUNK)
                        .await?;
                }
            }
            skip_head = Some(page_start.clone());
            if stop_after_page {
                break;
            }
        }
        self.classify_tombstone_candidates(
            &mut pending,
            &mut report,
            &mut per_molecule,
            stamp.then_some(&mut to_stamp),
            storage_prefix,
        )
        .await?;

        if stamp {
            self.drain_tombstone_stamps(&mut to_stamp, &mut report, 0)
                .await?;
            let _ = self.flush().await;
        }

        let mut rows: Vec<MoleculeTombstoneFlagStat> = per_molecule.into_values().collect();
        rows.sort_by(|a, b| {
            b.content_tombstoned_meta_false
                .cmp(&a.content_tombstoned_meta_false)
                .then_with(|| b.keys.cmp(&a.keys))
                .then_with(|| a.molecule.cmp(&b.molecule))
        });
        report.audit.per_molecule = rows;
        report.next_after_key = last_key;
        Ok(report)
    }

    /// Discover one bounded page of tombstone-content `mk:` rows by storage
    /// cursor. This deliberately never reconstructs API keys: BlindV1 hashes
    /// are one-way. The returned opaque slots may only be passed to the guarded
    /// storage-slot purge path.
    pub(crate) async fn scan_legacy_tombstone_slots(
        &self,
        molecules: &HashSet<String>,
        max_keys: usize,
        after_key: Option<&str>,
        storage_prefix: Option<&str>,
    ) -> Result<LegacyTombstoneScan, SchemaError> {
        use super::super::atom_store::PerKeyRecord;

        const FETCH_CHUNK: usize = 512;
        const SCAN_PAGE: usize = 2048;
        const PASS_TIME_BUDGET: std::time::Duration = std::time::Duration::from_secs(240);

        let pass_started = std::time::Instant::now();
        let mk_prefix = build_storage_key(storage_prefix, molecule_key_codec::MK_PREFIX);
        let mk_end = FilterUtils::create_prefix_end(&mk_prefix);
        let mut scan = LegacyTombstoneScan {
            keys_scanned: 0,
            keys_unreadable: 0,
            atoms_fetched: 0,
            atoms_missing: 0,
            slots: Vec::new(),
            more_remaining: false,
            next_after_key: None,
        };
        let mut pending: Vec<(String, String, PerKeyRecord)> = Vec::new();
        let mut page_start = after_key.map_or_else(|| mk_prefix.clone(), str::to_owned);
        let mut skip_head = after_key.map(str::to_owned);
        let mut range_exhausted = false;

        while !range_exhausted {
            let rows = self
                .raw()
                .inner()
                .scan_range_paged(page_start.as_bytes(), mk_end.as_bytes(), SCAN_PAGE)
                .await
                .map_err(|e| {
                    SchemaError::InvalidData(format!("range scan legacy tombstones: {e}"))
                })?;
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
                let base_key = key
                    .strip_prefix(&mk_prefix)
                    .map(|suffix| format!("{}{}", molecule_key_codec::MK_PREFIX, suffix));
                let Some(base_key) = base_key else {
                    continue;
                };
                let Some(molecule) = base_key
                    .strip_prefix(molecule_key_codec::MK_PREFIX)
                    .and_then(|rest| rest.split_once(':'))
                    .map(|(molecule, _)| molecule.to_string())
                else {
                    continue;
                };
                if !molecules.contains(&molecule) {
                    continue;
                }
                if scan.keys_scanned as usize >= max_keys
                    || (scan.keys_scanned > 0 && pass_started.elapsed() >= PASS_TIME_BUDGET)
                {
                    scan.more_remaining = true;
                    stop_after_page = true;
                    break;
                }
                scan.keys_scanned += 1;
                scan.next_after_key = Some(key.clone());
                let Ok(record) = serde_json::from_slice::<PerKeyRecord>(&v) else {
                    scan.keys_unreadable += 1;
                    continue;
                };
                pending.push((base_key, molecule, record));
                if pending.len() >= FETCH_CHUNK {
                    self.classify_legacy_tombstone_slots(&mut pending, &mut scan, storage_prefix)
                        .await?;
                }
            }
            skip_head = Some(page_start.clone());
            if stop_after_page {
                break;
            }
        }
        self.classify_legacy_tombstone_slots(&mut pending, &mut scan, storage_prefix)
            .await?;
        Ok(scan)
    }

    pub(super) async fn classify_legacy_tombstone_slots(
        &self,
        pending: &mut Vec<(String, String, super::super::atom_store::PerKeyRecord)>,
        scan: &mut LegacyTombstoneScan,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        if pending.is_empty() {
            return Ok(());
        }
        let atom_slots: Vec<(&str, Option<crate::atom::AtomPartition>)> = pending
            .iter()
            .map(|(_, _, record)| (record.entry.atom_uuid.as_str(), None))
            .collect();
        let atoms = self.get_atoms_located(&atom_slots, storage_prefix).await?;
        scan.atoms_fetched += atoms.len() as u64;
        for ((base_key, molecule, _), atom) in std::mem::take(pending).into_iter().zip(atoms) {
            let Some(atom) = atom else {
                scan.atoms_missing += 1;
                continue;
            };
            if !crate::atom::is_tombstone_value(atom.content()) {
                continue;
            }
            let Some((storage_hash, storage_range)) =
                molecule_key_codec::decode_hash_range(&base_key, &molecule)
            else {
                scan.keys_unreadable += 1;
                continue;
            };
            scan.slots.push(LegacyTombstoneSlot {
                molecule_uuid: molecule,
                storage_hash,
                storage_range,
            });
        }
        Ok(())
    }

    /// Write queued tombstone-flag stamps in `chunk`-sized batches.
    ///
    /// `min_queued` is the backlog below which this is a no-op — pass the chunk
    /// size mid-loop to write only full batches, and `0` at the end of a pass to
    /// flush whatever is left. Stamps are idempotent (they set a flag the atom
    /// content already implies), so writing them progressively rather than in
    /// one trailing burst changes nothing about the result and keeps the pass's
    /// wall-clock bound honest.
    pub(super) async fn drain_tombstone_stamps(
        &self,
        to_stamp: &mut Vec<(String, Value)>,
        report: &mut TombstoneFlagBackfillReport,
        min_queued: usize,
    ) -> Result<(), SchemaError> {
        while to_stamp.len() > min_queued && !to_stamp.is_empty() {
            let take = to_stamp.len().min(min_queued.max(1000));
            let chunk: Vec<(String, Value)> = to_stamp.drain(..take).collect();
            let n = chunk.len() as u64;
            self.raw()
                .batch_put_items(chunk)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("stamp tombstone flag put: {e}")))?;
            report.keys_stamped += n;
        }
        Ok(())
    }

    /// Resolve one batch of flag-says-live keys and classify each by atom
    /// content. Drains `pending`; queues rewrites into `to_stamp` when stamping.
    pub(super) async fn classify_tombstone_candidates(
        &self,
        pending: &mut Vec<(String, String, super::super::atom_store::PerKeyRecord)>,
        report: &mut TombstoneFlagBackfillReport,
        per_molecule: &mut HashMap<String, MoleculeTombstoneFlagStat>,
        mut to_stamp: Option<&mut Vec<(String, Value)>>,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        if pending.is_empty() {
            return Ok(());
        }
        let slots: Vec<(&str, Option<crate::atom::AtomPartition>)> = pending
            .iter()
            .map(|(_, _, rec)| (rec.entry.atom_uuid.as_str(), None))
            .collect();
        let atoms = self.get_atoms_located(&slots, storage_prefix).await?;
        report.audit.atoms_fetched += atoms.len() as u64;

        for ((key, molecule, mut rec), atom) in std::mem::take(pending).into_iter().zip(atoms) {
            let stat =
                per_molecule
                    .entry(molecule.clone())
                    .or_insert_with(|| MoleculeTombstoneFlagStat {
                        molecule,
                        ..Default::default()
                    });
            let Some(atom) = atom else {
                report.audit.atoms_missing += 1;
                stat.atoms_missing += 1;
                continue;
            };
            if !crate::atom::is_tombstone_value(atom.content()) {
                report.audit.live += 1;
                continue;
            }
            report.audit.content_tombstoned_meta_false += 1;
            stat.content_tombstoned_meta_false += 1;
            let Some(queue) = to_stamp.as_mut() else {
                continue;
            };
            rec.meta.get_or_insert_with(Default::default).tombstoned = true;
            let value = serde_json::to_value(&rec)
                .map_err(|e| SchemaError::InvalidData(format!("serialize stamped tip: {e}")))?;
            queue.push((key, value));
        }
        Ok(())
    }

    /// Audit or drain one bounded page of legacy/plain HashKey-encoding tips.
    // lint:fn-size-ok verbatim move from admin_db.rs; splitting this function is separate work
    pub async fn audit_legacy_key_forks(
        &self,
        dry_run: bool,
        max_keys: usize,
        after_key: Option<&str>,
        storage_prefix: Option<&str>,
    ) -> Result<LegacyKeyForkAudit, SchemaError> {
        const SCAN_PAGE: usize = 2048;
        const FETCH_CHUNK: usize = 512;
        const PASS_TIME_BUDGET: std::time::Duration = std::time::Duration::from_secs(240);

        let pass_started = std::time::Instant::now();
        let mk_prefix = build_storage_key(storage_prefix, molecule_key_codec::MK_PREFIX);
        let mk_end = FilterUtils::create_prefix_end(&mk_prefix);
        let mut report = LegacyKeyForkAudit {
            dry_run,
            ..Default::default()
        };
        let mut per_molecule: HashMap<String, LegacyKeyForkMoleculeStat> = HashMap::new();
        let mut pending: Vec<LegacyKeyForkCandidate> = Vec::new();
        let mut page_start = after_key.map_or_else(|| mk_prefix.clone(), str::to_owned);
        let mut skip_head = after_key.map(str::to_owned);
        let mut range_exhausted = false;

        while !range_exhausted {
            let rows = self
                .raw()
                .inner()
                .scan_range_paged(page_start.as_bytes(), mk_end.as_bytes(), SCAN_PAGE)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("scan legacy key forks: {e}")))?;
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
                if report.keys_scanned as usize >= max_keys
                    || (report.keys_scanned > 0 && pass_started.elapsed() >= PASS_TIME_BUDGET)
                {
                    report.more_remaining = true;
                    stop_after_page = true;
                    break;
                }
                report.keys_scanned += 1;
                report.next_after_key = Some(key.clone());

                let Some(suffix) = key.strip_prefix(&mk_prefix) else {
                    report.keys_unreadable += 1;
                    continue;
                };
                let base_key = format!("{}{}", molecule_key_codec::MK_PREFIX, suffix);
                let Some(molecule) = suffix.split_once(':').map(|(m, _)| m.to_string()) else {
                    report.keys_unreadable += 1;
                    continue;
                };
                let stat = per_molecule.entry(molecule.clone()).or_insert_with(|| {
                    LegacyKeyForkMoleculeStat {
                        molecule: molecule.clone(),
                        ..Default::default()
                    }
                });
                stat.keys += 1;
                let Some((hash, range)) =
                    molecule_key_codec::decode_hash_range(&base_key, &molecule)
                else {
                    report.keys_unreadable += 1;
                    continue;
                };
                if hash.is_empty() || self.key_codec().looks_like_storage_hash(&hash) {
                    report.current_form += 1;
                    stat.current_form += 1;
                    continue;
                }
                report.legacy_form += 1;
                stat.legacy_form += 1;
                let twin_hash = self.storage_hash(&molecule, &hash)?;
                let twin_base =
                    molecule_key_codec::hash_range_record_key(&molecule, &twin_hash, &range);
                pending.push(LegacyKeyForkCandidate {
                    legacy_key: key,
                    twin_key: build_storage_key(storage_prefix, &twin_base),
                    molecule,
                    bytes: (raw_key.len() + raw_value.len()) as u64,
                });
                if pending.len() >= FETCH_CHUNK {
                    self.classify_legacy_key_fork_candidates(
                        &mut pending,
                        &mut report,
                        &mut per_molecule,
                        storage_prefix,
                    )
                    .await?;
                }
            }
            skip_head = Some(page_start.clone());
            if stop_after_page {
                break;
            }
        }
        self.classify_legacy_key_fork_candidates(
            &mut pending,
            &mut report,
            &mut per_molecule,
            storage_prefix,
        )
        .await?;
        if !dry_run && report.legacy_tips_deleted > 0 {
            let _ = self.flush().await;
        }
        let mut rows: Vec<_> = per_molecule.into_values().collect();
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

    // lint:fn-size-ok verbatim move from admin_db.rs; splitting this function is separate work
    pub(super) async fn classify_legacy_key_fork_candidates(
        &self,
        pending: &mut Vec<LegacyKeyForkCandidate>,
        report: &mut LegacyKeyForkAudit,
        per_molecule: &mut HashMap<String, LegacyKeyForkMoleculeStat>,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        use super::super::atom_store::PerKeyRecord;

        if pending.is_empty() {
            return Ok(());
        }
        let candidates = std::mem::take(pending);
        let twin_keys: Vec<String> = candidates.iter().map(|c| c.twin_key.clone()).collect();
        let twins = self
            .raw()
            .get_items::<PerKeyRecord>(&twin_keys)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("read legacy fork twins: {e}")))?;
        let mut forked = Vec::new();
        for (candidate, twin) in candidates.into_iter().zip(twins) {
            let stat = per_molecule
                .get_mut(&candidate.molecule)
                .expect("candidate molecule was counted");
            let Some(twin) = twin else {
                report.legacy_only += 1;
                stat.legacy_only += 1;
                continue;
            };
            report.forked += 1;
            stat.forked += 1;
            forked.push((candidate, twin));
        }
        if forked.is_empty() {
            return Ok(());
        }
        let atom_slots: Vec<_> = forked
            .iter()
            .map(|(_, twin)| (twin.entry.atom_uuid.as_str(), None))
            .collect();
        let atoms = self.get_atoms_located(&atom_slots, storage_prefix).await?;
        let mut eligible: HashMap<String, Vec<LegacyKeyForkCandidate>> = HashMap::new();
        for ((candidate, _), atom) in forked.into_iter().zip(atoms) {
            let stat = per_molecule
                .get_mut(&candidate.molecule)
                .expect("candidate molecule was counted");
            match atom {
                None => {
                    report.twin_atoms_missing += 1;
                    stat.twin_atoms_missing += 1;
                }
                Some(atom) if crate::atom::is_tombstone_value(atom.content()) => {
                    report.twin_atoms_tombstoned += 1;
                    stat.twin_atoms_tombstoned += 1;
                }
                Some(_) => {
                    report.twin_atoms_live += 1;
                    stat.twin_atoms_live += 1;
                    // Dry-run must still report the reclaim it already measured.
                    // Eligible is only queued under execute; without this arm
                    // `bytes_freed_approx` stays structurally pinned at 0.
                    if report.dry_run {
                        report.bytes_freed_approx += candidate.bytes;
                    } else {
                        eligible
                            .entry(candidate.molecule.clone())
                            .or_default()
                            .push(candidate);
                    }
                }
            }
        }

        for (molecule, candidates) in eligible {
            let _guard = self.lock_molecule_commit(&molecule, storage_prefix).await;
            let twin_keys: Vec<_> = candidates.iter().map(|c| c.twin_key.clone()).collect();
            let twins = self
                .raw()
                .get_items::<PerKeyRecord>(&twin_keys)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("recheck legacy fork twins: {e}")))?;
            let present: Vec<_> = candidates
                .into_iter()
                .zip(twins)
                .filter_map(|(candidate, twin)| twin.map(|twin| (candidate, twin)))
                .collect();
            let atom_slots: Vec<_> = present
                .iter()
                .map(|(_, twin)| (twin.entry.atom_uuid.as_str(), None))
                .collect();
            let atoms = self.get_atoms_located(&atom_slots, storage_prefix).await?;
            let mut delete = Vec::new();
            let mut bytes = 0u64;
            for ((candidate, _), atom) in present.into_iter().zip(atoms) {
                if atom
                    .as_ref()
                    .is_some_and(|atom| !crate::atom::is_tombstone_value(atom.content()))
                {
                    bytes += candidate.bytes;
                    delete.push(candidate.legacy_key);
                }
            }
            if !delete.is_empty() {
                self.raw()
                    .batch_delete_keys(delete.clone())
                    .await
                    .map_err(|e| {
                        SchemaError::InvalidData(format!("delete legacy fork tips: {e}"))
                    })?;
                let deleted = delete.len() as u64;
                report.legacy_tips_deleted += deleted;
                report.bytes_freed_approx += bytes;
                per_molecule
                    .get_mut(&molecule)
                    .expect("candidate molecule was counted")
                    .legacy_tips_deleted += deleted;
            }
        }
        Ok(())
    }
}
