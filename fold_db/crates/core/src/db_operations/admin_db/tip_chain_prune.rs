//! Tip version-chain prune planning and the CAS-gated chain prune.

use super::*;

impl AtomStore {
    /// Plan (and optionally apply) tip-version chain pruning.
    ///
    /// Soft-delete leaves `mk:` heads as tombstones but keeps the full
    /// `prev_tip_id` → `tv:` chain, which pins every historical body atom so
    /// plain orphan GC cannot free them. For eligible tips we drop the chain
    /// (`tv:` nodes) and clear `prev_tip_id` on the head, then orphan GC can
    /// delete the unpinned atoms.
    ///
    /// * `include_live = false` (default): only **tombstoned** tips — live tips
    ///   keep full tip-version history for `as_of`.
    /// * `include_live = true`: **every** tip with a non-empty `prev_tip_id`
    ///   chain is pruned to tip-only. This drops `as_of` history but is the
    ///   reclaim path when append-heavy live records pin multi‑hundred‑MB of
    ///   old body atoms (measured: `fbrain/Reference` tip history).
    ///
    /// ONE PASS IS BOUNDED, and an interrupted one resumes. This used to plan
    /// the whole `mk:` plane into a vector and only then apply it. Measured
    /// 2026-08-19 on a copy-on-write clone of a 9.6 GiB primary: `gc-atoms
    /// --execute` ran 21 minutes, spent the whole budget in here, returned 0
    /// bytes and wrote 0 `gc-atoms` ledger rows. Nothing carried across
    /// attempts, so raising the deadline never changed the shape — the verb was
    /// non-convergent, not slow. Now each pass takes
    /// [`Self::GC_PRUNE_PASS_MAX_KEYS`] / [`Self::GC_PRUNE_PASS_TIME_BUDGET`],
    /// applies each prune as it is decided (so the work banks as it goes), and
    /// reports `next_after_key` for the caller to persist.
    ///
    /// An earlier version of this comment also said the `tips` plane "GREW 2.99
    /// → 3.33 GiB from the head rewrites" during that pass. **That claim is
    /// withdrawn**: sampling the same figure on an idle primary with no verb
    /// running at all shows `allocated` swinging 8.66 → 9.34 GiB, and `tips`
    /// 3.42 → 3.69 GiB, inside fourteen minutes, while `in records` moves 0.02
    /// GiB. A plane size is a point in a compaction cycle, so two readings
    /// minutes apart cannot attribute anything to a verb. The rewrites this
    /// function used to do were still pointless work; the plane figure was
    /// simply never the evidence for it. Attribute reclaim with the delete
    /// ledger row and `bytes_freed_approx`, which name the verb that acted.
    // lint:fn-size-ok verbatim move from admin_db.rs; splitting this function is separate work
    pub(super) async fn prune_tip_version_chains(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
        include_live: bool,
        resume_at: Option<&PhysicalScanCursor>,
        max_keys: usize,
    ) -> Result<TipVersionPrunePlan, SchemaError> {
        use crate::atom::molecule_key_codec;
        use crate::db_operations::atom_store::PerKeyRecord;

        let mut plan = TipVersionPrunePlan::default();
        let mk_prefix = build_storage_key(storage_prefix, "mk:");
        let pass_started = std::time::Instant::now();
        let mut progress = GcAtomsProgress::start("tip-chain-prune", dry_run);
        // Floor at one. A budget of zero would yield before deciding any key,
        // which is an unbounded run of passes that each make no progress.
        let max_keys = max_keys.max(1);
        let time_budget = Self::gc_prune_pass_time_budget();

        // Full storage keys of tv: rows we will drop — DRY-RUN ONLY, so
        // collect_referenced can ignore them without applying deletes first.
        // On execute each chain is deleted before the reference scan runs, so
        // the set stays empty and the pass carries no per-key residue at all.
        let mut tv_keys_skip: HashSet<String> = HashSet::new();

        let mk_end = FilterUtils::create_prefix_end(&mk_prefix);
        // A logical `mk:` page is row-bounded but not physically bounded: its
        // k-way merge resolves every tips group before it can return the first
        // row. The primary measured 281-350 seconds for that first request.
        // Visit one physical handle instead. The checkpoint carries the handle
        // and the exclusive key inside it, so the next call does not repeat the
        // all-group merge.
        let physical_page = self
            .raw()
            .inner()
            .scan_range_physical_paged(
                mk_prefix.as_bytes(),
                mk_end.as_bytes(),
                resume_at,
                max_keys.min(Self::GC_PLANE_SCAN_PAGE),
                1,
            )
            .await
            .map_err(|e| SchemaError::InvalidData(format!("gc physical scan mk (prune): {e}")))?;
        plan.physical_handles_visited = physical_page.handles_visited;
        plan.physical_cold_shard_loads = physical_page.cold_shard_loads;
        let row_handle = physical_page.row_handle.clone();
        let page_next_cursor = physical_page.next_cursor.clone();
        let mut budget_spent = false;
        for (k, v) in physical_page.rows {
            // Budget is checked BEFORE the row is decided, so
            // `next_after_key` never names a key this pass left half-done:
            // it is always the last key the pass carried all the way
            // through, and the next pass re-reads from there.
            if plan.keys_scanned as usize >= max_keys
                || (plan.keys_scanned > 0 && pass_started.elapsed() >= time_budget)
            {
                budget_spent = true;
                break;
            }
            let mk_key = String::from_utf8_lossy(&k).into_owned();
            plan.keys_scanned += 1;
            progress.walked(1);
            plan.next_after_key = Some(mk_key.clone());
            let Ok(rec) = serde_json::from_slice::<PerKeyRecord>(&v) else {
                continue;
            };
            let tombstoned = rec.meta.as_ref().is_some_and(|m| m.tombstoned);
            // Default: tombstoned tips only. With include_live: any tip that
            // still carries a history chain.
            let eligible = if include_live {
                !rec.entry.prev_tip_id.is_empty()
            } else {
                tombstoned && !rec.entry.prev_tip_id.is_empty()
            };
            if !eligible {
                continue;
            }

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

            if dry_run {
                plan.tips_chain_cleared += 1;
                plan.tip_versions_pruned += tv_keys.len() as u64;
                plan.tip_version_bytes_approx += chain_bytes;
                tv_keys_skip.extend(tv_keys);
                continue;
            }

            // Applied HERE, one key at a time, instead of from a plan
            // vector after the scan. Two reasons, and the first is the one
            // that made this verb useless: a pass that runs out of deadline
            // now keeps every chain it already pruned, so successive passes
            // converge instead of each replanning the plane from zero. The
            // second is the race guard in `apply_tip_chain_prune` — it
            // re-reads the head and refuses if a writer moved it, and
            // deciding a key immediately after reading it narrows that
            // window from the length of a full `mk:` scan to one row.
            // A chain skipped here is still prunable on a later pass.
            if !self.apply_tip_chain_prune(&mk_key, &rec, &tv_keys).await? {
                plan.tips_skipped_changed += 1;
                continue;
            }
            plan.tips_chain_cleared += 1;
            plan.tip_versions_pruned += tv_keys.len() as u64;
            plan.tip_version_bytes_approx += chain_bytes;
        }

        if !dry_run && plan.tips_chain_cleared > 0 {
            let _ = self.flush().await;
        }
        progress.finish();

        plan.next_physical_cursor = if budget_spent {
            // The physical page may contain rows past the time budget. Resume
            // in the same handle after the last row this pass fully decided.
            row_handle.map(|mut cursor| {
                cursor.after_key = plan
                    .next_after_key
                    .as_ref()
                    .map(|key| key.as_bytes().to_vec());
                cursor
            })
        } else {
            page_next_cursor
        };
        plan.more_remaining = plan.next_physical_cursor.is_some();
        if !plan.more_remaining {
            plan.next_after_key = None;
        }
        plan.tv_keys_skip = tv_keys_skip;
        Ok(plan)
    }

    /// `mk:` tips one prologue pass will look at before yielding.
    /// Override with `LASTDB_GC_PRUNE_PASS_MAX_KEYS`.
    pub(super) fn gc_prune_pass_max_keys() -> usize {
        Self::positive_env_usize(GC_PRUNE_PASS_MAX_KEYS_ENV, 200_000)
    }

    /// Wall clock one prologue pass may spend before yielding. Deliberately a
    /// small slice of the operator deadline: the prologue frees nothing by
    /// itself — it only UNPINS body atoms — so a pass that spends its whole
    /// budget here banks no bytes. The rest of the deadline belongs to the
    /// reference scan and the atom walk, which are what return storage.
    /// Override with `LASTDB_GC_PRUNE_PASS_SECS`.
    pub(super) fn gc_prune_pass_time_budget() -> std::time::Duration {
        std::time::Duration::from_secs(Self::positive_env_usize(GC_PRUNE_PASS_SECS_ENV, 60) as u64)
    }

    /// Read a positive integer knob, ignoring absent/unparseable/zero values.
    /// Zero is rejected rather than honoured: a budget of 0 would yield before
    /// deciding any key, which is an infinite loop of no-progress passes.
    pub(super) fn positive_env_usize(name: &str, default: usize) -> usize {
        env_flag::var_parsed(name)
            .filter(|v: &usize| *v > 0)
            .unwrap_or(default)
    }

    /// Decode a full `mk:` key, with or without a storage-prefix head.
    pub(super) fn molecule_slot_from_storage_key(key: &str) -> Option<(&str, String, String)> {
        let base_at = key.rfind(molecule_key_codec::MK_PREFIX)?;
        let base_key = &key[base_at..];
        let molecule_uuid = molecule_key_codec::molecule_uuid_from_storage_key(base_key)?;
        let (disk_hash, disk_range) =
            molecule_key_codec::decode_hash_range(base_key, molecule_uuid)?;
        Some((molecule_uuid, disk_hash, disk_range))
    }

    /// Re-validate one planned tip-chain prune against the live store and apply
    /// it only if the head has not moved since planning. Returns whether it was
    /// applied; `false` means a concurrent writer touched the key and the whole
    /// prune for it (chain delete AND head rewrite) was abandoned.
    ///
    /// The delete and the rewrite are deliberately gated by ONE check and share
    /// its outcome. Splitting them — deleting the chain but skipping the rewrite,
    /// or the reverse — leaves a head whose `prev_tip_id` points at a `tv:` row
    /// that no longer exists, which is a dangling chain rather than a race the
    /// next run can retry.
    pub(crate) async fn apply_tip_chain_prune(
        &self,
        mk_key: &str,
        observed: &super::super::atom_store::PerKeyRecord,
        tv_keys: &[String],
    ) -> Result<bool, SchemaError> {
        use super::super::atom_store::PerKeyRecord;
        const CHUNK: usize = 2000;

        let current: Option<PerKeyRecord> = self
            .raw()
            .get_item(mk_key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("gc re-read mk tip: {e}")))?;
        if !head_is_unchanged(current.as_ref(), observed) {
            return Ok(false);
        }

        let (molecule_uuid, disk_hash, disk_range) = Self::molecule_slot_from_storage_key(mk_key)
            .ok_or_else(|| {
            SchemaError::InvalidData(format!("gc cannot decode mk slot: {mk_key}"))
        })?;

        for chunk in tv_keys.chunks(CHUNK) {
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
                .map_err(|e| SchemaError::InvalidData(format!("gc delete tip versions: {e}")))?;
        }
        // Write back the record just re-read and confirmed current, not the
        // planning scan's copy, so no field observed at scan time can clobber a
        // newer one. `current` is Some here — `head_is_unchanged` rejects None.
        let mut rec = current.unwrap_or_else(|| observed.clone());
        rec.entry.prev_tip_id.clear();
        self.raw()
            .put_item(mk_key, &rec)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("gc rewrite mk tip: {e}")))?;
        Ok(true)
    }
}
