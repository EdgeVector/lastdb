// lint:file-size-ok verbatim move out of the 9.8k-line admin_db.rs; one admin theme per file, split further when next touched
//! Live byte inventory, tip-format scan, thin-tip migration and molecule key listings.

use super::*;

impl AtomStore {
    /// Fold every row under `prefix` into `(keys, live key+value bytes)` while
    /// holding at most one bounded page.
    ///
    /// The totals are exactly what a whole-prefix scan produced; this changes
    /// only how they are reached, not what they say.
    pub(super) async fn inventory_accumulate_prefix(
        &self,
        prefix: &str,
        what: &str,
    ) -> Result<(u64, u64), SchemaError> {
        let end = FilterUtils::create_prefix_end(prefix);
        let mut keys = 0u64;
        let mut bytes = 0u64;
        // `scan_range_paged`'s start bound is inclusive, so every page after the
        // first re-reads the previous page's last row. It is skipped by key
        // rather than by position: if that row is deleted between pages, the row
        // that takes its place must still be counted.
        let mut cursor: Option<Vec<u8>> = None;
        let mut page_rows = INVENTORY_PAGE_ROWS_MIN;
        loop {
            let start = cursor.as_deref().unwrap_or(prefix.as_bytes());
            let rows = self
                .raw()
                .inner()
                .scan_range_paged(start, end.as_bytes(), page_rows)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("scan {what}: {e}")))?;
            if rows.is_empty() {
                break;
            }
            // A short page means the range is drained. A full page always holds
            // at least `INVENTORY_PAGE_ROWS_MIN` distinct ascending keys, so the
            // cursor strictly advances and the loop terminates.
            let exhausted = rows.len() < page_rows;
            let mut page_bytes = 0u64;
            for (k, v) in &rows {
                let row_bytes = k.len() as u64 + v.len() as u64;
                page_bytes += row_bytes;
                if cursor.as_deref() == Some(k.as_slice()) {
                    continue;
                }
                keys += 1;
                bytes += row_bytes;
            }
            if exhausted {
                break;
            }
            page_rows = inventory_next_page_rows(page_bytes, rows.len());
            cursor = rows.last().map(|(k, _)| k.clone());
        }
        Ok((keys, bytes))
    }

    /// Scan `main` by known key prefixes and sum live key+value sizes.
    // lint:fn-size-ok verbatim move from admin_db.rs; splitting this function is separate work
    pub async fn inventory_main_key_classes(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<MainKeyClass>, SchemaError> {
        // Prefixes must be ordered longest-first for classification; we scan
        // each prefix independently (they don't nest).
        let classes: &[(&str, &str, &str)] = &[
            ("atom_content", "atom:", "atom:{uuid} — field VALUES"),
            (
                "legacy_schema_secondary_index",
                "schemaidx:",
                "schemaidx:{len}:{schema}:{atom} — legacy main-tree marker",
            ),
            (
                "mutation_history",
                "history:",
                "history:{molecule}:{ts} — per-field mutation log",
            ),
            (
                "molecule_per_key_index",
                "mk:",
                "mk:{M}:… — current tip pointer per record key",
            ),
            (
                "tip_version_chain",
                "tv:",
                "tv:{id} — archived tip versions (prev_tip_id chain for as_of)",
            ),
            (
                "molecule_update_order_log",
                "mord:",
                "mord:{M}:{seq} — append-only update order",
            ),
            (
                "molecule_hash_range_page_index",
                "mhr:",
                "mhr:{M}\\0… — HashRange page index",
            ),
            (
                "molecule_legacy_blob",
                "ref:",
                "ref:{M} — pre per-key whole-molecule blob",
            ),
            ("molecule_header", "mh:", "mh:{M} — molecule header"),
            (
                "molecule_update_order_count",
                "moc:",
                "moc:{M} — order log count",
            ),
            (
                "molecule_hash_range_index_complete",
                "mhi:",
                "mhi:v2:{M} — page index complete marker",
            ),
            (
                "molecule_update_order_legacy",
                "mo:",
                "mo:{M} — legacy single-record order",
            ),
            (
                "conflict",
                "conflict:",
                "conflict:… — merge conflict records",
            ),
            (
                "legacy_schema_index_sentinel",
                "schemaidx_v1_done",
                "schemaidx_v1_done legacy main-tree sentinel",
            ),
            // Protein membership. Absent from this table until 2026-07-30, which
            // is why an orphan-protein leak grew unnoticed: each prefix is
            // scanned independently and there is no catch-all bucket, so an
            // unlisted key class is not merely mis-labelled — it is invisible,
            // and `lastdb db inventory` under-reports the main tree by its size.
            (
                "protein_record",
                "protein:",
                "protein:{uuid} — protein record (member list). Nothing deletes these; empty ones are orphans.",
            ),
            (
                "protein_member_backref",
                "molprot:",
                "molprot:{M} → protein uuid — molecule's binding back-ref",
            ),
            (
                "protein_fold_state",
                "fldprot:",
                "fldprot:{field_hash} — per-field protein fold state",
            ),
            (
                "protein_fold_queue",
                "pfq:",
                "pfq:{job_id} — pending sibling-tip fold job",
            ),
            // Listed so the audit trail's own footprint stays visible. Nothing
            // reaps these by design (see `db_operations::delete_ledger`), so an
            // unbounded class that is NOT reported would be exactly the kind of
            // invisible growth the protein-record entry above was added for.
            (
                "atom_delete_ledger",
                super::super::delete_ledger::ATOM_DELETE_LEDGER_PREFIX,
                "dellog:{ts}:{uuid} — hard-delete audit row (purge / gc-atoms). Never auto-reaped.",
            ),
        ];

        let mut out = Vec::with_capacity(classes.len());
        for (class, bare_prefix, label) in classes {
            let prefix = build_storage_key(storage_prefix, bare_prefix);
            let (keys, bytes) = self.inventory_accumulate_prefix(&prefix, class).await?;
            if keys > 0 {
                out.push(MainKeyClass {
                    class: (*class).to_string(),
                    prefix: (*label).to_string(),
                    keys,
                    bytes,
                });
            }
        }
        out.sort_by_key(|b| std::cmp::Reverse(b.bytes));
        Ok(out)
    }

    /// Build a full inventory: main key classes + per-schema atom sizes + history stats.
    // lint:fn-size-ok verbatim move from admin_db.rs; splitting this function is separate work
    pub async fn db_inventory(
        &self,
        schema_names: &[String],
        storage_prefix: Option<&str>,
        // field molecule uuids per schema: (schema, Vec<field_mol_uuid>)
        schema_field_molecules: &[(String, Vec<String>)],
    ) -> Result<DbInventory, SchemaError> {
        let main_classes = self.inventory_main_key_classes(storage_prefix).await?;
        let main_total_keys = main_classes.iter().map(|c| c.keys).sum();
        let main_total_bytes = main_classes.iter().map(|c| c.bytes).sum();

        let atoms: StorageBreakdown = self.storage_breakdown(schema_names, storage_prefix).await?;

        let mut per_schema_history = Vec::new();
        let mut per_schema_order_log = Vec::new();
        for (schema_name, mols) in schema_field_molecules {
            let mut events = 0u64;
            let mut bytes = 0u64;
            let mut order_log_entries = 0u64;
            let mut order_log_bytes = 0u64;
            let mut order_count_keys = 0u64;
            let mut order_count_bytes = 0u64;
            for mol in mols {
                let base = crate::atom::molecule_key_codec::history_molecule_prefix(mol);
                let prefix = build_storage_key(storage_prefix, &base);
                let (mol_events, mol_bytes) = self
                    .inventory_accumulate_prefix(&prefix, &format!("history {schema_name}"))
                    .await?;
                events += mol_events;
                bytes += mol_bytes;

                let order_base = format!("mord:{mol}:");
                let order_prefix = build_storage_key(storage_prefix, &order_base);
                let (mol_order_entries, mol_order_bytes) = self
                    .inventory_accumulate_prefix(&order_prefix, &format!("order-log {schema_name}"))
                    .await?;
                order_log_entries += mol_order_entries;
                order_log_bytes += mol_order_bytes;

                // `moc:{mol}` is a single key, and the prefix scan this replaces
                // discarded every row but the exact match — so a point read is
                // the same answer without walking the molecule's neighbours.
                let count_base = format!("moc:{mol}");
                let count_key = build_storage_key(storage_prefix, &count_base);
                if let Some(v) =
                    self.raw()
                        .inner()
                        .get(count_key.as_bytes())
                        .await
                        .map_err(|e| {
                            SchemaError::InvalidData(format!("order-count read {schema_name}: {e}"))
                        })?
                {
                    order_count_keys += 1;
                    order_count_bytes += count_key.len() as u64 + v.len() as u64;
                }
            }
            if events > 0 {
                per_schema_history.push(SchemaHistoryStat {
                    schema_name: schema_name.clone(),
                    history_events: events,
                    history_bytes_approx: bytes,
                });
            }
            if order_log_entries > 0 || order_count_keys > 0 {
                per_schema_order_log.push(SchemaOrderLogStat {
                    schema_name: schema_name.clone(),
                    order_log_entries,
                    order_log_bytes_approx: order_log_bytes,
                    order_count_keys,
                    order_count_bytes_approx: order_count_bytes,
                });
            }
        }
        per_schema_history.sort_by_key(|b| std::cmp::Reverse(b.history_bytes_approx));
        per_schema_order_log.sort_by_key(|b| std::cmp::Reverse(b.order_log_bytes_approx));

        let mut notes = vec![
            "main = single sled namespace holding atoms, molecule indexes, and history (not a schema itself).".into(),
            "Sizes are live key+value lengths (decrypted path when store is encrypting); sled freelist not included.".into(),
            "clear-history: keep_last=N trims; keep_last=0 purges all history: rows. New writes no longer append history: (prev is tip prev_atom_uuid). Tips/atoms kept; orphan atoms need gc-atoms.".into(),
            "schemaidx markers now live in the local-only schema_index namespace; main-tree schemaidx rows are legacy leftovers.".into(),
            "live dual-read for schemaidx: is indexes → tips only; legacy_schema_secondary_index is not consulted (explicit CoW drain still available).".into(),
        ];
        if main_classes
            .iter()
            .any(|c| c.class == "legacy_schema_secondary_index")
        {
            notes.push(
                "legacy_schema_secondary_index residue may still exist on disk: purge via \
                 `lastdb-local-maintain drain-index-residue --source legacy-schema-secondary-index \
                 --execute --drop-empty-source` on CoW first; active listings use schema_index."
                    .into(),
            );
        }

        // A protein is reachable only through some molecule's `molprot:` back-ref,
        // and at most one back-ref points at each protein a client created for a
        // single molecule. So `protein:` rows far exceeding `molprot:` rows means
        // proteins were created and never bound — orphans, which nothing reclaims.
        let class_keys = |class: &str| {
            main_classes
                .iter()
                .find(|c| c.class == class)
                .map_or(0, |c| c.keys)
        };
        let proteins = class_keys("protein_record");
        let backrefs = class_keys("protein_member_backref");
        if proteins > backrefs.saturating_mul(2) && proteins.saturating_sub(backrefs) > 1_000 {
            notes.push(format!(
                "ORPHAN PROTEINS LIKELY: {proteins} protein: rows vs {backrefs} molprot: back-refs (~{} unbound). A client that creates a protein before checking `GET /api/protein/of-molecule/{{mol}}` leaks one per call, and no code path deletes protein: rows.",
                proteins.saturating_sub(backrefs)
            ));
        }

        let tip_format = self.scan_tip_format(None, None, storage_prefix).await?;
        if tip_format.tips_fat > 0 {
            notes.push(format!(
                "LEGACY fat tips still present: {} of {} mk: values — run `lastdb db migrate-thin-tips --execute`.",
                tip_format.tips_fat, tip_format.tips_scanned
            ));
        } else if tip_format.tips_scanned > 0 {
            notes.push("All scanned mk: tips are thin (no per-tip signature/pubkey).".into());
        }

        Ok(DbInventory {
            main_classes,
            main_total_keys,
            main_total_bytes,
            per_schema_atoms: atoms.per_schema,
            per_schema_atoms_total_bytes: atoms.total_logical_bytes,
            per_schema_history,
            per_schema_order_log,
            tip_format,
            attribution: crate::db_operations::AttributionSummary::default(),
            notes,
        })
    }

    /// Scan `mk:` tip values and count thin vs fat, bounded and resumable.
    ///
    /// **Why bounded.** `tips` is the largest plane on a real home. The
    /// whole-prefix form of this scan materialized every `mk:` row in one
    /// allocation before deciding any of them, so its transient footprint grew
    /// with the plane rather than with the answer. Measured on a copy-on-write
    /// clone of a real 7.6 GiB home (2026-08-18): 2,188,593 `mk:` rows,
    /// 1.22 GB of key+value bytes held at once. That home completes in ~47s,
    /// so this is a scaling hazard rather than an observed timeout — but the
    /// allocation is unbounded in the one dimension that keeps growing, and
    /// the control socket's read deadline is what it grows toward.
    /// Paging the ordered keyspace with a key cap and a wall-clock budget makes
    /// the question answerable one window at a time, the same discipline
    /// [`AtomStore::audit_key_tombstone_flags`] already uses.
    ///
    /// `max_keys` caps rows decided by this call; `after_key` resumes past a
    /// previous call's `next_after_key`. Neither is required: with both `None`
    /// the pass still yields at [`Self::TIP_PASS_TIME_BUDGET`] rather than
    /// running to the deadline, and says so in `more_remaining`.
    pub async fn scan_tip_format(
        &self,
        max_keys: Option<usize>,
        after_key: Option<&str>,
        storage_prefix: Option<&str>,
    ) -> Result<TipFormatStats, SchemaError> {
        use super::super::atom_store::PerKeyRecord;

        let pass_started = std::time::Instant::now();
        let prefix = build_storage_key(storage_prefix, "mk:");
        let end = FilterUtils::create_prefix_end(&prefix);
        let mut stats = TipFormatStats::default();
        let mut page_start = after_key.map_or_else(|| prefix.clone(), str::to_owned);
        let mut skip_head = after_key.map(str::to_owned);
        let mut range_exhausted = false;

        while !range_exhausted {
            let rows = self
                .raw()
                .inner()
                .scan_range_paged(page_start.as_bytes(), end.as_bytes(), Self::TIP_SCAN_PAGE)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("scan mk tips: {e}")))?;
            if rows.len() < Self::TIP_SCAN_PAGE {
                range_exhausted = true;
            }
            if rows.is_empty() {
                break;
            }
            let head = skip_head.take();
            let mut stop_after_page = false;
            for (k, v) in rows {
                let key = String::from_utf8_lossy(&k).into_owned();
                page_start.clone_from(&key);
                if head.as_deref() == Some(key.as_str()) {
                    continue;
                }
                if max_keys.is_some_and(|max| stats.tips_scanned as usize >= max)
                    || (stats.tips_scanned > 0
                        && pass_started.elapsed() >= Self::TIP_PASS_TIME_BUDGET)
                {
                    stats.more_remaining = true;
                    stop_after_page = true;
                    break;
                }
                stats.tips_scanned += 1;
                stats.next_after_key = Some(key);
                let bytes = k.len() as u64 + v.len() as u64;
                match serde_json::from_slice::<PerKeyRecord>(&v) {
                    Ok(rec) if rec.entry.is_thin() => {
                        stats.tips_thin += 1;
                        stats.thin_bytes_approx += bytes;
                    }
                    Ok(_) => {
                        stats.tips_fat += 1;
                        stats.fat_bytes_approx += bytes;
                    }
                    Err(_) => {
                        stats.tips_unreadable += 1;
                    }
                }
            }
            skip_head = Some(page_start.clone());
            if stop_after_page {
                break;
            }
        }
        Ok(stats)
    }

    /// Raw `mk:` rows fetched per storage page. Bounds one pass's transient
    /// scan allocation independently of how large the `tips` plane is.
    const TIP_SCAN_PAGE: usize = 2048;
    /// Rewrites per batched put, drained inside the walk.
    const TIP_PUT_CHUNK: usize = 1000;
    /// Wall-clock budget for one pass, well under the 600s control-socket read
    /// deadline. A key cap alone cannot bound a pass whose per-row cost varies
    /// by home; the bound the caller actually needs is the one the socket
    /// enforces, so bound on that.
    const TIP_PASS_TIME_BUDGET: std::time::Duration = std::time::Duration::from_secs(240);

    /// Rewrite fat `mk:` tip values to thin in place (same keys), bounded and
    /// resumable.
    ///
    /// **Why bounded.** The whole-prefix form scanned every `mk:` row into one
    /// allocation and then queued every rewrite into a second one before
    /// writing anything — on a real home that is millions of rows twice over,
    /// and the write half only started after the whole plane had been read.
    /// Worse, the dry run built the rewrite queue it would never use, so
    /// *measuring* the lever cost as much memory as pulling it. Pages of
    /// [`Self::TIP_SCAN_PAGE`] with a key cap and a time budget bound the read,
    /// draining puts inside the walk bounds the write, and dry runs now
    /// allocate nothing.
    ///
    /// `max_keys` caps rows decided by this call; `after_key` resumes past a
    /// previous call's `next_after_key`. A pass that stops early says so in
    /// `more_remaining` — the caller follows the cursor to the end.
    ///
    /// **What it returns, measured — do not budget bytes against this.** On a
    /// copy-on-write clone of a real 7.6 GiB home (2026-08-18), the dry run
    /// decided 2,188,593 `mk:` rows and found 2,184,537 of them (**99.81%**)
    /// already thin. Rewriting the remaining 4,056 moves 1,219,905,942 bytes
    /// to 1,219,650,414 — **249.5 KiB, 0.02% of the `mk:` population**. Local
    /// writes have emitted thin for long enough that the legacy fat tail is
    /// spent. This verb is an instrument, not a reclaim lever: it answers
    /// "is any of this plane still legacy fat?" cheaply and repeatedly.
    /// The mass in `tips` is the `tv:` archived-chain half, which
    /// [`AtomStore::drain_tip_history_chains`] addresses.
    pub async fn migrate_thin_tips(
        &self,
        dry_run: bool,
        max_keys: Option<usize>,
        after_key: Option<&str>,
        storage_prefix: Option<&str>,
    ) -> Result<ThinTipMigrateReport, SchemaError> {
        use super::super::atom_store::PerKeyRecord;

        let pass_started = std::time::Instant::now();
        let prefix = build_storage_key(storage_prefix, "mk:");
        let end = FilterUtils::create_prefix_end(&prefix);

        let mut report = ThinTipMigrateReport {
            dry_run,
            ..Default::default()
        };
        let mut to_put: Vec<(String, Value)> = Vec::new();
        let mut page_start = after_key.map_or_else(|| prefix.clone(), str::to_owned);
        let mut skip_head = after_key.map(str::to_owned);
        let mut range_exhausted = false;

        while !range_exhausted {
            let rows = self
                .raw()
                .inner()
                .scan_range_paged(page_start.as_bytes(), end.as_bytes(), Self::TIP_SCAN_PAGE)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("scan mk migrate: {e}")))?;
            if rows.len() < Self::TIP_SCAN_PAGE {
                range_exhausted = true;
            }
            if rows.is_empty() {
                break;
            }
            let head = skip_head.take();
            let mut stop_after_page = false;
            for (k, v) in rows {
                let key = String::from_utf8_lossy(&k).into_owned();
                page_start.clone_from(&key);
                if head.as_deref() == Some(key.as_str()) {
                    continue;
                }
                if max_keys.is_some_and(|max| report.tips_scanned as usize >= max)
                    || (report.tips_scanned > 0
                        && pass_started.elapsed() >= Self::TIP_PASS_TIME_BUDGET)
                {
                    report.more_remaining = true;
                    stop_after_page = true;
                    break;
                }
                report.tips_scanned += 1;
                report.bytes_before_approx += k.len() as u64 + v.len() as u64;
                report.next_after_key = Some(key.clone());

                let Ok(mut rec) = serde_json::from_slice::<PerKeyRecord>(&v) else {
                    report.tips_unreadable += 1;
                    report.bytes_after_approx += k.len() as u64 + v.len() as u64;
                    continue;
                };
                if rec.entry.is_thin() {
                    report.tips_already_thin += 1;
                    report.bytes_after_approx += k.len() as u64 + v.len() as u64;
                    continue;
                }
                rec.entry = rec.entry.to_thin();
                let val = serde_json::to_value(&rec)
                    .map_err(|e| SchemaError::InvalidData(format!("serialize thin tip: {e}")))?;
                report.bytes_after_approx +=
                    key.len() as u64 + serde_json::to_vec(&val).map_or(0, |b| b.len() as u64);
                report.tips_rewritten += 1;
                // A dry run answers "how much would this return?" — it must not
                // pay for the answer in the memory the execute path needs.
                if !dry_run {
                    to_put.push((key, val));
                    if to_put.len() >= Self::TIP_PUT_CHUNK {
                        self.drain_thin_tip_puts(&mut to_put).await?;
                    }
                }
            }
            skip_head = Some(page_start.clone());
            if stop_after_page {
                break;
            }
        }

        if !dry_run {
            self.drain_thin_tip_puts(&mut to_put).await?;
            let _ = self.flush().await;
        }

        Ok(report)
    }

    /// Write and clear the pending thin-tip rewrites.
    pub(super) async fn drain_thin_tip_puts(
        &self,
        to_put: &mut Vec<(String, Value)>,
    ) -> Result<(), SchemaError> {
        if to_put.is_empty() {
            return Ok(());
        }
        for chunk in to_put.chunks(Self::TIP_PUT_CHUNK) {
            self.raw()
                .batch_put_items(chunk.to_vec())
                .await
                .map_err(|e| SchemaError::InvalidData(format!("migrate thin put: {e}")))?;
        }
        to_put.clear();
        Ok(())
    }

    /// Read-only list of one molecule's live `mk:` storage keys.
    ///
    /// Point-scoped to `mk:{M}:` (has a partition separator — not a full-store
    /// sweep). Emits the storage key, decoded hash/range when the unified
    /// encoding parses, and marks rows whose decoded pair collides with another
    /// row in the same molecule. No atom bodies are fetched.
    ///
    /// This is the diagnostic that separates H1 (two logical keys, one order
    /// entry) from H2 (one logical key stored as two encodings) for the
    /// order-log shortfall investigation.
    // lint:fn-size-ok verbatim move from admin_db.rs; splitting this function is separate work
    pub async fn list_molecule_keys(
        &self,
        molecule: &str,
        max_keys: Option<usize>,
        storage_prefix: Option<&str>,
    ) -> Result<MoleculeKeysReport, SchemaError> {
        let mol_prefix = build_storage_key(
            storage_prefix,
            &molecule_key_codec::molecule_record_prefix(molecule),
        );
        let mol_end = FilterUtils::create_prefix_end(&mol_prefix);
        let mut page_start = mol_prefix.clone();
        let mut skip_head: Option<String> = None;
        let mut rows_out: Vec<MoleculeKeyRow> = Vec::new();
        let mut seen: HashMap<(String, String), usize> = HashMap::new();
        let mut more = false;
        const PAGE: usize = 512;

        loop {
            let rows = self
                .raw()
                .inner()
                .scan_range_paged(page_start.as_bytes(), mol_end.as_bytes(), PAGE)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("scan molecule keys: {e}")))?;
            let page_len = rows.len();
            if rows.is_empty() {
                break;
            }
            let head = skip_head.take();
            for (k, _) in rows {
                let key = String::from_utf8_lossy(&k).into_owned();
                page_start = key.clone();
                if head.as_deref() == Some(key.as_str()) {
                    continue;
                }
                if max_keys.is_some_and(|m| rows_out.len() >= m) {
                    more = true;
                    break;
                }
                let decoded = molecule_key_codec::decode_hash_range(&key, molecule).or_else(|| {
                    // Storage-prefix form: strip outer prefix then decode bare mk: key.
                    let bare = key
                        .rsplit_once(":mk:")
                        .map_or_else(|| key.clone(), |(_, rest)| format!("mk:{rest}"));
                    molecule_key_codec::decode_hash_range(&bare, molecule)
                });
                let (hash, range, collision) = if let Some((h, r)) = decoded {
                    let n = seen.entry((h.clone(), r.clone())).or_insert(0);
                    *n += 1;
                    (Some(h), Some(r), *n > 1)
                } else {
                    (None, None, false)
                };
                rows_out.push(MoleculeKeyRow {
                    storage_key: key,
                    hash,
                    range,
                    collision,
                });
            }
            if more {
                break;
            }
            if page_len < PAGE {
                break;
            }
            skip_head = Some(page_start.clone());
        }

        // Second pass: mark every row that participates in a multi-row logical
        // identity, not only the second+ sightings (first occurrence also
        // collides when n>1 after the walk).
        let multi: HashSet<(String, String)> = seen
            .into_iter()
            .filter(|(_, n)| *n > 1)
            .map(|(k, _)| k)
            .collect();
        for row in &mut rows_out {
            if let (Some(h), Some(r)) = (&row.hash, &row.range) {
                row.collision = multi.contains(&(h.clone(), r.clone()));
            }
        }
        let collisions = rows_out.iter().filter(|r| r.collision).count() as u64;
        let unique = rows_out
            .iter()
            .filter_map(|r| match (&r.hash, &r.range) {
                (Some(h), Some(r)) => Some((h.as_str(), r.as_str())),
                _ => None,
            })
            .collect::<HashSet<_>>()
            .len() as u64;
        let undecodable = rows_out.iter().filter(|r| r.hash.is_none()).count() as u64;

        Ok(MoleculeKeysReport {
            molecule: molecule.to_string(),
            keys: rows_out.len() as u64,
            unique_keys: unique + undecodable,
            collision_rows: collisions,
            more_remaining: more,
            rows: rows_out,
        })
    }

    /// Dump one molecule's raw `mk:` rows under one API HashKey.
    ///
    /// Unlike [`Self::list_molecule_keys`], this method never walks the full
    /// molecule. It maps `api_hash` through the in-process molecule key bundle
    /// and reads only the exact hash partition. The node route that exposes
    /// this method requires an explicit development-only environment gate.
    pub async fn debug_molecule_hash_bucket(
        &self,
        molecule: &str,
        api_hash: &str,
        max_keys: usize,
        storage_prefix: Option<&str>,
    ) -> Result<MoleculeHashBucketReport, SchemaError> {
        if molecule.is_empty() {
            return Err(SchemaError::InvalidData(
                "debug molecule hash bucket requires a molecule".into(),
            ));
        }
        if api_hash.is_empty() {
            return Err(SchemaError::InvalidData(
                "debug molecule hash bucket requires an API hash".into(),
            ));
        }
        if max_keys == 0 {
            return Err(SchemaError::InvalidData(
                "debug molecule hash bucket requires max_keys > 0".into(),
            ));
        }

        // Warm an existing per-molecule bundle without minting one. A missing
        // bundle is a legacy molecule and correctly falls back to node keys.
        self.load_molecule_key_bundle(molecule).await?;
        let codec = self.key_codec_for_molecule(molecule);
        let prefixes = codec
            .api_hash_range_scan_prefixes_for_read(molecule, api_hash)
            .map_err(|error| SchemaError::InvalidData(error.to_string()))?;

        let want = max_keys.saturating_add(1);
        let mut raw_rows = self
            .scan_distinct_per_key_prefixes(&prefixes, storage_prefix, want)
            .await?;
        let more_remaining = raw_rows.len() > max_keys;
        raw_rows.truncate(max_keys);

        let mut entries = Vec::with_capacity(raw_rows.len());
        for (storage_key, record) in raw_rows {
            let Some((storage_hash, storage_range)) =
                molecule_key_codec::decode_hash_range_any(&storage_key)
            else {
                return Err(SchemaError::InvalidData(format!(
                    "cannot decode molecule key {storage_key}"
                )));
            };
            let api_range = crate::crypto::E2eKeys::ope_decode_range_plaintext(&storage_range)
                .or_else(|| {
                    (codec.range_encoding() == crate::atom::RangeKeyEncoding::Plain)
                        .then(|| storage_range.clone())
                });
            let atom_content = self
                .get_atom_by_uuid(&record.entry.atom_uuid, storage_prefix)
                .await?
                .map(|atom| atom.content().clone());
            entries.push(MoleculeHashBucketRow {
                storage_key,
                storage_hash,
                storage_range,
                api_range,
                tip: record.entry,
                metadata: record.meta,
                atom_content,
            });
        }

        Ok(MoleculeHashBucketReport {
            molecule: molecule.to_string(),
            api_hash: api_hash.to_string(),
            storage_prefixes: prefixes,
            rows: entries.len() as u64,
            more_remaining,
            entries,
        })
    }

    /// Read up to `want` distinct rows across possibly overlapping prefixes.
    ///
    /// A raw-row budget is incorrect here. A later prefix can repeat rows from
    /// an earlier prefix before it reaches a new row. Resume each prefix until
    /// it is exhausted or the distinct-row target is full.
    pub(super) async fn scan_distinct_per_key_prefixes(
        &self,
        prefixes: &[String],
        storage_prefix: Option<&str>,
        want: usize,
    ) -> Result<Vec<(String, crate::db_operations::atom_store::PerKeyRecord)>, SchemaError> {
        if want == 0 {
            return Ok(Vec::new());
        }

        let mut distinct = BTreeMap::new();
        for prefix in prefixes {
            let end = FilterUtils::create_prefix_end(prefix);
            let mut cursor: Option<String> = None;
            loop {
                if distinct.len() >= want {
                    break;
                }
                let remaining = want - distinct.len();
                let (batch, exhausted) = match cursor.as_deref() {
                    None => {
                        let rows = self
                            .scan_per_key_prefix_paged(prefix, storage_prefix, remaining)
                            .await?;
                        let exhausted = rows.len() < remaining;
                        (rows, exhausted)
                    }
                    Some(after) => {
                        let probe = remaining.saturating_add(1);
                        let rows = self
                            .scan_per_key_range_paged(after, &end, storage_prefix, probe)
                            .await?;
                        let exhausted = rows.len() < probe;
                        let advanced = rows
                            .into_iter()
                            .filter(|(key, _)| key.as_str() > after)
                            .collect();
                        (advanced, exhausted)
                    }
                };

                let Some((last, _)) = batch.last() else {
                    break;
                };
                cursor = Some(last.clone());
                for (key, record) in batch {
                    distinct.entry(key).or_insert(record);
                }
                if exhausted {
                    break;
                }
            }
            if distinct.len() >= want {
                break;
            }
        }
        Ok(distinct.into_iter().take(want).collect())
    }
}
