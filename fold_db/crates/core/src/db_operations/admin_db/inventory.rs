//! Live byte inventory and key-class accounting.

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
}
