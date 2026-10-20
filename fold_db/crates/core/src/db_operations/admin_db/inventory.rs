//! Live byte inventory and key-class accounting.

use super::inventory_key_classes::{ATOM_COUNTER_CLASS_LABEL, MAIN_KEY_CLASSES};
use super::*;
use crate::db_operations::atom_store::RangeWalk;

impl AtomStore {
    /// Fold every row under `prefix` into `(keys, live key+value bytes)` while
    /// holding at most one bounded page.
    ///
    /// The totals are exactly what a whole-prefix scan produced; this changes
    /// only how they are reached, not what they say. Every read is bounded by
    /// the read budget of one call (see [`RangeWalk`]), also in a range that
    /// holds many deleted keys.
    pub(super) async fn inventory_accumulate_prefix(
        &self,
        prefix: &str,
        what: &str,
    ) -> Result<(u64, u64), SchemaError> {
        let end = FilterUtils::create_prefix_end(prefix)?;
        let mut walk = RangeWalk::new(prefix.as_bytes(), end.as_bytes(), INVENTORY_PAGE_ROWS_MIN);
        let mut keys = 0u64;
        let mut bytes = 0u64;
        while !walk.done() {
            let rows = self
                .walk_step(&mut walk)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("scan {what}: {e}")))?
                .rows;
            let mut page_bytes = 0u64;
            for (k, v) in &rows {
                let row_bytes = k.len() as u64 + v.len() as u64;
                page_bytes += row_bytes;
                keys += 1;
                bytes += row_bytes;
            }
            if !rows.is_empty() {
                walk.set_limit(inventory_next_page_rows(page_bytes, rows.len()));
            }
        }
        Ok((keys, bytes))
    }

    /// Like [`Self::inventory_accumulate_prefix`] for the `atom:` prefix, which
    /// holds two kinds of record: bodies and counters. Returns the totals of
    /// the bodies first and the totals of the counters second, from one walk.
    async fn inventory_accumulate_atoms(
        &self,
        prefix: &str,
    ) -> Result<((u64, u64), (u64, u64)), SchemaError> {
        let end = FilterUtils::create_prefix_end(prefix)?;
        let mut walk = RangeWalk::new(prefix.as_bytes(), end.as_bytes(), INVENTORY_PAGE_ROWS_MIN);
        let (mut bodies, mut counters) = ((0u64, 0u64), (0u64, 0u64));
        while !walk.done() {
            let rows = self
                .walk_step(&mut walk)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("scan atom_content: {e}")))?
                .rows;
            let mut page_bytes = 0u64;
            for (k, v) in &rows {
                let row_bytes = k.len() as u64 + v.len() as u64;
                page_bytes += row_bytes;
                let is_counter =
                    std::str::from_utf8(k).is_ok_and(crate::atom::atom_key_codec::is_counter_key);
                let total = if is_counter {
                    &mut counters
                } else {
                    &mut bodies
                };
                total.0 += 1;
                total.1 += row_bytes;
            }
            if !rows.is_empty() {
                walk.set_limit(inventory_next_page_rows(page_bytes, rows.len()));
            }
        }
        Ok((bodies, counters))
    }

    /// Scan `main` by known key prefixes and sum live key+value sizes.
    pub async fn inventory_main_key_classes(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<MainKeyClass>, SchemaError> {
        // Prefixes must be ordered longest-first for classification; we scan
        // each prefix independently (they don't nest).
        let classes = MAIN_KEY_CLASSES;

        let mut out = Vec::with_capacity(classes.len() + 1);
        for (class, bare_prefix, label) in classes {
            let prefix = build_storage_key(storage_prefix, bare_prefix);
            // The `atom:` prefix holds the bodies and the counters of atoms.
            // The counters are not atom content, so they get their own class.
            let rows = if *class == "atom_content" {
                let (bodies, counters) = self.inventory_accumulate_atoms(&prefix).await?;
                vec![
                    (*class, *label, bodies),
                    ("atom_counter", ATOM_COUNTER_CLASS_LABEL, counters),
                ]
            } else {
                let totals = self.inventory_accumulate_prefix(&prefix, class).await?;
                vec![(*class, *label, totals)]
            };
            for (class, label, (keys, bytes)) in rows {
                if keys > 0 {
                    out.push(MainKeyClass {
                        class: class.to_string(),
                        prefix: label.to_string(),
                        keys,
                        bytes,
                    });
                }
            }
        }
        out.sort_by_key(|b| std::cmp::Reverse(b.bytes));
        Ok(out)
    }

    /// Build a full inventory: main key classes + per-schema atom sizes + history stats.
    pub async fn db_inventory(
        &self,
        schema_names: &[String],
        storage_prefix: Option<&str>,
        // field molecule uuids per schema: (schema, Vec<field_mol_uuid>)
        schema_field_molecules: &[(String, Vec<String>)],
    ) -> Result<DbInventory, SchemaError> {
        let walks_before = self.range_walks().snapshot();
        let main_classes = self.inventory_main_key_classes(storage_prefix).await?;
        let main_total_keys = main_classes.iter().map(|c| c.keys).sum();
        let main_total_bytes = main_classes.iter().map(|c| c.bytes).sum();

        let atoms: StorageBreakdown = self.storage_breakdown(schema_names, storage_prefix).await?;

        let per_schema_history = self
            .inventory_schema_history(schema_field_molecules, storage_prefix)
            .await?;

        let scan = self.range_walks().snapshot().since(walks_before);
        let mut notes = inventory_notes(&main_classes);
        if scan.unsealed_rows > 0 {
            notes.push(format!(
                "{} read(s) dropped a row with no at-rest envelope. Such a row reads as absent and is not in these totals. \
                 Run `lastdb db reap-unsealed --collection <name>` (dry run first) to remove them.",
                scan.unsealed_rows
            ));
        }

        Ok(DbInventory {
            main_classes,
            main_total_keys,
            main_total_bytes,
            per_schema_atoms: atoms.per_schema,
            per_schema_atoms_total_bytes: atoms.total_logical_bytes,
            atom_counters: atoms.atom_counters,
            per_schema_history,
            scan,
            attribution: crate::db_operations::AttributionSummary::default(),
            notes,
        })
    }
    /// Per-schema history sizes, summed over each schema's field molecules.
    async fn inventory_schema_history(
        &self,
        schema_field_molecules: &[(String, Vec<String>)],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<SchemaHistoryStat>, SchemaError> {
        let mut per_schema_history = Vec::new();
        for (schema_name, mols) in schema_field_molecules {
            let mut events = 0u64;
            let mut bytes = 0u64;
            for mol in mols {
                let base = crate::atom::molecule_key_codec::history_molecule_prefix(mol);
                let prefix = build_storage_key(storage_prefix, &base);
                let (mol_events, mol_bytes) = self
                    .inventory_accumulate_prefix(&prefix, &format!("history {schema_name}"))
                    .await?;
                events += mol_events;
                bytes += mol_bytes;
            }
            if events > 0 {
                per_schema_history.push(SchemaHistoryStat {
                    schema_name: schema_name.clone(),
                    history_events: events,
                    history_bytes_approx: bytes,
                });
            }
        }
        per_schema_history.sort_by_key(|b| std::cmp::Reverse(b.history_bytes_approx));
        Ok(per_schema_history)
    }
}

/// Human-readable caveats and residue warnings for a finished inventory.
fn inventory_notes(main_classes: &[MainKeyClass]) -> Vec<String> {
    let mut notes = vec![
        "Sizes count live keys and values; physical file sizes can differ.".into(),
        "History contains current mutation events and conflict records.".into(),
        "atom: holds two records for each atom: the body (atom:{uuid}\\0b) and its counter (atom:{uuid}\\0c). atom_counters reports the counters.".into(),
    ];
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

    notes
}
