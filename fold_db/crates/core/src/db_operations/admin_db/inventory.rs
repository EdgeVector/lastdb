//! Live byte inventory and key-class accounting.

use super::inventory_key_classes::MAIN_KEY_CLASSES;
use super::*;
use crate::db_operations::record_store::RangeWalk;

impl RecordStore {
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

    /// Build a full inventory: the main key classes and the live records of
    /// each schema. `schema_names` names the record rows (see
    /// [`RecordStore::add_record_storage`]).
    pub async fn db_inventory(
        &self,
        schema_names: &[String],
        storage_prefix: Option<&str>,
    ) -> Result<DbInventory, SchemaError> {
        let walks_before = self.range_walks().snapshot();
        let main_classes = self.inventory_main_key_classes(storage_prefix).await?;
        let main_total_keys = main_classes.iter().map(|c| c.keys).sum();
        let main_total_bytes = main_classes.iter().map(|c| c.bytes).sum();

        let records = self
            .record_storage_breakdown(schema_names, storage_prefix)
            .await?;

        let scan = self.range_walks().snapshot().since(walks_before);
        let notes = inventory_notes();

        Ok(DbInventory {
            main_classes,
            main_total_keys,
            main_total_bytes,
            per_schema_records: records.per_schema,
            per_schema_records_total_bytes: records.total_logical_bytes,
            scan,
            attribution: crate::db_operations::AttributionSummary::default(),
            notes,
        })
    }
}

/// Human-readable caveats and residue warnings for a finished inventory.
fn inventory_notes() -> Vec<String> {
    let notes = vec![
        "Sizes count live keys and values; physical file sizes can differ.".into(),
        "per_schema_records holds the live records of each schema.".into(),
    ];

    notes
}
