//! Per-database and per-schema storage inventory reports.

use super::*;

impl FoldDB {
    /// Live inventory of the `main` tree key classes and the live records of
    /// each schema, plus a fresh attribution summary.
    ///
    /// The attribution numbers are not a passive read: this route runs the
    /// bounded schema/system-root and retention-root walks (catalog- and
    /// registry-sized, not record-sized — the same cost class as the rest of
    /// this admin scan) before it summarizes the ledger, so the counts
    /// reflect the current catalog rather than whatever a prior caller
    /// happened to leave behind.
    pub async fn db_inventory(&self) -> Result<DbInventory, FoldDbError> {
        let schemas = self
            .db_ops()
            .get_all_schemas()
            .await
            .map_err(|e| FoldDbError::Database(format!("list schemas: {e}")))?;
        let names: Vec<String> = schemas.values().map(|schema| schema.name.clone()).collect();
        let mut inventory = self
            .db_ops()
            .records()
            .db_inventory(&names, None)
            .await
            .map_err(|e| FoldDbError::Database(format!("db_inventory: {e}")))?;

        self.db_ops()
            .attribute_schema_root_field_keys(LIVE_ATTRIBUTION_EPOCH_ID, 0)
            .await
            .map_err(|e| FoldDbError::Database(format!("attribute schema roots: {e}")))?;
        self.db_ops()
            .attribute_schema_retention_roots(LIVE_ATTRIBUTION_EPOCH_ID, 0)
            .await
            .map_err(|e| FoldDbError::Database(format!("attribute retention roots: {e}")))?;
        inventory.attribution = self
            .db_ops()
            .attribution()
            .summarize()
            .await
            .map_err(|e| FoldDbError::Database(format!("summarize attribution: {e}")))?;
        Ok(inventory)
    }

    /// Per-schema logical storage: the live records of each schema
    /// (`record_count`, `record_bytes`). Their bytes count in `bytes` and the
    /// logical total.
    ///
    /// Catalog `descriptive_name` is joined as `display_name` when it differs
    /// from the runtime identity. This is the `lastdb db schemas` product path.
    pub async fn db_schema_storage(&self) -> Result<SchemaStorageReport, FoldDbError> {
        let schemas = self
            .db_ops()
            .get_all_schemas()
            .await
            .map_err(|e| FoldDbError::Database(format!("list schemas: {e}")))?;
        // The record space of each runtime schema name names its row.
        let names: Vec<String> = schemas.values().map(|schema| schema.name.clone()).collect();
        let mut breakdown = self
            .db_ops()
            .records()
            .record_storage_breakdown(&names, None)
            .await
            .map_err(|e| FoldDbError::Database(format!("schema record storage: {e}")))?;
        let mut display = HashMap::new();
        for (key, schema) in &schemas {
            let label = schema
                .descriptive_name
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or(schema.name.trim());
            if label.is_empty() {
                continue;
            }
            display.insert(key.clone(), label.to_string());
            display.insert(schema.name.clone(), label.to_string());
            if let Some(hash) = schema.identity_hash.as_ref() {
                display.insert(hash.clone(), label.to_string());
            }
        }
        breakdown.apply_display_names(&display);
        Ok(SchemaStorageReport::from_breakdown(breakdown))
    }
}
