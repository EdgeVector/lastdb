//! Per-database and per-schema storage inventory reports.

use super::*;

impl FoldDB {
    /// Live inventory of the `main` tree key classes and the live records of
    /// each schema. It writes nothing.
    pub async fn db_inventory(&self) -> Result<DbInventory, FoldDbError> {
        let schemas = self
            .db_ops()
            .get_all_schemas()
            .await
            .map_err(|e| FoldDbError::Database(format!("list schemas: {e}")))?;
        let names: Vec<String> = schemas.values().map(|schema| schema.name.clone()).collect();
        self.db_ops()
            .records()
            .db_inventory(&names, None)
            .await
            .map_err(|e| FoldDbError::Database(format!("db_inventory: {e}")))
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
