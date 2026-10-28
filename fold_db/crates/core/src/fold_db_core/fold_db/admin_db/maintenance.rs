//! History clearing.

use super::*;

impl FoldDB {
    /// Clear mutation history rows. `schema` = None means all schemas.
    /// `keep_last_per_key`: `0` purges all history (latest-only / tip is current);
    /// `>= 1` keeps the newest N events per field key.
    pub async fn clear_mutation_history(
        &self,
        schema: Option<&str>,
        keep_last_per_key: usize,
        dry_run: bool,
    ) -> Result<HistoryClearReport, FoldDbError> {
        let names: Vec<String> = if let Some(s) = schema {
            vec![s.to_string()]
        } else {
            self.db_ops()
                .get_all_schemas()
                .await
                .map_err(|e| FoldDbError::Database(format!("list schemas: {e}")))?
                .into_keys()
                .collect()
        };
        let field_mols = self.schema_field_molecules(&names);
        self.db_ops()
            .atoms()
            .clear_mutation_history(&field_mols, keep_last_per_key, dry_run, None)
            .await
            .map_err(|e| FoldDbError::Database(format!("clear_mutation_history: {e}")))
    }
}
