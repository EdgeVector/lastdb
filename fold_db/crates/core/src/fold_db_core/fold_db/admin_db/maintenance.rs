//! Reseal, unsealed-row reap and history clearing.

use super::*;

impl FoldDB {
    /// Rewrite sealed values in one plane to the requested ENB target policy.
    ///
    /// Dry-run is the default. Never point this at the live primary from a
    /// routine; owner-gated there. Same-key replace; no dual-key copies.
    pub async fn reseal_at_rest(
        &self,
        options: crate::storage::ResealAtRestOptions,
    ) -> Result<crate::storage::ResealAtRestReport, FoldDbError> {
        self.db_ops()
            .namespaced_store()
            .reseal_at_rest(options)
            .await
            .map_err(|e| FoldDbError::Database(format!("reseal-at-rest: {e}")))
    }

    /// Remove un-enveloped rows from one encrypted plane. They already read as
    /// absent (`decision-2026-09-14-drop-dual-read-unsealed-is-gone`); this
    /// returns their bytes.
    ///
    /// Dry-run is the default. Owner-gated. Refuses every plaintext-by-policy
    /// namespace by name. On request only: a non-zero count on a healthy home
    /// is a signal for a human, not a job for a schedule.
    pub async fn reap_unsealed(
        &self,
        options: crate::storage::ReapUnsealedOptions,
    ) -> Result<crate::storage::ReapUnsealedReport, FoldDbError> {
        self.db_ops()
            .namespaced_store()
            .reap_unsealed(options)
            .await
            .map_err(|e| FoldDbError::Database(format!("reap-unsealed: {e}")))
    }

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
