//! Plane residue drain, reseal, compaction, pin-log audit and history clearing.

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

    /// Compact one allowlisted LastStore collection (default dry-run).
    ///
    /// Reclaims superseded segment bodies on allowlisted planes. Atom execute
    /// records durable retirement provenance for the next backup-manifest cut.
    pub async fn compact_collection(
        &self,
        collection: &str,
        dry_run: bool,
    ) -> Result<crate::storage::laststore::CollectionCompactReport, FoldDbError> {
        self.db_ops()
            .namespaced_store()
            .compact_collection(crate::storage::laststore::CollectionCompactOptions {
                collection: collection.to_string(),
                dry_run,
                seed_committed_history: collection == "atoms" && !dry_run,
            })
            .await
            .map_err(|e| FoldDbError::Database(format!("compact collection: {e}")))
    }

    /// Read-only, bounded pin-log plane audit (`lastdb db pin-log-audit`).
    ///
    /// Opens `sync_pin_log`, loads durable published-F maps, and classifies
    /// each entry row as confirmed-orphan (frontier ≤ writer HWM) or
    /// genuinely pending. Never deletes.
    pub async fn audit_pin_log(
        &self,
        max_keys: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<crate::sync::engine::PinLogPlaneReport, FoldDbError> {
        let store = self
            .db_ops()
            .namespaced_store()
            .open_namespace(crate::sync::engine::PIN_LOG_NAMESPACE)
            .await
            .map_err(|e| FoldDbError::Database(format!("open sync_pin_log: {e}")))?;
        let max_keys = max_keys.unwrap_or(crate::sync::engine::PIN_LOG_OPERATOR_KEYS_PER_CALL);
        crate::sync::engine::audit_pin_log_plane(store.as_ref(), max_keys, after_key)
            .await
            .map_err(|e| admin_op_error("pin-log-audit", e))
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
