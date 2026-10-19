//! Delete ledger and thin-tip migrate.

use super::*;

impl FoldDB {
    /// Read the durable atom hard-delete ledger, oldest first.
    ///
    /// The audit counterpart to [`Self::gc_orphan_atoms`] and the purge verb:
    /// every hard-delete batch either appears here or did not happen. `limit`
    /// of 0 means unbounded. See [`crate::db_operations::delete_ledger`] for
    /// what a row does and does not carry.
    pub async fn list_atom_delete_ledger(
        &self,
        limit: usize,
    ) -> Result<Vec<AtomDeleteLedgerEntry>, FoldDbError> {
        self.db_ops()
            .atoms()
            .list_atom_delete_ledger(None, limit)
            .await
            .map_err(|e| FoldDbError::Database(format!("list_atom_delete_ledger: {e}")))
    }

    /// Rewrite fat `mk:` tip values to thin `{atom_uuid, written_at, device_id}` in place.
    ///
    /// Bounded and resumable — see
    /// [`crate::db_operations::AtomStore::migrate_thin_tips`]. Callers follow
    /// `next_after_key` while `more_remaining` is true.
    pub async fn migrate_thin_tips(
        &self,
        dry_run: bool,
        max_keys: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<ThinTipMigrateReport, FoldDbError> {
        self.db_ops()
            .atoms()
            .migrate_thin_tips(dry_run, max_keys, after_key, None)
            .await
            .map_err(|e| admin_op_error("migrate_thin_tips", e))
    }
}
