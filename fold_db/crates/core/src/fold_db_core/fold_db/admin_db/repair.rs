//! The delete ledger.

use super::*;

impl FoldDB {
    /// Read the durable record hard-delete ledger, oldest first.
    ///
    /// The audit counterpart to the purge verb: every hard-delete batch either
    /// appears here or did not happen. `limit` of 0 means unbounded. See [`crate::db_operations::delete_ledger`] for
    /// what a row does and does not carry.
    pub async fn list_record_delete_ledger(
        &self,
        limit: usize,
    ) -> Result<Vec<RecordDeleteLedgerEntry>, FoldDbError> {
        self.db_ops()
            .records()
            .list_record_delete_ledger(None, limit)
            .await
            .map_err(|e| FoldDbError::Database(format!("list_record_delete_ledger: {e}")))
    }
}
