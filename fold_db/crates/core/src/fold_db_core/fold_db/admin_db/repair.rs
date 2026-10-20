//! Delete ledger and molecule keys.

use super::*;

impl FoldDB {
    /// Read the durable atom hard-delete ledger, oldest first.
    ///
    /// The audit counterpart to the purge verb: every hard-delete batch either
    /// appears here or did not happen. `limit` of 0 means unbounded. See [`crate::db_operations::delete_ledger`] for
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
}

impl FoldDB {
    pub async fn list_molecule_keys(
        &self,
        molecule: &str,
        max_keys: Option<usize>,
    ) -> Result<crate::db_operations::MoleculeKeysReport, FoldDbError> {
        self.db_ops()
            .atoms()
            .list_molecule_keys(molecule, max_keys, None)
            .await
            .map_err(|e| admin_op_error("list_molecule_keys", e))
    }
    pub async fn debug_molecule_hash_bucket(
        &self,
        molecule: &str,
        api_hash: &str,
        max_keys: usize,
    ) -> Result<crate::db_operations::MoleculeHashBucketReport, FoldDbError> {
        self.db_ops()
            .atoms()
            .debug_molecule_hash_bucket(molecule, api_hash, max_keys, None)
            .await
            .map_err(|e| admin_op_error("debug_molecule_hash_bucket", e))
    }
}
