//! Orphan protein garbage collection.

use super::*;

impl FoldDB {
    /// Delete empty `protein:` rows that are not named by any `molprot:` back-ref.
    pub async fn gc_orphan_proteins(&self, dry_run: bool) -> Result<ProteinGcReport, FoldDbError> {
        self.db_ops()
            .atoms()
            .gc_orphan_proteins(dry_run, None)
            .await
            .map_err(|e| FoldDbError::Database(format!("gc_orphan_proteins: {e}")))
    }
}
