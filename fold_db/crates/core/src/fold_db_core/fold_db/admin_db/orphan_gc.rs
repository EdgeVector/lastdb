//! Orphan file-blob and protein garbage collection.

use super::*;

impl FoldDB {
    /// Delete local file-blob rows (`cas_blobs` namespace + resident
    /// `cas_blob:` rows) no live atom references. The reclaim path for the
    /// sealed bytes a purge leaves behind — see
    /// [`crate::db_operations::file_blob_gc`].
    pub async fn gc_orphan_file_blobs(
        &self,
        dry_run: bool,
    ) -> Result<crate::db_operations::file_blob_gc::FileBlobGcReport, FoldDbError> {
        crate::db_operations::file_blob_gc::gc_orphan_file_blobs(self.db_ops(), dry_run)
            .await
            .map_err(|e| FoldDbError::Database(format!("gc_orphan_file_blobs: {e}")))
    }

    /// Delete empty `protein:` rows that are not named by any `molprot:` back-ref.
    pub async fn gc_orphan_proteins(&self, dry_run: bool) -> Result<ProteinGcReport, FoldDbError> {
        self.db_ops()
            .atoms()
            .gc_orphan_proteins(dry_run, None)
            .await
            .map_err(|e| FoldDbError::Database(format!("gc_orphan_proteins: {e}")))
    }
}
