//! A read-only, bounded view of selected conflict metadata.

use crate::db_operations::{MoleculeConflictState, SELECTED_CONFLICT_MAX_MOLECULES};

use super::FoldDB;

impl FoldDB {
    /// Read personal field-molecule conflict metadata without cache repair.
    ///
    /// The existing serving fence excludes logical and direct KV replay writers.
    /// No flush, cloud request, stamp, or mutation occurs here. A missing fence,
    /// failed admission, failed read, or excessive input stays unknown.
    pub async fn selected_molecule_conflict_metadata(
        &self,
        molecules: &[String],
    ) -> Vec<MoleculeConflictState> {
        let unknown = vec![MoleculeConflictState::Unknown; molecules.len()];
        if molecules.is_empty() || molecules.len() > SELECTED_CONFLICT_MAX_MOLECULES {
            return unknown;
        }
        let Some(router) = self.mutation_manager.capture_router() else {
            return unknown;
        };
        // Admission and the entire metadata read share one finite deadline.
        // Cancellation drops the fence and never changes writer state.
        let read = async {
            let _fence = router.fence_mutations().await;
            self.db_ops.read_selected_conflict_metadata(molecules).await
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), read)
            .await
            .unwrap_or(unknown)
    }
}
