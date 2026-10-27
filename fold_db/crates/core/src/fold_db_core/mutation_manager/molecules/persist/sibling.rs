//! Native protein sibling tip storage.

use super::*;

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) async fn persist_sibling_tip_updates(
        &self,
        sibling_updates: &[(String, MoleculeData, HashSet<ChangedKey>)],
        bodies: &[Atom],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        if sibling_updates.is_empty() {
            return Ok(());
        }
        self.db_ops
            .atoms()
            .store_molecules_changed_keys_batch(sibling_updates, bodies, storage_prefix)
            .await
    }
}
