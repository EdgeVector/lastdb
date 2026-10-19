//! Boot and restore duties of [`AtomStore`] that depend on durable state.

use super::AtomStore;

impl AtomStore {
    /// Prepare a store that this boot constructed over a home.
    ///
    /// Reads the automatic atom-collection generation from the home, so the
    /// reference markers of the previous run stay valid.
    pub(crate) async fn prepare_boot(self) -> Result<Self, crate::storage::StorageError> {
        self.hydrate_automatic_gc_atoms_generation().await?;
        Ok(self)
    }

    /// Prepare the serving store after a photograph restore.
    ///
    /// A restore target booted empty and may carry a fresh-home stamp that the
    /// photograph never held. Drop it before the restored data is served, then
    /// read the generation that the restored home holds.
    pub(crate) async fn refresh_after_restore(&self) -> Result<(), crate::storage::StorageError> {
        self.revoke_fresh_home_atom_ref_markers().await?;
        self.clone().prepare_boot().await.map(|_| ())
    }
}
