//! Boot and restore duties of [`AtomStore`] that depend on durable state.

use super::AtomStore;

impl AtomStore {
    /// Prepare a store that this boot constructed over a home.
    ///
    /// A counter needs no state from the home at boot: the first planner run
    /// reads the counters that it changes.
    pub(crate) async fn prepare_boot(self) -> Result<Self, crate::storage::StorageError> {
        Ok(self)
    }

    /// Prepare the serving store after a photograph restore.
    pub(crate) async fn refresh_after_restore(&self) -> Result<(), crate::storage::StorageError> {
        Ok(())
    }
}
