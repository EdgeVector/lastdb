//! Exact invalidation of changed record keys and typed definitions.

use super::*;

impl CoreWarmSet {
    /// Call under the same publication boundary as the changed disk/write view.
    /// A changed record key loses its decoded record, and a definition that
    /// depends on a changed key or collection leaves.
    pub(crate) fn invalidate_sources(&mut self, changes: &[(String, String)]) {
        self.memory.invalidate(changes);
        self.invalidate_records(changes);
    }
}
