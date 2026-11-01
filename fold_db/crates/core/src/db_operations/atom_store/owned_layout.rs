//! The home-level owned-layout marker, kept only for rollback.
//!
//! #901 added this marker as the startup proof that every stored schema owns
//! its record values. This release opens only format 7 homes, and the format
//! is the proof, so this release never reads the marker. Programs from #911
//! to #941 read format 7 and still refuse a stored schema that has neither the
//! marker, the per-schema `independent_fields` flag, nor alternate keys. This
//! release writes no flag. So a serving node still writes the marker once, and
//! a home that this release created can roll back to one of those programs.
//! Delete this file and its call in `FoldDB::initialize_from_init` when no
//! rollback target is older than this release.
//!
//! The marker is one key in the main namespace, beside the other `layout:`
//! records. The byte backup and the restore copy it with the home.

use super::AtomStore;
use crate::storage::{KvMutation, StorageError};

/// Presence is the proof. The value only names the marker version.
const OWNED_LAYOUT_KEY: &str = "layout:owned-values";
const OWNED_LAYOUT_VALUE: &[u8] = b"v1";

impl AtomStore {
    /// Write the marker when it is absent. Call this only after startup
    /// admitted every stored schema, and never from offline maintenance.
    /// Returns true when this call wrote the marker.
    pub(crate) async fn record_owned_layout(&self) -> Result<bool, StorageError> {
        let store = self.main_store.inner();
        if store.get(OWNED_LAYOUT_KEY.as_bytes()).await?.is_some() {
            return Ok(false);
        }
        store
            .batch_mutate(vec![KvMutation::put(
                OWNED_LAYOUT_KEY.as_bytes(),
                OWNED_LAYOUT_VALUE,
            )])
            .await?;
        store.flush().await?;
        Ok(true)
    }
}
