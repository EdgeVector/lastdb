//! The home-level proof that every stored schema owns its record values.
//!
//! Releases with cross-schema value sharing stored schemas that read values
//! through another schema. Startup refuses such a home. Until this marker,
//! the only proof of the owned layout was the per-schema `independent_fields`
//! flag. A serving node now writes this marker once, after startup admitted
//! every stored schema of the home. Startup then needs no per-schema flag for
//! a home with the marker, so a later release can stop writing the flag. A
//! home without the marker still gets the per-schema check.
//!
//! The marker is one key in the main namespace, beside the other `layout:`
//! records. The byte backup and the restore copy it with the home.

use super::AtomStore;
use crate::storage::{KvMutation, StorageError};

/// Presence is the proof. The value only names the marker version.
pub(crate) const OWNED_LAYOUT_KEY: &str = "layout:owned-values";
const OWNED_LAYOUT_VALUE: &[u8] = b"v1";

impl AtomStore {
    /// True when a serving node already admitted every stored schema of this
    /// home as owned and recorded that fact.
    pub(crate) async fn owned_layout_recorded(&self) -> Result<bool, StorageError> {
        Ok(self
            .main_store
            .inner()
            .get(OWNED_LAYOUT_KEY.as_bytes())
            .await?
            .is_some())
    }

    /// Write the marker when it is absent. Call this only after startup
    /// admitted every stored schema, and never from offline maintenance.
    /// Returns true when this call wrote the marker.
    pub(crate) async fn record_owned_layout(&self) -> Result<bool, StorageError> {
        if self.owned_layout_recorded().await? {
            return Ok(false);
        }
        self.main_store
            .inner()
            .batch_mutate(vec![KvMutation::put(
                OWNED_LAYOUT_KEY.as_bytes(),
                OWNED_LAYOUT_VALUE,
            )])
            .await?;
        self.main_store.inner().flush().await?;
        Ok(true)
    }
}
