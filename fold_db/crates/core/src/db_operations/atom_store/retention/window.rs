//! The record of the retention window.
//!
//! An entry of the queue holds the expiry that the node computed from its window
//! (`LASTDB_CONFLICT_RETENTION_SECS`) when it wrote the record. The offline
//! recount runs in a process of its own, with its own environment. A recount that
//! used another window than the node would give a repaired record the wrong
//! expiry, and would list slices that do not line up with the buckets. So the
//! janitor records the window of the node in one row, and the recount reads it.
//!
//! The row is `crb:v1\0params`, in the family of the bucket directory. Its key
//! sorts after every bucket row (`p` follows the digits), so no read of the
//! directory (the reader, the survey, the size count) meets it.
//!
//! Only the janitor writes the row, once at its start and again when the window
//! changes. A node whose janitor is off records nothing, and the recount then
//! uses its own environment.

use super::keys::QUEUE_DIRECTORY;
use crate::db_operations::atom_store::counter::TipChangeSet;
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use serde::{Deserialize, Serialize};
use serde_json::json;

/// The row that records the window.
fn window_key() -> String {
    format!("{QUEUE_DIRECTORY}\0params")
}

#[derive(Deserialize, Serialize)]
struct WindowRow {
    #[serde(rename = "v")]
    version: u8,
    window_secs: u64,
}

impl AtomStore {
    /// The window that the node recorded, in seconds. `None` when the row is
    /// missing or does not decode.
    pub(crate) async fn recorded_retention_window(&self) -> Result<Option<u64>, SchemaError> {
        let stored = self
            .main_store
            .inner()
            .get(window_key().as_bytes())
            .await
            .map_err(|error| SchemaError::InvalidData(format!("read the window row: {error}")))?;
        Ok(stored
            .and_then(|bytes| serde_json::from_slice::<WindowRow>(&bytes).ok())
            .map(|row| row.window_secs))
    }

    /// Record the window of this process, when the row says another one. One
    /// batch of the planner (rows only).
    pub(crate) async fn record_retention_window(&self) -> Result<(), SchemaError> {
        let window_secs = self.retention_state().window_nanos() / 1_000_000_000;
        if self.recorded_retention_window().await? == Some(window_secs) {
            return Ok(());
        }
        let row = json!({ "v": 1, "window_secs": window_secs });
        self.in_background_scope(async {
            self.commit_tip_changes(TipChangeSet {
                rows: vec![(window_key(), row)],
                ..TipChangeSet::default()
            })
            .await
            .map(|_| ())
        })
        .await
    }
}
