//! A blocked or removed schema cannot acquire fresh expiry tracking.

use super::SchemaFormat;
use crate::schema::SchemaState;
use crate::storage::{error::StorageResult, StorageError};

impl SchemaFormat {
    /// Runs inside the same native write scope as the tracking puts. This
    /// also fences owner backfills and moves against an in-progress drop.
    pub(super) async fn require_tracking_writable(&self, schemas: &[String]) -> StorageResult<()> {
        if schemas.is_empty() {
            return Ok(());
        }
        let keys = schemas
            .iter()
            .map(|name| name.as_bytes().to_vec())
            .collect();
        let (states, definitions) = tokio::try_join!(
            self.schema_states_store.get_items::<SchemaState>(schemas),
            self.schemas_store.inner().get_many(keys),
        )?;
        if states.len() != schemas.len() || definitions.len() != schemas.len() {
            return Err(StorageError::BackendError(
                "tracking owner read is incomplete".into(),
            ));
        }
        for ((schema, state), definition) in schemas.iter().zip(states).zip(definitions) {
            if definition.is_none() || state == Some(SchemaState::Blocked) {
                return Err(StorageError::InvalidOperation(format!(
                    "expiry tracking owner {schema} is blocked or not installed"
                )));
            }
        }
        Ok(())
    }
}
