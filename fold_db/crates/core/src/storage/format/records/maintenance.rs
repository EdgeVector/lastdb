//! In-request delete audit writes.

use super::{RecordDeleteLedgerEntry, RecordFormat};
use crate::schema::SchemaError;
use crate::storage::format::DELETE_AUDIT_FAMILY;
use crate::storage::StorageError;

impl RecordFormat {
    pub(crate) async fn record_delete_audit(
        &self,
        entry: RecordDeleteLedgerEntry,
    ) -> Result<(), SchemaError> {
        self.require_scope()?;
        let ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0).max(0);
        let key = lastdb_model::kind_partition::anchored(
            DELETE_AUDIT_FAMILY,
            &format!("{ts:020}:{}", uuid::Uuid::new_v4()),
        );
        let bytes = serde_json::to_vec(&entry)
            .map_err(|error| StorageError::SerializationError(error.to_string()))?;
        self.main_store
            .put(key.as_bytes(), bytes)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("stage erase audit: {error}")))
    }
}
