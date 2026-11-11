//! Erase audit rows and the frozen key-format fence.

use super::RecordFormat;
use crate::schema::SchemaError;
use crate::storage::format::{
    control, CURRENT_KEY_FORMAT_KEY, DELETE_AUDIT_FAMILY, DELETE_REPLAY_CUTOFF_KEY,
};
use crate::storage::StorageError;
use serde::{Deserialize, Serialize};

/// One audit row per erase batch. The row joins the native erase atomically,
/// so a stored row always names a committed erase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordDeleteLedgerEntry {
    /// The retained delete or purge verb.
    pub verb: String,
    /// When the batch was planned (RFC3339); the key is stamped when staged.
    pub at: String,
    /// Free-form caller provenance tag (not authenticated identity).
    pub caller: String,
    /// The schema whose records were erased.
    pub schema: String,
    /// SHA-256 hex over `{schema}\0{described key}`.
    pub key_fingerprint: String,
    /// Records the batch erased.
    pub records_erased: u64,
    /// Erased records that had a file.
    pub file_records_erased: u64,
}

impl RecordFormat {
    pub(in crate::storage::format) async fn require_current_key_format(
        &self,
    ) -> Result<(), StorageError> {
        let controls = self
            .main_store
            .get_many(vec![
                CURRENT_KEY_FORMAT_KEY.as_bytes().to_vec(),
                DELETE_REPLAY_CUTOFF_KEY.as_bytes().to_vec(),
            ])
            .await?;
        if controls.len() != 2 {
            return Err(control::invalid("incomplete fence read"));
        }
        if !controls[0]
            .as_deref()
            .is_some_and(lastdb_model::field_key_codec::current_key_format)
        {
            return Err(StorageError::InvalidOperation(
                "the logical key format does not match this node home".into(),
            ));
        }
        control::validate_frozen_replay_floor(controls[0].as_deref(), controls[1].as_deref())
            .map(|_| ())
    }

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
