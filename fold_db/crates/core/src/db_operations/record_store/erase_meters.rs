//! Audit facts join the native erase through the format writer.

use super::RecordStore;
use crate::db_operations::RecordDeleteLedgerEntry;
use crate::schema::SchemaError;

impl RecordStore {
    pub(crate) async fn record_record_delete_audit(
        &self,
        entry: RecordDeleteLedgerEntry,
    ) -> Result<(), SchemaError> {
        self.format.record_delete_audit(entry).await
    }
}
