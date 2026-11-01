//! The erase audit row joins the same native snapshot as the removed data.

use super::*;
use crate::db_operations::AtomDeleteLedgerEntry;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use chrono::Utc;

impl RecordStore {
    /// Stage final audit facts with the native erase. Failure aborts the scope.
    pub(crate) async fn record_atom_delete_audit(
        &self,
        storage_prefix: Option<&str>,
        mut entry: AtomDeleteLedgerEntry,
    ) -> Result<(), SchemaError> {
        let runtime = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime());
        if !crate::storage::disk_pack::transaction::publication::active(runtime) {
            return Err(SchemaError::InvalidData(
                "erase audit requires a complete native write scope".into(),
            ));
        }
        let ts = Utc::now().timestamp_nanos_opt().unwrap_or(0).max(0);
        let base = lastdb_model::kind_partition::anchored(
            "dellog",
            &format!("{ts:020}:{}", uuid::Uuid::new_v4()),
        );
        let key = build_storage_key(storage_prefix, &base);
        entry.committed = true;
        self.raw()
            .put_item(&key, &entry)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("stage erase audit: {error}")))
    }
}
