//! One complete memory and disk boundary for a read of the record store.

use super::RecordStore;
use crate::schema::SchemaError;

impl RecordStore {
    /// Run `future` inside one pack read scope of this store. Every read
    /// inside sees the same memory and disk state.
    pub(in crate::db_operations::record_store) async fn record_read_scope<T>(
        &self,
        future: impl std::future::Future<Output = Result<T, SchemaError>>,
    ) -> Result<T, SchemaError> {
        crate::storage::disk_pack::PackReadScope::for_store(&self.namespaced_store)
            .await?
            .run_result(future)
            .await
    }
}
