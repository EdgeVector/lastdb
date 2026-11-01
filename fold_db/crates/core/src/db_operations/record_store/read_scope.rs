//! One complete memory and disk boundary for a read of the atom store.

use super::RecordStore;
use crate::schema::SchemaError;

impl RecordStore {
    /// Run `future` inside one pack read scope of this store, when the store
    /// has a disk pack. Every read inside sees the same memory and disk state.
    pub(in crate::db_operations::record_store) async fn atom_read_scope<T>(
        &self,
        future: impl std::future::Future<Output = Result<T, SchemaError>>,
    ) -> Result<T, SchemaError> {
        let Some(store) = self.namespaced_store.as_ref() else {
            return future.await;
        };
        let Some(scope) = crate::storage::disk_pack::PackReadScope::for_store(store).await? else {
            return future.await;
        };
        scope.run_result(future).await
    }
}
