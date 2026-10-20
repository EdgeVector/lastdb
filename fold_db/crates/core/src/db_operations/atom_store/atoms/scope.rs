//! Every public body read keeps one complete memory and disk boundary.

use super::*;
use futures::FutureExt;

impl AtomStore {
    /// Read exact physical keys, with normal content opening and source receipts.
    pub async fn get_atoms_by_storage_keys(
        &self,
        keys: &[String],
    ) -> Result<Vec<Option<Atom>>, SchemaError> {
        self.atom_read_scope(self.get_atoms_by_storage_keys_captured(keys).boxed())
            .await
    }

    pub(in crate::db_operations::atom_store) async fn atom_read_scope<T>(
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
