//! Every public body read keeps one complete boundary through locator fallback.

use super::*;
use crate::atom::AtomPartition;
use futures::FutureExt;

impl AtomStore {
    /// Read one Atom, preserving its physical hint and UUID-only fallback.
    pub async fn get_atom_by_uuid_in_partition(
        &self,
        uuid: &str,
        hint: Option<&AtomPartition>,
        scope: Option<&str>,
    ) -> Result<Option<Atom>, SchemaError> {
        self.atom_read_scope(self.get_atom_by_uuid_captured(uuid, hint, scope).boxed())
            .await
    }

    /// Read one located batch at a common memory and disk boundary.
    pub async fn get_atoms_located(
        &self,
        slots: &[(&str, Option<AtomPartition>)],
        scope: Option<&str>,
    ) -> Result<Vec<Option<Atom>>, SchemaError> {
        self.atom_read_scope(self.get_atoms_located_captured(slots, scope).boxed())
            .await
    }

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
