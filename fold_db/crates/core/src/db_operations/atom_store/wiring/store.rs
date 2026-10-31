//! Ports that delegate to store primitives the trial did not convert.

use std::future::Future;
use std::sync::Arc;

use futures::future::BoxFuture;

use super::super::counter::{TipChangeSet, TipCommit};
use super::super::ports::{
    AtomReads, CommitTips, GenerationLocks, KeyCodecs, KeyedRecord, MainRows, MoleculeReads,
    NativeRuntime, PendingDeletes, WriteScopes,
};
use super::super::types::{MoleculeGenerationPointer, MoleculeHeader};
use super::super::AtomStore;
use crate::atom::delete_barrier::DeleteBarrier;
use crate::atom::{Atom, MoleculeKeyCodec};
use crate::schema::SchemaError;
use crate::storage::disk_pack::PackRuntime;
use crate::storage::{traits::KvStore, TypedKvStore};

impl MainRows for AtomStore {
    fn rows(&self) -> &Arc<TypedKvStore<dyn KvStore>> {
        self.raw()
    }
}

impl NativeRuntime for AtomStore {
    fn native_runtime(&self) -> Option<Arc<PackRuntime>> {
        self.namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime())
    }
}

impl KeyCodecs for AtomStore {
    fn key_codec_for_molecule(&self, molecule_uuid: &str) -> Result<MoleculeKeyCodec, SchemaError> {
        AtomStore::key_codec_for_molecule(self, molecule_uuid)
    }

    fn storage_hash(&self, molecule_uuid: &str, api_hash: &str) -> Result<String, SchemaError> {
        AtomStore::storage_hash(self, molecule_uuid, api_hash)
    }

    async fn load_molecule_key_bundle(&self, molecule_uuid: &str) -> Result<(), SchemaError> {
        AtomStore::load_molecule_key_bundle(self, molecule_uuid).await
    }
}

impl AtomReads for AtomStore {
    async fn atom_read_scope<T: Send>(
        &self,
        future: impl Future<Output = Result<T, SchemaError>> + Send,
    ) -> Result<T, SchemaError> {
        AtomStore::atom_read_scope(self, future).await
    }

    async fn get_atoms(
        &self,
        uuids: &[&str],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<Option<Atom>>, SchemaError> {
        AtomStore::get_atoms(self, uuids, storage_prefix).await
    }

    fn body_storage_key(&self, atom: &str, scope: Option<&str>) -> String {
        AtomStore::body_storage_key(self, atom, scope)
    }
}

impl WriteScopes for AtomStore {
    fn in_background_scope<'a, T: Send + 'a>(
        &'a self,
        work: impl Future<Output = Result<T, SchemaError>> + Send + 'a,
    ) -> BoxFuture<'a, Result<T, SchemaError>> {
        AtomStore::in_background_scope(self, work)
    }
}

impl CommitTips for AtomStore {
    async fn commit_tip_changes(&self, set: TipChangeSet<'_>) -> Result<TipCommit, SchemaError> {
        AtomStore::commit_tip_changes(self, set).await
    }
}

impl PendingDeletes for AtomStore {
    fn pending_delete_barriers_for(&self, keys: &[String]) -> Vec<Option<DeleteBarrier>> {
        let guard = self
            .pending_delete_barriers
            .lock()
            .expect("pending_delete_barriers poisoned");
        keys.iter().map(|key| guard.get(key).cloned()).collect()
    }
}

impl GenerationLocks for AtomStore {
    fn generation_cleanup_lock(
        &self,
        molecule: &str,
        scope: Option<&str>,
    ) -> Arc<tokio::sync::RwLock<()>> {
        AtomStore::generation_cleanup_lock(self, molecule, scope)
    }
}

impl MoleculeReads for AtomStore {
    async fn active_molecule_generation(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<MoleculeGenerationPointer>, SchemaError> {
        AtomStore::active_molecule_generation(self, molecule_uuid, storage_prefix).await
    }

    async fn merge_generation_prefix_rows(
        &self,
        molecule_uuid: &str,
        base_prefix: &str,
        storage_prefix: Option<&str>,
        live_rows: Vec<KeyedRecord>,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        AtomStore::merge_generation_prefix_rows(
            self,
            molecule_uuid,
            base_prefix,
            storage_prefix,
            live_rows,
        )
        .await
    }

    async fn merge_generation_range_rows(
        &self,
        molecule_uuid: &str,
        base_start: &str,
        base_end: &str,
        storage_prefix: Option<&str>,
        live_rows: Vec<KeyedRecord>,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        AtomStore::merge_generation_range_rows(
            self,
            molecule_uuid,
            base_start,
            base_end,
            storage_prefix,
            live_rows,
        )
        .await
    }

    async fn read_header_exact_prefix<'a>(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&'a str>,
    ) -> Result<Option<(MoleculeHeader, Option<&'a str>)>, SchemaError> {
        AtomStore::read_header_exact_prefix(self, molecule_uuid, storage_prefix).await
    }
}
