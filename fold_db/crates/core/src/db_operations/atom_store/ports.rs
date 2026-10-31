//! Ports: the narrow capabilities that store concepts depend on.
//!
//! A converted concept (the filter, the schema keys, the delete barriers, the
//! marker cleanup) is a struct over `&S`, where `S` implements only the ports
//! the concept lists. Concept code calls these trait methods and pure
//! functions. It never names `AtomStore`, and it never calls another concept
//! except through a port. `AtomStore` implements every port in `wiring/`, by
//! delegation, so the composition stays in one place.
//!
//! Each async method returns a `Send` future. Generic concept code can then
//! box, scope and spawn its own futures exactly as the inherent methods did.
//! The concepts are generic over `S` (static dispatch): no `dyn` hop and no
//! extra allocation on the read and write paths.
//!
//! The ports name model types only (`PerKeyRecord`, `TipChangeSet`, ...).

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use futures::future::BoxFuture;

use super::counter::{TipChangeSet, TipCommit};
use super::hash_selection::HashTipSelection;
use super::types::{MoleculeGenerationPointer, MoleculeHeader, PerKeyRecord};
use crate::atom::delete_barrier::DeleteBarrier;
use crate::atom::{Atom, MoleculeKeyCodec};
use crate::schema::types::key_value::KeyValue;
use crate::schema::SchemaError;
use crate::storage::disk_pack::PackRuntime;
use crate::storage::{traits::KvStore, TypedKvStore};

/// One stored row with its key, as the per-key reads return it.
pub(crate) type KeyedRecord = (String, PerKeyRecord);

/// The main namespace: tips, headers, bodies, barriers and queue rows.
pub(crate) trait MainRows: Sync {
    fn rows(&self) -> &Arc<TypedKvStore<dyn KvStore>>;
}

/// The native runtime of this home, when the home is native.
pub(crate) trait NativeRuntime: Sync {
    fn native_runtime(&self) -> Option<Arc<PackRuntime>>;
}

/// The typed key codec of each molecule.
pub(crate) trait KeyCodecs: Sync {
    fn key_codec_for_molecule(&self, molecule_uuid: &str) -> Result<MoleculeKeyCodec, SchemaError>;
    fn storage_hash(&self, molecule_uuid: &str, api_hash: &str) -> Result<String, SchemaError>;
    fn load_molecule_key_bundle(
        &self,
        molecule_uuid: &str,
    ) -> impl Future<Output = Result<(), SchemaError>> + Send;
}

/// Atom bodies and the read scope of one body read.
pub(crate) trait AtomReads: Sync {
    fn atom_read_scope<T: Send>(
        &self,
        future: impl Future<Output = Result<T, SchemaError>> + Send,
    ) -> impl Future<Output = Result<T, SchemaError>> + Send;
    fn get_atoms(
        &self,
        uuids: &[&str],
        storage_prefix: Option<&str>,
    ) -> impl Future<Output = Result<Vec<Option<Atom>>, SchemaError>> + Send;
    fn body_storage_key(&self, atom: &str, scope: Option<&str>) -> String;
}

/// The write gate of a background job.
pub(crate) trait WriteScopes: Sync {
    fn in_background_scope<'a, T: Send + 'a>(
        &'a self,
        work: impl Future<Output = Result<T, SchemaError>> + Send + 'a,
    ) -> BoxFuture<'a, Result<T, SchemaError>>;
}

/// The atom counter planner: the one writer of tips, bodies and counters.
pub(crate) trait CommitTips: Sync {
    fn commit_tip_changes(
        &self,
        set: TipChangeSet<'_>,
    ) -> impl Future<Output = Result<TipCommit, SchemaError>> + Send;
}

/// Delete barriers that a Delete holds in memory before its durable flush.
pub(crate) trait PendingDeletes: Sync {
    /// The pending barrier of each key, read under one lock, in key order.
    fn pending_delete_barriers_for(&self, keys: &[String]) -> Vec<Option<DeleteBarrier>>;
}

/// The lock that excludes marker cleanup while a generation is prepared.
pub(crate) trait GenerationLocks: Sync {
    fn generation_cleanup_lock(
        &self,
        molecule: &str,
        scope: Option<&str>,
    ) -> Arc<tokio::sync::RwLock<()>>;
}

/// Exact per-key tip reads (the filter concept).
pub(crate) trait PerKeyReads: Sync {
    fn load_exact_per_key_records(
        &self,
        storage_prefix: Option<&str>,
        base_keys: &[String],
    ) -> impl Future<Output = Result<Vec<Option<PerKeyRecord>>, SchemaError>> + Send;
    fn load_visible_per_key_records(
        &self,
        storage_prefix: Option<&str>,
        keys: &[String],
    ) -> impl Future<Output = Result<Vec<Option<PerKeyRecord>>, SchemaError>> + Send;
    fn filter_delete_barriers_for_records(
        &self,
        rows: Vec<KeyedRecord>,
        storage_prefix: Option<&str>,
    ) -> impl Future<Output = Result<Vec<KeyedRecord>, SchemaError>> + Send;
}

/// Durable Delete barrier winners (the delete barrier concept).
pub(crate) trait DeleteBarrierReads: Sync {
    fn winning_delete_barriers(
        &self,
        molecule_keys: &[String],
    ) -> impl Future<Output = Result<HashMap<String, DeleteBarrier>, SchemaError>> + Send;
}

/// Molecule headers and immutable generations (the molecules concept).
pub(crate) trait MoleculeReads: Sync {
    fn active_molecule_generation(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> impl Future<Output = Result<Option<MoleculeGenerationPointer>, SchemaError>> + Send;
    fn merge_generation_prefix_rows(
        &self,
        molecule_uuid: &str,
        base_prefix: &str,
        storage_prefix: Option<&str>,
        live_rows: Vec<KeyedRecord>,
    ) -> impl Future<Output = Result<Vec<KeyedRecord>, SchemaError>> + Send;
    fn merge_generation_range_rows(
        &self,
        molecule_uuid: &str,
        base_start: &str,
        base_end: &str,
        storage_prefix: Option<&str>,
        live_rows: Vec<KeyedRecord>,
    ) -> impl Future<Output = Result<Vec<KeyedRecord>, SchemaError>> + Send;
    fn read_header_exact_prefix<'a>(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&'a str>,
    ) -> impl Future<Output = Result<Option<(MoleculeHeader, Option<&'a str>)>, SchemaError>> + Send;
}

/// Ready key membership in memory (the schema keys concept).
pub(crate) trait ReadyKeys: Sync {
    fn preloaded_window(
        &self,
        molecule: &str,
        selection: &HashTipSelection<'_>,
        scope: Option<&str>,
        include_tombstones: bool,
    ) -> impl Future<Output = Result<Option<Vec<KeyedRecord>>, SchemaError>> + Send;
    fn preloaded_hash_range_keys_page_window(
        &self,
        molecule: &str,
        scope: Option<&str>,
        take: Option<usize>,
        after: Option<(&str, &str)>,
    ) -> impl Future<Output = Result<Option<Vec<(String, String)>>, SchemaError>> + Send;
    fn preloaded_per_key_prefix(
        &self,
        prefix: &str,
        scope: Option<&str>,
    ) -> impl Future<Output = Result<Option<Vec<KeyedRecord>>, SchemaError>> + Send;
    fn preloaded_per_key_prefix_for_api(
        &self,
        prefix: &str,
        scope: Option<&str>,
        api_hash: Option<&str>,
    ) -> impl Future<Output = Result<Option<Vec<KeyedRecord>>, SchemaError>> + Send;
    fn preloaded_per_key_range(
        &self,
        start: &str,
        end: &str,
        scope: Option<&str>,
    ) -> impl Future<Output = Result<Option<Vec<KeyedRecord>>, SchemaError>> + Send;
    fn preloaded_per_key_range_for_api(
        &self,
        start: &str,
        end: &str,
        scope: Option<&str>,
        api_hash: Option<&str>,
    ) -> impl Future<Output = Result<Option<Vec<KeyedRecord>>, SchemaError>> + Send;
}

/// Exact query tips and their warm set receipts (the schema keys concept).
pub(crate) trait QueryTips: Sync {
    fn load_query_tip_records(
        &self,
        scope: Option<&str>,
        molecule: &str,
        keys: &[KeyValue],
    ) -> impl Future<Output = Result<Vec<Option<KeyedRecord>>, SchemaError>> + Send;
    fn note_selected_query_tip(
        &self,
        scope: Option<&str>,
        molecule: &str,
        api_key: &KeyValue,
        record: &PerKeyRecord,
        base: &str,
    ) -> Result<(), SchemaError>;
}
