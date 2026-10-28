use super::atom_store::AtomStore;
use super::attribution_ledger::AttributionLedger;
use super::change_feed::ChangeFeedStore;
use super::db_catalog_store::DbCatalogStore;
use super::lineage_index::LineageIndex;
use super::metadata_store::MetadataStore;
use super::molecule_key_store::MoleculeKeyStore;
use super::public_key_store::PublicKeyStore;
use super::schema_store::SchemaStore;
use crate::schema::SchemaError;
use crate::storage::traits::*;
use std::sync::Arc;

mod open;
mod row_drops;
mod unresolved;

pub use row_drops::{
    note_page_key_forms, note_tombstoned_row, with_query_row_drop_tally,
    with_refilled_query_row_drops, KeyForm, QueryRowDrops,
};
pub use unresolved::{UnresolvedAtomIdentity, UnresolvedAtomIdentityReport, UnresolvedDamage};

/// Database operations with pluggable storage backend
///
/// Uses the storage abstraction layer (Last Store).
///
/// All persistence is encapsulated in domain store structs whose
/// namespace fields are private. External callers reach them through
/// `schemas()`, `atoms()`, `public_keys()`, and `metadata()`.
#[derive(Clone)]
pub struct DbOperations {
    /// Primary namespace backend used to open side stores that are not one of
    /// the pre-wired domain stores below.
    store: Arc<dyn NamespacedStore>,
    /// Schema / schema-state / superseded-by namespaces
    schema_store: SchemaStore,
    /// Database/schema references into the node-universal molecule store.
    db_catalog: DbCatalogStore,
    /// Per-molecule key bundles and access-domain wraps.
    molecule_keys: MoleculeKeyStore,
    /// Main namespace — atoms, molecules, mutation events, sync conflicts
    atom_store: AtomStore,
    /// System-wide public key
    public_key_store: PublicKeyStore,
    /// Metadata + idempotency + process results
    metadata_store: MetadataStore,
    /// Node-local, rebuildable proof rows for an isolated attribution migration.
    attribution_ledger: AttributionLedger,
    /// Durable app change feed. Product rows remain authoritative. It rides
    /// cloud backup sealed (`ENC:`).
    change_feed: ChangeFeedStore,
    /// Forward and reverse lineage namespaces. Shutdown flushes them.
    /// No caller writes or reads the index.
    lineage_index: LineageIndex,

    /// Background purge counters shared with the native runtime.
    persist_metrics: Arc<crate::resident::PersistLaneMetrics>,
}

impl DbOperations {
    // ===== Domain store accessors (public) =====

    /// Access the schema domain store.
    pub fn schemas(&self) -> &SchemaStore {
        &self.schema_store
    }

    /// Access the exact-key database catalog.
    pub fn db_catalog(&self) -> &DbCatalogStore {
        &self.db_catalog
    }

    /// Access durable per-molecule key bundles and domain wraps.
    pub fn molecule_keys(&self) -> &MoleculeKeyStore {
        &self.molecule_keys
    }

    /// Access the durable node-local app change feed.
    pub fn change_feed(&self) -> &ChangeFeedStore {
        &self.change_feed
    }

    /// Access the atom domain store.
    pub fn atoms(&self) -> &AtomStore {
        &self.atom_store
    }

    /// Access background purge counters, separate from logical record counts.
    pub fn persist_metrics(&self) -> &Arc<crate::resident::PersistLaneMetrics> {
        &self.persist_metrics
    }

    /// Logical queries require one captured core state and matching disk root.
    pub async fn read_scope(
        &self,
    ) -> crate::storage::error::StorageResult<crate::storage::disk_pack::PackReadScope> {
        crate::storage::disk_pack::PackReadScope::for_store(&self.store)
            .await?
            .ok_or_else(|| {
                crate::storage::StorageError::ConfigurationError(
                    "logical reads require native packed storage".into(),
                )
            })
    }

    /// Access the system public-key domain store.
    pub fn public_keys(&self) -> &PublicKeyStore {
        &self.public_key_store
    }

    /// Access the metadata / idempotency / process-results domain store.
    pub fn metadata(&self) -> &MetadataStore {
        &self.metadata_store
    }

    /// Access copy-migration attribution proof rows.
    pub fn attribution(&self) -> &AttributionLedger {
        &self.attribution_ledger
    }

    /// Borrow the primary namespaced store handle.
    pub fn namespaced_store(&self) -> Arc<dyn NamespacedStore> {
        Arc::clone(&self.store)
    }

    /// Most operations that one complete batch holds on a native home. `None`
    /// when no native home backs this store.
    #[must_use]
    pub fn batch_operation_limit(&self) -> Option<usize> {
        self.store
            .disk_pack_runtime()
            .map(|runtime| runtime.batch_operation_limit())
    }

    /// Open a side namespace on the primary storage backend.
    pub async fn open_namespace(
        &self,
        name: &str,
    ) -> Result<Arc<dyn KvStore>, crate::storage::StorageError> {
        self.store.open_namespace(name).await
    }

    /// Flush all pending writes to durable storage
    pub async fn flush(&self) -> Result<(), SchemaError> {
        self.schema_store.flush().await?;
        self.db_catalog.flush().await?;
        self.molecule_keys.flush().await?;
        self.atom_store.flush().await?;
        self.public_key_store.flush().await?;
        self.metadata_store.flush().await?;
        self.attribution_ledger.flush().await?;
        self.lineage_index.flush().await?;
        Ok(())
    }
}
