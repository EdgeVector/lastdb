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
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex};

mod gate_hold;
mod open;
mod row_drops;
mod unresolved;

pub use gate_hold::{MoleculeGateHold, MoleculeGateHoldStats};
pub use row_drops::{
    note_page_key_forms, note_tombstoned_row, with_query_row_drop_tally, KeyForm, QueryRowDrops,
};
pub(crate) use unresolved::UnresolvedAtomContext;
use unresolved::UnresolvedAtomIdentityMap;
pub use unresolved::{UnresolvedAtomIdentity, UnresolvedAtomIdentityReport, UnresolvedDamage};

/// Database operations with pluggable storage backend
///
/// Uses the storage abstraction layer (Last Store locally, with optional cloud sync).
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
    /// Per-molecule content/index/OPE keys and access-domain wraps.
    molecule_keys: MoleculeKeyStore,
    /// Main namespace — atoms, molecules, mutation events, sync conflicts
    atom_store: AtomStore,
    /// System-wide public key
    public_key_store: PublicKeyStore,
    /// Metadata + idempotency + process results
    metadata_store: MetadataStore,
    /// Node-local, rebuildable proof rows for an isolated attribution migration.
    attribution_ledger: AttributionLedger,
    /// Durable app change feed. Product rows remain authoritative. Skipped by
    /// store-level capture (`crate::sync::policy::CAPTURE_SKIP_NAMESPACES`),
    /// but the chunk-backup plane does not consult that list, so this still
    /// rides cloud backup sealed (`ENC:`).
    change_feed: ChangeFeedStore,
    /// Forward + reverse lineage indexes for derived molecules. Both
    /// backing namespaces (`lineage_forward`, `lineage_reverse`) are skipped
    /// by store-level capture via
    /// `crate::sync::policy::CAPTURE_SKIP_NAMESPACES`, but still ride cloud
    /// backup (encrypted, not at-rest-exempt).
    /// Scaffolding only; no production call sites yet (PR 6 of
    /// `projects/molecule-provenance-dag`).
    lineage_index: LineageIndex,

    /// T0 resident graph — rehydrate miss / hit for schema·tip·atom·blob.
    /// Product law: `concepts-lastdb-rehydrate` (memory → disk → cloud).
    resident: Arc<crate::resident::ResidentGraph>,
    /// Cumulative count of query rows skipped because a tip referenced an atom
    /// body that could not be resolved. `/api/query` reads a before/after delta
    /// around one query and reports it as `unresolved_rows`.
    ///
    /// This counts *events*, not damaged rows: one broken edge re-read a
    /// thousand times bumps this a thousand times. Pair it with
    /// [`Self::unresolved_atom_distinct`] before drawing a conclusion about
    /// how much is actually broken.
    unresolved_atom_skips: Arc<AtomicU64>,
    /// Distinct `(atom_uuid, key)` identities behind `unresolved_atom_skips`,
    /// bounded by [`UNRESOLVED_IDENTITY_CAP`].
    ///
    /// The event counter alone cannot distinguish "four rows are broken and a
    /// hot client re-reads them" from "we are losing rows and it is
    /// accelerating" — the first is a trivial repair, the second is an
    /// incident. On the primary those looked identical until the daemon log
    /// was grouped by hand.
    ///
    /// An identity is one broken *field* tip, not one row: group by the `key`
    /// half for a row count. [`UnresolvedDamage`] reports both.
    unresolved_atom_identities: Arc<Mutex<HashSet<(String, String)>>>,
    /// Set once the identity set hits the cap, so the reported distinct count
    /// can be labelled a floor rather than a total.
    unresolved_identities_capped: Arc<AtomicBool>,
    /// Exact structured keys for owner diagnosis. Kept separate so the
    /// existing status gauge retains its historical dedup contract.
    unresolved_atom_details: Arc<Mutex<UnresolvedAtomIdentityMap>>,
    unresolved_details_capped: Arc<AtomicBool>,
    /// How long molecule write gates are held — the other end of the
    /// `molecule_gate` wait phase. See [`MoleculeGateHoldStats`].
    molecule_gate_hold: Arc<MoleculeGateHoldStats>,
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

    /// Access the resident graph (T0: resolve / rehydrate / apply).
    pub fn resident(&self) -> &Arc<crate::resident::ResidentGraph> {
        &self.resident
    }

    /// Override the atom body key encoding for the whole node.
    ///
    /// Production reads the encoding from the environment once, when the atom
    /// store is constructed, so a running node never straddles two encodings.
    /// This seam exists so a caller that owns the whole `DbOperations` — the
    /// migration driver, and the tests that prove the read paths under
    /// [`crate::atom::AtomKeyEncoding::PartitionPrefix`] — can name the
    /// encoding explicitly instead of mutating process-global state.
    #[must_use]
    pub fn with_atom_key_encoding(mut self, encoding: crate::atom::AtomKeyEncoding) -> Self {
        self.atom_store = self.atom_store.with_atom_key_encoding(encoding);
        self
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

    /// Access the derived-molecule lineage index (forward + reverse, local-only).
    pub fn lineage(&self) -> &LineageIndex {
        &self.lineage_index
    }

    /// Borrow the primary namespaced store handle.
    pub fn namespaced_store(&self) -> Arc<dyn NamespacedStore> {
        Arc::clone(&self.store)
    }

    /// Cold shard loads since open — one relaxed atomic load, safe to call on
    /// the per-request telemetry path. `None` on backends without hash groups.
    pub fn cold_shard_loads(&self) -> Option<u64> {
        self.store.cold_shard_loads()
    }

    /// Ids stepped over by keys-only walks since open — the scan sensor.
    ///
    /// One relaxed atomic load. A read path that claims to be scan-free can be
    /// pinned by asserting this counter does not move across the call.
    pub fn walk_ids_visited(&self) -> Option<u64> {
        self.store.walk_ids_visited()
    }

    /// Full read-cost picture (cold loads + warm residency vs budget).
    ///
    /// Takes the warm-set lock — sampler and `/api/status` only. Use
    /// [`Self::cold_shard_loads`] on hot paths.
    pub fn read_cost(&self) -> Option<crate::storage::traits::ReadCostStats> {
        self.store.read_cost()
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
        // Apply `durable_through()` per token. An Ok flush does not clear
        // every dirty bit: a token above the seal stays dirty.
        if let (Some(store), Some(set)) = (
            self.store.raw_last_store(),
            self.store.logical_resident_set(),
        ) {
            crate::storage::laststore::logical_path::apply_durable_through(&store, &set);
        }
        Ok(())
    }

    /// One barrier for the groups this batch wrote.
    ///
    /// The namespace stores share one LastStore. Eight scoped flushes would
    /// still be eight barriers. `written` is deduped here; fanout siblings
    /// are not part of it.
    pub async fn flush_dirty_scope(
        &self,
        written: &[laststore::ShardKey],
    ) -> Result<(), SchemaError> {
        let mut scope = written.to_vec();
        scope.sort_unstable();
        scope.dedup();
        let last_store = self.store.raw_last_store();
        // No LastStore means no group barrier. `flush_written_keys` then
        // defaults to a no-op, and a Durable receipt would return while the
        // idempotency row is still pending. The namespace flush is that barrier.
        let Some(store) = last_store else {
            return self.flush().await;
        };
        let foreign = crate::durable_flush::foreign_group_count(&scope, written);
        store.set_flush_foreign_groups(foreign);
        crate::durable_flush::warn_if_flush_foreign(foreign);
        self.store
            .flush_written_keys(&scope)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("scoped flush failed: {error}")))?;
        Ok(())
    }

    /// The LastStore group that holds `key` of namespace `name` (pure; no read or
    /// write). `None` without a LastStore or for a `b64:` key: use [`Self::flush`].
    #[must_use]
    pub fn namespace_row_group(&self, name: &str, key: &str) -> Option<laststore::ShardKey> {
        let store = self.store.raw_last_store()?;
        (!key.starts_with("b64:")).then(|| store.shard_key_of(name, key))
    }
}
