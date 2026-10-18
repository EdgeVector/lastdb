use super::super::atom_store::AtomStore;
use super::super::attribution_ledger::AttributionLedger;
use super::super::change_feed::ChangeFeedStore;
use super::super::db_catalog_store::DbCatalogStore;
use super::super::lineage_index::LineageIndex;
use super::super::metadata_store::MetadataStore;
use super::super::molecule_key_store::MoleculeKeyStore;
use super::super::public_key_store::PublicKeyStore;
use super::super::schema_store::SchemaStore;
use super::{DbOperations, MoleculeGateHoldStats};
use crate::storage::traits::*;
use crate::storage::TypedKvStore;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex};

/// The namespace handles `DbOperationsCore` opens at construction.
struct CoreNamespaces {
    main_kv: Arc<dyn KvStore>,
    metadata_kv: Arc<dyn KvStore>,
    schema_states_kv: Arc<dyn KvStore>,
    schemas_kv: Arc<dyn KvStore>,
    db_catalog_kv: Arc<dyn KvStore>,
    molecule_keys_kv: Arc<dyn KvStore>,
    public_keys_kv: Arc<dyn KvStore>,
    idempotency_kv: Arc<dyn KvStore>,
    superseded_by_kv: Arc<dyn KvStore>,
    schema_index_kv: Arc<dyn KvStore>,
    lineage_forward_kv: Arc<dyn KvStore>,
    lineage_reverse_kv: Arc<dyn KvStore>,
    change_feed_kv: Arc<dyn KvStore>,
    attribution_ledger_kv: Arc<dyn KvStore>,
    keep_small_kv: Arc<dyn KvStore>,
}

impl CoreNamespaces {
    async fn open(store: &Arc<dyn NamespacedStore>) -> Result<Self, crate::storage::StorageError> {
        let main_kv = store.open_namespace("main").await?;
        let metadata_kv = store.open_namespace("metadata").await?;
        let schema_states_kv = store.open_namespace("schema_states").await?;
        let schemas_kv = store.open_namespace("schemas").await?;
        let db_catalog_kv = store.open_namespace("db_catalog").await?;
        let molecule_keys_kv = store.open_namespace("molecule_keys").await?;
        let public_keys_kv = store.open_namespace("public_keys").await?;
        let idempotency_kv = store.open_namespace("idempotency").await?;
        let superseded_by_kv = store.open_namespace("schema_superseded_by").await?;
        let schema_index_kv = store.open_namespace("schema_index").await?;
        // `lineage_forward` / `lineage_reverse` back the derived-molecule
        // lineage index. Skipped by store-level capture per
        // `crate::sync::policy::CAPTURE_SKIP_NAMESPACES`; rebuildable from
        // replay (project 2). Both namespaces are still cloud-backed-up
        // (encrypted since #1153, not at-rest-exempt) — capture-skip does not
        // mean cloud-excluded. No production writers exist until
        // `projects/view-compute-as-mutations` wires them in.
        let lineage_forward_kv = store.open_namespace("lineage_forward").await?;
        let lineage_reverse_kv = store.open_namespace("lineage_reverse").await?;
        let change_feed_kv = store.open_namespace("change_feed").await?;
        let attribution_ledger_kv = store.open_namespace("attribution_ledger").await?;
        // The keep-small snapshot gets its own plane (2026-09-21). It is one
        // whole-map value rewritten every debounce tick, so every put is dead
        // bytes the moment the next one lands. In `metadata` (no automatic
        // compaction; shares hash groups with correctness records) that grew
        // one group to 39 GB and looped the primary. `keep_small` is
        // capture-skipped, backup-excluded and residual-self-compacted, so the
        // plane stays one live value plus whatever the last compact left.
        let keep_small_kv = store
            .open_namespace(super::super::KEEP_SMALL_SNAPSHOT_COLLECTION)
            .await?;
        Ok(Self {
            main_kv,
            metadata_kv,
            schema_states_kv,
            schemas_kv,
            db_catalog_kv,
            molecule_keys_kv,
            public_keys_kv,
            idempotency_kv,
            superseded_by_kv,
            schema_index_kv,
            lineage_forward_kv,
            lineage_reverse_kv,
            change_feed_kv,
            attribution_ledger_kv,
            keep_small_kv,
        })
    }
}

impl DbOperations {
    /// Create logical database operations from a native namespaced store.
    ///
    /// Mini has no in-process native embedding index. Mutation writes still
    /// deliver Search app outbox batches (`db_operations::search_index`).
    pub async fn from_namespaced_store(
        store: Arc<dyn NamespacedStore>,
    ) -> Result<Self, crate::storage::StorageError> {
        Self::from_namespaced_store_with_atom_content_and_hash_key(
            store,
            None,
            crate::atom::MoleculeKeyCodec::plain(),
        )
        .await
    }

    /// Like [`Self::from_namespaced_store_with_atom_content_key`], with an
    /// explicit HashKey storage encoding (design-lastdb-hashkey-blind-v1).
    pub async fn from_namespaced_store_with_atom_content_and_hash_key(
        store: Arc<dyn NamespacedStore>,
        atom_content_key: Option<[u8; 32]>,
        hash_key_codec: crate::atom::MoleculeKeyCodec,
    ) -> Result<Self, crate::storage::StorageError> {
        Self::from_namespaced_store_with_atom_and_molecule_keys_mode(
            store,
            atom_content_key,
            hash_key_codec,
            None,
            true,
        )
        .await
    }

    /// Production constructor with a node key for durable molecule-key wraps.
    pub async fn from_namespaced_store_with_atom_and_molecule_keys(
        store: Arc<dyn NamespacedStore>,
        atom_content_key: Option<[u8; 32]>,
        hash_key_codec: crate::atom::MoleculeKeyCodec,
        molecule_wrap_key: Option<[u8; 32]>,
    ) -> Result<Self, crate::storage::StorageError> {
        Self::from_namespaced_store_with_atom_and_molecule_keys_mode(
            store,
            atom_content_key,
            hash_key_codec,
            molecule_wrap_key,
            true,
        )
        .await
    }

    async fn from_namespaced_store_with_atom_and_molecule_keys_mode(
        store: Arc<dyn NamespacedStore>,
        atom_content_key: Option<[u8; 32]>,
        hash_key_codec: crate::atom::MoleculeKeyCodec,
        molecule_wrap_key: Option<[u8; 32]>,
        admit_meter_trust: bool,
    ) -> Result<Self, crate::storage::StorageError> {
        if store.disk_pack_runtime().is_none() {
            return Err(crate::storage::StorageError::ConfigurationError(
                "LastDB logical operations require native packed storage; old engines are unsupported"
                    .into(),
            ));
        }
        let CoreNamespaces {
            main_kv,
            metadata_kv,
            schema_states_kv,
            schemas_kv,
            db_catalog_kv,
            molecule_keys_kv,
            public_keys_kv,
            idempotency_kv,
            superseded_by_kv,
            schema_index_kv,
            lineage_forward_kv,
            lineage_reverse_kv,
            change_feed_kv,
            attribution_ledger_kv,
            keep_small_kv,
        } = CoreNamespaces::open(&store).await?;

        // Wrap KvStores in TypedKvStore adapters
        let main_store = Arc::new(TypedKvStore::new(main_kv));
        let metadata_typed = Arc::new(TypedKvStore::new(metadata_kv));
        let schema_states_store = Arc::new(TypedKvStore::new(schema_states_kv));
        let schemas_store = Arc::new(TypedKvStore::new(schemas_kv));
        let db_catalog_store = Arc::new(TypedKvStore::new(db_catalog_kv));
        let molecule_keys_store = Arc::new(TypedKvStore::new(molecule_keys_kv));
        let public_keys_typed = Arc::new(TypedKvStore::new(public_keys_kv));
        let idempotency_typed = Arc::new(TypedKvStore::new(idempotency_kv));
        let superseded_by_store = Arc::new(TypedKvStore::new(superseded_by_kv));
        let schema_index_store = Arc::new(TypedKvStore::new(schema_index_kv));

        // Domain stores
        let schema_store = {
            let store = SchemaStore::new(
                schemas_store,
                schema_states_store,
                superseded_by_store,
                store.disk_pack_runtime(),
            );
            match atom_content_key {
                Some(key) => store.with_catalog_unwrap_key(key),
                None => store,
            }
        };
        let db_catalog = DbCatalogStore::new(db_catalog_store, store.disk_pack_runtime());
        let molecule_keys = MoleculeKeyStore::new(
            molecule_keys_store,
            molecule_wrap_key,
            store.disk_pack_runtime(),
        );
        let (atom_store, resident_metrics) = Self::open_atom_store(
            &store,
            main_store,
            schema_index_store,
            keep_small_kv,
            atom_content_key,
            hash_key_codec,
            &molecule_keys,
            &db_catalog,
            admit_meter_trust,
        )
        .await?;
        let public_key_store = PublicKeyStore::new(public_keys_typed);
        let metadata_store = MetadataStore::new(metadata_typed, idempotency_typed);
        let attribution_ledger = AttributionLedger::new(attribution_ledger_kv).await?;
        let change_feed = ChangeFeedStore::new(change_feed_kv, store.disk_pack_runtime()).await?;
        let lineage_index = LineageIndex::new(lineage_forward_kv, lineage_reverse_kv);

        Ok(Self {
            store,
            schema_store,
            db_catalog,
            molecule_keys,
            atom_store,
            public_key_store,
            metadata_store,
            attribution_ledger,
            change_feed,
            lineage_index,
            resident_metrics,
            unresolved_atom_skips: Arc::new(AtomicU64::new(0)),
            unresolved_atom_identities: Arc::new(Mutex::new(HashSet::new())),
            unresolved_identities_capped: Arc::new(AtomicBool::new(false)),
            unresolved_atom_details: Arc::new(Mutex::new(HashMap::new())),
            unresolved_details_capped: Arc::new(AtomicBool::new(false)),
            molecule_gate_hold: Arc::new(MoleculeGateHoldStats::default()),
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn open_atom_store(
        store: &Arc<dyn NamespacedStore>,
        main_store: Arc<TypedKvStore<dyn KvStore>>,
        schema_index_store: Arc<TypedKvStore<dyn KvStore>>,
        keep_small_kv: Arc<dyn KvStore>,
        atom_content_key: Option<[u8; 32]>,
        hash_key_codec: crate::atom::MoleculeKeyCodec,
        molecule_keys: &MoleculeKeyStore,
        db_catalog: &DbCatalogStore,
        admit_meter_trust: bool,
    ) -> Result<(AtomStore, Arc<crate::resident::ResidentMetrics>), crate::storage::StorageError>
    {
        // Resolve the atom body key encoding against the home, not just the
        // environment, and refuse to serve a flat view of a migrated home.
        // Every construction path funnels through here, so this is the one
        // place a boot can pick the wrong addressing.
        let resident_metrics = store
            .disk_pack_runtime()
            .ok_or_else(|| {
                crate::storage::StorageError::ConfigurationError(
                    "logical atom construction requires native packed storage".into(),
                )
            })?
            .resident_metrics();
        let atom_store = AtomStore::new_with_content_and_hash_key_codec(
            main_store,
            schema_index_store,
            atom_content_key,
            hash_key_codec,
        )
        .with_namespaced_store(Arc::clone(store))
        .with_molecule_keys(molecule_keys.clone())
        .with_keep_small_persist(Arc::new(TypedKvStore::new(keep_small_kv)))
        .resolve_boot_encoding()
        .await?;
        Self::initialize_fresh_native_refs(store, &atom_store).await?;
        atom_store
            .recover_catalog_atom_ref_transitions(db_catalog)
            .await
            .map_err(|error| {
                crate::storage::StorageError::BackendError(format!(
                    "recover database-catalog atom references: {error}"
                ))
            })?;
        // Fail-closed meter TRUST, not fail-closed availability. #2186 refused
        // boot on any hydrate error; on 2026-09-25 that made every new build
        // refuse the primary home because its keep_small group (1.08 GB of
        // superseded snapshots) was over the cold-load cap. The meters are
        // gauge state, rebuildable from authoritative records: a failed
        // hydrate leaves every domain incomplete (reports stay incomplete,
        // meter-dependent deletion stays closed) and the node boots. Tom's
        // decision: decision-2026-09-25-lastdb-node-stays-upgradeable-soft-hard-cap.
        if admit_meter_trust {
            if let Err(e) = atom_store.hydrate_keep_small().await {
                atom_store
                    .note_keep_small_hydrate_failure(&e.to_string())
                    .await;
                tracing::warn!(
                    target: "fold_node::database",
                    error = %e,
                    "LASTDB_KEEP_SMALL_HYDRATE_FAILED meters marked incomplete; boot continues \
                     (reports stay incomplete and meter-dependent deletion stays closed until \
                     the liveness bootstrap re-measures)"
                );
            }
        }
        Ok((atom_store, resident_metrics))
    }

    async fn initialize_fresh_native_refs(
        store: &Arc<dyn NamespacedStore>,
        atoms: &AtomStore,
    ) -> Result<(), crate::storage::StorageError> {
        let Some(runtime) = store.disk_pack_runtime() else {
            return Ok(());
        };
        crate::storage::disk_pack::transaction::scope(Some(Arc::clone(&runtime)), async {
            if runtime.claim_fresh_empty_boundary() {
                // The native constructor proves that no atom or molecule
                // exists. Use the normal typed/encrypted store and complete
                // publication; never infer completeness from an absent marker.
                atoms
                    .bootstrap_molecule_ref_edges_on_isolated_copy(&[], 0, 0, None)
                    .await
                    .map_err(|error| {
                        crate::storage::StorageError::BackendError(format!(
                            "initialize known empty native molecule references: {error}"
                        ))
                    })?;
            }
            Ok(())
        })
        .await
    }
}
