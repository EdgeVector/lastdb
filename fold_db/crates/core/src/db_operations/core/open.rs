use super::super::attribution_ledger::AttributionLedger;
use super::super::change_feed::ChangeFeedStore;
use super::super::db_catalog_store::DbCatalogStore;
use super::super::metadata_store::MetadataStore;
use super::super::public_key_store::PublicKeyStore;
use super::super::record_store::RecordStore;
use super::super::schema_store::SchemaStore;
use super::DbOperations;
use crate::storage::field_key_store::FieldKeyStore;
use crate::storage::traits::*;
use crate::storage::TypedKvStore;
use std::sync::Arc;

/// The namespace handles `DbOperationsCore` opens at construction.
struct CoreNamespaces {
    main_kv: Arc<dyn KvStore>,
    metadata_kv: Arc<dyn KvStore>,
    schema_states_kv: Arc<dyn KvStore>,
    schemas_kv: Arc<dyn KvStore>,
    db_catalog_kv: Arc<dyn KvStore>,
    field_keys_kv: Arc<dyn KvStore>,
    public_keys_kv: Arc<dyn KvStore>,
    idempotency_kv: Arc<dyn KvStore>,
    superseded_by_kv: Arc<dyn KvStore>,
    change_feed_kv: Arc<dyn KvStore>,
    attribution_ledger_kv: Arc<dyn KvStore>,
    keep_small_kv: Arc<dyn KvStore>,
}

/// The namespace handles required to construct the record store.
struct RecordNamespaces {
    main: Arc<TypedKvStore<dyn KvStore>>,
    keep_small: Arc<TypedKvStore<dyn KvStore>>,
}

impl RecordNamespaces {
    fn new(main: Arc<dyn KvStore>, keep_small: Arc<dyn KvStore>) -> Self {
        Self {
            main: Arc::new(TypedKvStore::new(main)),
            keep_small: Arc::new(TypedKvStore::new(keep_small)),
        }
    }
}

impl CoreNamespaces {
    async fn open(store: &Arc<dyn NamespacedStore>) -> Result<Self, crate::storage::StorageError> {
        let main_kv = store.open_namespace("main").await?;
        let metadata_kv = store.open_namespace("metadata").await?;
        let schema_states_kv = store.open_namespace("schema_states").await?;
        let schemas_kv = store.open_namespace("schemas").await?;
        let db_catalog_kv = store.open_namespace("db_catalog").await?;
        let field_keys_kv = store.open_namespace("field_keys").await?;
        let public_keys_kv = store.open_namespace("public_keys").await?;
        let idempotency_kv = store.open_namespace("idempotency").await?;
        let superseded_by_kv = store.open_namespace("schema_superseded_by").await?;
        let change_feed_kv = store.open_namespace("change_feed").await?;
        let attribution_ledger_kv = store.open_namespace("attribution_ledger").await?;
        // The keep-small snapshot gets its own plane (2026-09-21). The snapshot
        // (a small header plus shards) is rewritten on each debounce tick, so
        // the old bytes are dead when the next put lands. Under the LastStore
        // segment engine (removed with disk format v1, decision D1) that value in
        // `metadata` grew one shared hash group to 39 GB and looped the
        // primary. In disk format v2 the plane is part of the main run home,
        // so the V2 byte backup, when on, copies it with the other run files.
        let keep_small_kv = store
            .open_namespace(super::super::KEEP_SMALL_SNAPSHOT_COLLECTION)
            .await?;
        Ok(Self {
            main_kv,
            metadata_kv,
            schema_states_kv,
            schemas_kv,
            db_catalog_kv,
            field_keys_kv,
            public_keys_kv,
            idempotency_kv,
            superseded_by_kv,
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
        Self::from_namespaced_store_with_content_and_field_keys_mode(store, None, None, true).await
    }

    /// Production constructor with a node key for durable field-key wraps.
    ///
    /// `content_key` is the account E2E content key. It seals local file
    /// blobs and is the key that `content_key_proof` checks on each open.
    /// The wrap key is also the `content_key_proof` key when `content_key` is
    /// `None`. Each field key's key bundle blinds its hashes (`blind_v1`).
    pub async fn from_namespaced_store_with_content_and_field_keys(
        store: Arc<dyn NamespacedStore>,
        content_key: Option<[u8; 32]>,
        field_key_wrap_key: Option<[u8; 32]>,
    ) -> Result<Self, crate::storage::StorageError> {
        Self::from_namespaced_store_with_content_and_field_keys_mode(
            store,
            content_key,
            field_key_wrap_key,
            true,
        )
        .await
    }

    async fn from_namespaced_store_with_content_and_field_keys_mode(
        store: Arc<dyn NamespacedStore>,
        content_key: Option<[u8; 32]>,
        field_key_wrap_key: Option<[u8; 32]>,
        admit_meter_trust: bool,
    ) -> Result<Self, crate::storage::StorageError> {
        let runtime = store.disk_pack_runtime().ok_or_else(|| {
            crate::storage::StorageError::ConfigurationError(
                "LastDB logical operations require native packed storage; old engines are unsupported"
                    .into(),
            )
        })?;
        let persist_metrics = runtime.persist_metrics();
        let CoreNamespaces {
            main_kv,
            metadata_kv,
            schema_states_kv,
            schemas_kv,
            db_catalog_kv,
            field_keys_kv,
            public_keys_kv,
            idempotency_kv,
            superseded_by_kv,
            change_feed_kv,
            attribution_ledger_kv,
            keep_small_kv,
        } = CoreNamespaces::open(&store).await?;

        // Wrap KvStores in TypedKvStore adapters
        let metadata_typed = Arc::new(TypedKvStore::new(metadata_kv));
        let schema_states_store = Arc::new(TypedKvStore::new(schema_states_kv));
        let schemas_store = Arc::new(TypedKvStore::new(schemas_kv));
        let db_catalog_store = Arc::new(TypedKvStore::new(db_catalog_kv));
        let field_keys_store = Arc::new(TypedKvStore::new(field_keys_kv));
        let public_keys_typed = Arc::new(TypedKvStore::new(public_keys_kv));
        let idempotency_typed = Arc::new(TypedKvStore::new(idempotency_kv));
        let superseded_by_store = Arc::new(TypedKvStore::new(superseded_by_kv));

        // Domain stores
        let schema_store = SchemaStore::new(
            schemas_store,
            schema_states_store,
            superseded_by_store,
            store.disk_pack_runtime(),
        );
        let db_catalog = DbCatalogStore::new(db_catalog_store, store.disk_pack_runtime());
        let field_keys = FieldKeyStore::new(
            field_keys_store,
            field_key_wrap_key,
            store.disk_pack_runtime(),
        );
        let record_store = Self::open_record_store(
            &store,
            RecordNamespaces::new(main_kv, keep_small_kv),
            content_key,
            &field_keys,
            admit_meter_trust,
            content_key.or(field_key_wrap_key),
        )
        .await?;
        let public_key_store = PublicKeyStore::new(public_keys_typed);
        let metadata_store = MetadataStore::new(metadata_typed, idempotency_typed);
        let attribution_ledger = AttributionLedger::new(attribution_ledger_kv).await?;
        let change_feed = ChangeFeedStore::new(change_feed_kv, store.disk_pack_runtime()).await?;

        Ok(Self {
            store,
            schema_store,
            db_catalog,
            field_keys,
            record_store,
            public_key_store,
            metadata_store,
            attribution_ledger,
            change_feed,
            persist_metrics,
        })
    }

    async fn open_record_store(
        store: &Arc<dyn NamespacedStore>,
        namespaces: RecordNamespaces,
        content_key: Option<[u8; 32]>,
        field_keys: &FieldKeyStore,
        admit_meter_trust: bool,
        proof_key: Option<[u8; 32]>,
    ) -> Result<RecordStore, crate::storage::StorageError> {
        // Creation records the key layout. Every open requires that exact marker.
        let record_store = RecordStore::new_with_content_key(namespaces.main, content_key)
            .with_namespaced_store(Arc::clone(store))
            .with_field_keys(field_keys.clone())
            .with_keep_small_persist(namespaces.keep_small);
        Self::initialize_fresh_native_refs(store, &record_store, proof_key).await?;
        // Fail-closed meter TRUST, not fail-closed availability. #2186 refused
        // boot on any hydrate error; on 2026-09-25 that made every new build
        // refuse the primary home because its keep_small group (1.08 GB of
        // superseded snapshots) was over the cold-load cap. The meters are
        // gauge state, rebuildable from authoritative records: a failed
        // hydrate leaves every domain incomplete (reports stay incomplete,
        // meter-dependent deletion stays closed) and the node boots. Tom's
        // decision: decision-2026-09-25-lastdb-node-stays-upgradeable-soft-hard-cap.
        if admit_meter_trust {
            if let Err(e) = record_store.hydrate_keep_small().await {
                record_store
                    .note_keep_small_hydrate_failure(&e.to_string())
                    .await;
                tracing::warn!(
                    target: "fold_node::database",
                    error = %e,
                    "LASTDB_KEEP_SMALL_HYDRATE_FAILED meters marked incomplete; boot continues \
                     (reports stay incomplete and meter-dependent deletion stays closed until \
                     a new complete meter snapshot is available)"
                );
            }
        }
        Ok(record_store)
    }

    async fn initialize_fresh_native_refs(
        store: &Arc<dyn NamespacedStore>,
        records: &RecordStore,
        proof_key: Option<[u8; 32]>,
    ) -> Result<(), crate::storage::StorageError> {
        let Some(runtime) = store.disk_pack_runtime() else {
            return Ok(());
        };
        crate::storage::disk_pack::transaction::scope(Some(Arc::clone(&runtime)), async {
            records.require_current_key_format().await?;
            if let Some(key) = proof_key {
                crate::storage::content_key_proof::verify_stored(store.as_ref(), &key).await?;
            }
            Ok::<(), crate::storage::StorageError>(())
        })
        .await?;
        runtime.finish_fresh_initialization().await
    }
}
