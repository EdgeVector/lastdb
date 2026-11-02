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
        Self::from_namespaced_store_with_content_and_field_keys(store, None, None).await
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
        let runtime = store.disk_pack_runtime();
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
            Arc::clone(&runtime),
        );
        let db_catalog = DbCatalogStore::new(db_catalog_store, Arc::clone(&runtime));
        let field_keys =
            FieldKeyStore::new(field_keys_store, field_key_wrap_key, Arc::clone(&runtime));
        let record_store = Self::open_record_store(
            &store,
            Arc::new(TypedKvStore::new(main_kv)),
            content_key,
            &field_keys,
            content_key.or(field_key_wrap_key),
        )
        .await?;
        let public_key_store = PublicKeyStore::new(public_keys_typed);
        let metadata_store = MetadataStore::new(metadata_typed, idempotency_typed);
        let change_feed = ChangeFeedStore::new(change_feed_kv, Arc::clone(&runtime)).await?;

        Ok(Self {
            store,
            schema_store,
            db_catalog,
            field_keys,
            record_store,
            public_key_store,
            metadata_store,
            change_feed,
        })
    }

    async fn open_record_store(
        store: &Arc<dyn NamespacedStore>,
        main: Arc<TypedKvStore<dyn KvStore>>,
        content_key: Option<[u8; 32]>,
        field_keys: &FieldKeyStore,
        proof_key: Option<[u8; 32]>,
    ) -> Result<RecordStore, crate::storage::StorageError> {
        // Creation records the key layout. Every open requires that exact marker.
        let record_store = RecordStore::new_with_content_key(main, Arc::clone(store), content_key)
            .with_field_keys(field_keys.clone());
        Self::initialize_fresh_native_refs(store, &record_store, proof_key).await?;
        Ok(record_store)
    }

    async fn initialize_fresh_native_refs(
        store: &Arc<dyn NamespacedStore>,
        records: &RecordStore,
        proof_key: Option<[u8; 32]>,
    ) -> Result<(), crate::storage::StorageError> {
        let runtime = store.disk_pack_runtime();
        crate::storage::disk_pack::transaction::scope(Arc::clone(&runtime), async {
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
