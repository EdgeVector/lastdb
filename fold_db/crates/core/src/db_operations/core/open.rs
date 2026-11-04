use super::super::change_feed::ChangeFeedStore;
use super::super::metadata_store::MetadataStore;
use super::super::record_store::RecordStore;
use super::super::schema_store::SchemaStore;
use super::DbOperations;
use crate::storage::field_key_store::FieldKeyStore;
use crate::storage::format;
use crate::storage::traits::*;
use std::sync::Arc;

/// The namespace handles `DbOperationsCore` opens at construction.
struct CoreNamespaces {
    main_kv: Arc<dyn KvStore>,
    metadata_kv: Arc<dyn KvStore>,
    schema_states_kv: Arc<dyn KvStore>,
    schemas_kv: Arc<dyn KvStore>,
    field_keys_kv: Arc<dyn KvStore>,
    change_feed_kv: Arc<dyn KvStore>,
}

impl CoreNamespaces {
    async fn open(store: &Arc<dyn NamespacedStore>) -> Result<Self, crate::storage::StorageError> {
        let main_kv = store.open_namespace(format::MAIN_NAMESPACE).await?;
        let metadata_kv = store.open_namespace(format::METADATA_NAMESPACE).await?;
        let schema_states_kv = store
            .open_namespace(format::SCHEMA_STATES_NAMESPACE)
            .await?;
        let schemas_kv = store.open_namespace(format::SCHEMAS_NAMESPACE).await?;
        let field_keys_kv = store.open_namespace(format::FIELD_KEYS_NAMESPACE).await?;
        let change_feed_kv = store.open_namespace(format::CHANGE_FEED_NAMESPACE).await?;
        Ok(Self {
            main_kv,
            metadata_kv,
            schema_states_kv,
            schemas_kv,
            field_keys_kv,
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
            field_keys_kv,
            change_feed_kv,
        } = CoreNamespaces::open(&store).await?;

        // Domain stores
        let schema_store = SchemaStore::new(format::schemas::SchemaFormat::new(
            schemas_kv,
            schema_states_kv,
            Arc::clone(&runtime),
        ));
        let field_keys = FieldKeyStore::new(
            format::field_keys::FieldKeyFormat::new(field_keys_kv, Arc::clone(&runtime)),
            field_key_wrap_key,
        );
        let record_store = Self::open_record_store(
            &store,
            format::records::RecordFormat::new(main_kv, Arc::clone(&store), content_key),
            &field_keys,
            content_key.or(field_key_wrap_key),
        )
        .await?;
        let metadata_store = MetadataStore::new(format::metadata::Metadata::new(
            metadata_kv,
            Arc::clone(&runtime),
        ));
        let change_feed = ChangeFeedStore::new(format::change_feed::ChangeFeed::new(
            change_feed_kv,
            Arc::clone(&runtime),
        ))
        .await?;

        Ok(Self {
            store,
            schema_store,
            field_keys,
            record_store,
            metadata_store,
            change_feed,
        })
    }

    async fn open_record_store(
        store: &Arc<dyn NamespacedStore>,
        format: format::records::RecordFormat,
        field_keys: &FieldKeyStore,
        proof_key: Option<[u8; 32]>,
    ) -> Result<RecordStore, crate::storage::StorageError> {
        // Creation records the key layout. Every open requires that exact marker.
        let record_store = RecordStore::new(format).with_field_keys(field_keys.clone());
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
