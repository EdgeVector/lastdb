//! `RecordStore` constructors and builder hooks.

use super::*;

impl RecordStore {
    pub(crate) fn new(
        main_store: Arc<TypedKvStore<dyn KvStore>>,
        namespaced_store: Arc<dyn NamespacedStore>,
    ) -> Self {
        Self::new_with_content_key(main_store, namespaced_store, None)
    }

    pub(crate) fn new_with_content_key(
        main_store: Arc<TypedKvStore<dyn KvStore>>,
        namespaced_store: Arc<dyn NamespacedStore>,
        content_key: Option<[u8; 32]>,
    ) -> Self {
        Self {
            main_store,
            serving_replay_cutoff: Arc::default(),
            namespaced_store,
            content_key,
            field_keys: None,
            field_key_commit_locks: Arc::default(),
            blob_events: Arc::default(),
            retention: Arc::default(),
            history_activation: Arc::default(),
            counter_verify: Arc::default(),
            range_walks: Arc::default(),
            local_origin_device: Arc::default(),
            blob_liveness_locks: Arc::new(
                (0..256)
                    .map(|_| Arc::new(tokio::sync::Mutex::new(())))
                    .collect(),
            ),
        }
    }

    #[must_use]
    pub(crate) fn with_field_keys(
        mut self,
        field_keys: crate::db_operations::FieldKeyStore,
    ) -> Self {
        self.field_keys = Some(field_keys);
        self
    }
}
