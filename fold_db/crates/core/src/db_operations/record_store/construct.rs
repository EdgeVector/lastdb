//! `RecordStore` constructors and builder hooks.

use super::*;

impl RecordStore {
    pub(crate) fn new(main_store: Arc<TypedKvStore<dyn KvStore>>) -> Self {
        Self::new_with_content_key(main_store, None)
    }

    pub(crate) fn new_with_content_key(
        main_store: Arc<TypedKvStore<dyn KvStore>>,
        content_key: Option<[u8; 32]>,
    ) -> Self {
        Self {
            main_store,
            serving_replay_cutoff: Arc::default(),
            namespaced_store: None,
            content_key,
            field_keys: None,
            field_key_commit_locks: Arc::default(),
            keep_small: Arc::new(KeepSmallMeters::default()),
            keep_small_persist: None,
            keep_small_last_persist: Arc::default(),
            keep_small_dirty: Arc::default(),
            keep_small_clean_stop_written: Arc::default(),
            keep_small_persist_lock: Arc::default(),
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

    /// Attach the owner namespace store.
    #[must_use]
    pub(crate) fn with_namespaced_store(mut self, store: Arc<dyn NamespacedStore>) -> Self {
        self.namespaced_store = Some(store);
        self
    }
}
