//! `AtomStore` constructors and builder hooks.

use super::*;

impl AtomStore {
    pub(crate) fn new(main_store: Arc<TypedKvStore<dyn KvStore>>) -> Self {
        Self::new_with_content_key(main_store, None)
    }

    pub(crate) fn new_with_content_key(
        main_store: Arc<TypedKvStore<dyn KvStore>>,
        content_key: Option<[u8; 32]>,
    ) -> Self {
        Self {
            main_store,
            app_schema_conversion_active: Arc::default(),
            serving_replay_cutoff: Arc::default(),
            namespaced_store: None,
            content_key,
            molecule_keys: None,
            molecule_commit_locks: Arc::default(),
            generation_cleanup_locks: Arc::default(),
            tip_commit_locks: Arc::default(),
            tip_publication_locks: Arc::default(),
            pending_delete_barriers: Arc::default(),
            keep_small: Arc::new(KeepSmallMeters::default()),
            keep_small_persist: None,
            keep_small_last_persist: Arc::default(),
            keep_small_dirty: Arc::default(),
            keep_small_clean_stop_written: Arc::default(),
            keep_small_persist_lock: Arc::default(),
            counter_events: Arc::default(),
            blob_events: Arc::default(),
            retention: Arc::default(),
            history_activation: Arc::default(),
            counter_verify: Arc::default(),
            molecule_drain: Arc::default(),
            range_walks: Arc::default(),
            local_origin_device: Arc::default(),
            conflict_record_epoch: Arc::default(),
            blob_liveness_locks: Arc::new(
                (0..256)
                    .map(|_| Arc::new(tokio::sync::Mutex::new(())))
                    .collect(),
            ),
            store_id: crate::atom::history_event_memo::StoreId::next(),
        }
    }

    #[must_use]
    pub(crate) fn with_molecule_keys(
        mut self,
        molecule_keys: crate::db_operations::MoleculeKeyStore,
    ) -> Self {
        self.molecule_keys = Some(molecule_keys);
        self
    }

    /// Attach the owner namespace store.
    #[must_use]
    pub(crate) fn with_namespaced_store(mut self, store: Arc<dyn NamespacedStore>) -> Self {
        self.namespaced_store = Some(store);
        self
    }
}
