//! `RecordStore` constructors and builder hooks.

use super::*;

impl RecordStore {
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
            queue_clock: Arc::default(),
            history_activation: Arc::default(),
            range_walks: Arc::default(),
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
