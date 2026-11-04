//! `RecordStore` constructors and builder hooks.

use super::*;

impl RecordStore {
    pub(crate) fn new(format: RecordFormat) -> Self {
        Self {
            main_store: format.reader(),
            runtime: format.runtime(),
            format: Arc::new(format),
            serving_replay_cutoff: Arc::default(),
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
