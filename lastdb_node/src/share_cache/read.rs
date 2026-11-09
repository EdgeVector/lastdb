//! Ordinary database reads from one verified cache slot.

use super::ShareReader;
use fold_db::error::FoldDbError;
use fold_db::record_value::{FileMarker, StoredRecord};
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::{KeyValue, Schema};
use fold_db::storage::disk_pack::run_restore::{SnapshotRecordPage, SnapshotSchema};
use fold_db::storage::StorageError;

impl ShareReader {
    pub(crate) fn schemas(&self) -> Result<Vec<SnapshotSchema>, StorageError> {
        self.native.schemas()
    }

    pub(crate) fn exact_records(
        &self,
        schema: &Schema,
        keys: &[KeyValue],
    ) -> Result<Vec<StoredRecord>, StorageError> {
        self.native.exact_records(schema, keys, &self.content_key)
    }

    pub(crate) fn record_page(
        &self,
        schema: &Schema,
        filter: Option<&HashRangeFilter>,
        after: Option<&str>,
        limit: usize,
    ) -> Result<SnapshotRecordPage, StorageError> {
        self.native
            .record_page(schema, filter, after, limit, &self.content_key)
    }

    pub(crate) fn inline_file(
        &self,
        schema: &Schema,
        key: &KeyValue,
        expected_revision: Option<&str>,
    ) -> Result<Option<(FileMarker, Vec<u8>)>, FoldDbError> {
        self.native
            .inline_file(schema, key, expected_revision, &self.content_key)
    }
}
