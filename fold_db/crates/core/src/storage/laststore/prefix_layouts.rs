//! Prefix reads preserve exact stored keys.

use super::*;

impl LastStoreKvStore {
    pub(super) fn prefix_rows_sync(
        store: &LastStore,
        collection: &str,
        prefix: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        Self::prefix_rows_exact_sync(store, collection, prefix, limit)
    }

    pub(super) fn prefix_keys_sync(
        store: &LastStore,
        collection: &str,
        prefix: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<Vec<u8>>> {
        Self::prefix_keys_exact_sync(store, collection, prefix, limit)
    }
}
