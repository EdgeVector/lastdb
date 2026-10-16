//! Share the existing logical-main routing with the native disk backend.
use super::*;

pub(in crate::storage) fn read_candidates(key: &[u8]) -> Vec<&'static str> {
    main_collections_for_key(key)
}

pub(in crate::storage) fn write_collection(key: &[u8]) -> &'static str {
    main_collection_for_key(key)
}

pub(in crate::storage) fn prefix_collections(prefix: &[u8]) -> Vec<&'static str> {
    main_collections_for_prefix(prefix)
}

pub(in crate::storage) fn range_collections(start: &[u8], end: &[u8]) -> Vec<&'static str> {
    main_collections_for_range(start, end)
}

pub(in crate::storage) fn encode(key: &[u8]) -> String {
    LastStoreKvStore::encode_key(key)
}

pub(in crate::storage) fn decode(key: &str) -> StorageResult<Vec<u8>> {
    LastStoreKvStore::decode_key(key)
}
