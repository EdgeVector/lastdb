//! Prefix reads cover the current flat and compound codec layouts once.

use super::*;

impl LastStoreKvStore {
    pub(super) fn prefix_rows_sync(
        store: &LastStore,
        collection: &str,
        prefix: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut rows = Self::prefix_rows_exact_sync(store, collection, prefix, limit)?;
        let Some((requested, alias)) = prefix_alias(prefix) else {
            return Ok(rows);
        };
        rows.extend(Self::prefix_rows_exact_sync(
            store,
            collection,
            alias.as_bytes(),
            limit,
        )?);
        for (key, _) in &mut rows {
            if let Ok(text) = std::str::from_utf8(key) {
                *key = crate::kind_partition::rewrite_key_like(text, requested).into_bytes();
            }
        }
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        rows.dedup_by(|left, right| left.0 == right.0);
        rows.truncate(limit);
        Ok(rows)
    }

    pub(super) fn prefix_keys_sync(
        store: &LastStore,
        collection: &str,
        prefix: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<Vec<u8>>> {
        let mut keys = Self::prefix_keys_exact_sync(store, collection, prefix, limit)?;
        let Some((requested, alias)) = prefix_alias(prefix) else {
            return Ok(keys);
        };
        keys.extend(Self::prefix_keys_exact_sync(
            store,
            collection,
            alias.as_bytes(),
            limit,
        )?);
        for key in &mut keys {
            if let Ok(text) = std::str::from_utf8(key) {
                *key = crate::kind_partition::rewrite_key_like(text, requested).into_bytes();
            }
        }
        keys.sort();
        keys.dedup();
        keys.truncate(limit);
        Ok(keys)
    }
}

fn prefix_alias(prefix: &[u8]) -> Option<(&str, String)> {
    let requested = std::str::from_utf8(prefix).ok()?;
    crate::kind_partition::form_twin(requested).map(|alias| (requested, alias))
}
