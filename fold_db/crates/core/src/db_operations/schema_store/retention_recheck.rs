//! Age re-check for Hash and Single keys that a retention sweep listed earlier.

use super::catalog_keys::*;
use super::SchemaStore;
use crate::schema::types::KeyValue;
use crate::schema::SchemaError;

impl SchemaStore {
    /// The keys of `keys` whose latest write is still older than `cutoff`, in
    /// the order given, from one batched read of the reverse age rows.
    ///
    /// A sweep lists expired keys once and erases them batch by batch, with a
    /// rest between batches. A client can rewrite a listed key in that time.
    /// A rewrite moves the reverse row to the new write time, so the key is no
    /// longer older than `cutoff` and this drops it. A key with no reverse row
    /// was erased by someone else, so it is dropped too.
    pub async fn retention_keys_still_expired(
        &self,
        schema_name: &str,
        keys: &[KeyValue],
        cutoff: u64,
    ) -> Result<Vec<KeyValue>, SchemaError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let latest_keys: Vec<_> = keys
            .iter()
            .map(|key| retention_age_latest_key(schema_name, key))
            .collect();
        let latest = self
            .schema_states_store
            .get_items::<SchemaRetentionAgeEntry>(&latest_keys)
            .await?;
        Ok(keys
            .iter()
            .zip(latest)
            .filter(|(_, entry)| {
                entry
                    .as_ref()
                    .is_some_and(|entry| entry.written_at < cutoff)
            })
            .map(|(key, _)| key.clone())
            .collect())
    }
}
