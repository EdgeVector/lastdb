//! Keyed membership used by account source preparation.

use super::*;

impl DbCatalogStore {
    pub async fn get_many(
        &self,
        identities: &[(String, String)],
    ) -> StorageResult<Vec<Option<DbCatalogEntry>>> {
        let keys = identities
            .iter()
            .map(|(locator, name)| entry_key(locator, name).into_bytes())
            .collect();
        self.entries
            .inner()
            .get_many(keys)
            .await?
            .into_iter()
            .map(|value| {
                value
                    .map(|bytes| {
                        serde_json::from_slice(&bytes).map_err(|_| {
                            StorageError::SerializationError("invalid catalog membership".into())
                        })
                    })
                    .transpose()
            })
            .collect()
    }

    pub async fn put_membership_batch(&self, entries: &[DbCatalogEntry]) -> StorageResult<()> {
        let mut puts = Vec::with_capacity(entries.len() * 2);
        for entry in entries {
            validate_entry(entry)?;
            let bytes = serde_json::to_vec(entry).map_err(|_| {
                StorageError::SerializationError("serialize catalog membership".into())
            })?;
            puts.push((
                entry_key(&entry.db_locator, &entry.schema_name).into_bytes(),
                bytes.clone(),
            ));
            let locator_hash = Sha256::digest(entry.db_locator.as_bytes());
            puts.push((
                format!("membership:{locator_hash:x}\0{}", entry.schema_name).into_bytes(),
                bytes,
            ));
        }
        self.entries.inner().batch_put(puts).await
    }
}
