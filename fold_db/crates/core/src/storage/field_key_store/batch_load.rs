//! Two batch reads resolve all uncached field key contracts for one schema.

use super::{
    open_wrap, validate_identity, wrap_key_for, BundleWrap, FieldKeyStore, StorageError,
    StorageResult, NODE_DOMAIN,
};

impl FieldKeyStore {
    pub(crate) async fn load_existing(&self, field_keys: &[String]) -> StorageResult<()> {
        let mut missing = Vec::new();
        for field_key in field_keys {
            validate_identity(field_key, NODE_DOMAIN)?;
            if self.cached_contract(field_key)?.is_none() {
                missing.push(field_key.as_str());
            }
        }
        let Some(wrap_key) = self.node_wrap_key.as_ref() else {
            return Ok(());
        };
        if missing.is_empty() {
            return Ok(());
        }
        let metadata = self.metadata_batch_from_memory(&missing).await?;
        let mut present = Vec::new();
        for (field_key, metadata) in missing.into_iter().zip(metadata) {
            let contract = metadata
                .map(|metadata| self.metadata_contract(field_key, &metadata))
                .transpose()?;
            present.push((field_key, contract));
        }
        if present.is_empty() {
            return Ok(());
        }
        let wrap_keys: Vec<_> = present
            .iter()
            .map(|(field_key, _)| wrap_key_for(field_key, NODE_DOMAIN))
            .collect();
        let wraps = self.entries.get_items::<BundleWrap>(&wrap_keys).await?;
        if wraps.len() != present.len() {
            return Err(incomplete());
        }
        // No cache entry publishes until the complete set validates.
        let contracts = present
            .into_iter()
            .zip(wraps)
            .map(|((field_key, profile), wrap)| match (profile, wrap) {
                (Some(profile), Some(wrap)) => Ok(Some((
                    field_key,
                    profile,
                    open_wrap(&wrap, field_key, NODE_DOMAIN, wrap_key)?,
                ))),
                (None, None) => Ok(None),
                (Some(_), None) => Err(StorageError::InvalidOperation(
                    "a typed field key contract has no node wrap".into(),
                )),
                (None, Some(_)) => Err(StorageError::InvalidOperation(
                    "a typed field key wrap has no range contract".into(),
                )),
            })
            .collect::<StorageResult<Vec<_>>>()?;
        for (field_key, profile, bundle) in contracts.into_iter().flatten() {
            self.remember_contract(field_key, profile, bundle)?;
        }
        Ok(())
    }
}

fn incomplete() -> StorageError {
    StorageError::BackendError("field key batch read returned an incomplete result".into())
}
