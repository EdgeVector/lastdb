//! Batched read of existing node wraps; never mint or repair key state.

use super::*;

impl MoleculeKeyStore {
    pub(crate) async fn load_existing_many(&self, names: &[String]) -> StorageResult<()> {
        let mut missing = Vec::new();
        for name in names {
            validate_identity(name, NODE_DOMAIN)?;
            if self.cached(name).is_none() {
                missing.push(name);
            }
        }
        if missing.is_empty() {
            return Ok(());
        }
        let wrapping_key = self.node_wrap_key.as_ref().ok_or_else(|| {
            StorageError::InvalidOperation("offline molecule wraps require a node key".into())
        })?;
        let keys = missing
            .iter()
            .flat_map(|name| {
                [
                    metadata_key(name).into_bytes(),
                    wrap_key_for(name, NODE_DOMAIN).into_bytes(),
                ]
            })
            .collect::<Vec<_>>();
        let values = self.entries.inner().get_many(keys).await?;
        if values.len() != missing.len() * 2 {
            return Err(StorageError::InvalidOperation(
                "offline molecule key batch count differs".into(),
            ));
        }
        let mut resolved = Vec::with_capacity(missing.len());
        for (name, pair) in missing.into_iter().zip(values.chunks_exact(2)) {
            let metadata: BundleMetadata =
                serde_json::from_slice(pair[0].as_deref().ok_or_else(|| {
                    StorageError::InvalidOperation(
                        "offline atom names missing molecule metadata".into(),
                    )
                })?)
                .map_err(|error| StorageError::SerializationError(error.to_string()))?;
            if metadata.version != BUNDLE_VERSION || metadata.molecule_uuid != *name {
                return Err(StorageError::InvalidOperation(
                    "offline molecule key metadata identity differs".into(),
                ));
            }
            let wrap: BundleWrap = serde_json::from_slice(pair[1].as_deref().ok_or_else(|| {
                StorageError::InvalidOperation("offline molecule metadata has no node wrap".into())
            })?)
            .map_err(|error| StorageError::SerializationError(error.to_string()))?;
            let bundle = open_wrap(&wrap, name, NODE_DOMAIN, wrapping_key)?;
            resolved.push((name.clone(), bundle));
        }
        for (name, bundle) in resolved {
            self.remember(&name, bundle);
        }
        Ok(())
    }
}
