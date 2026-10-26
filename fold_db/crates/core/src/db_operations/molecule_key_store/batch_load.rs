//! Two batch reads resolve all uncached molecule contracts for one schema.

use super::{
    metadata_key, open_wrap, validate_identity, wrap_key_for, BundleMetadata, BundleWrap,
    MoleculeKeyStore, StorageError, StorageResult, NODE_DOMAIN,
};

impl MoleculeKeyStore {
    pub(super) async fn load_existing(&self, molecules: &[String]) -> StorageResult<()> {
        let mut missing = Vec::new();
        for molecule in molecules {
            validate_identity(molecule, NODE_DOMAIN)?;
            if self.cached_bundle(molecule)?.is_none() {
                missing.push(molecule);
            }
        }
        let Some(wrap_key) = self.node_wrap_key.as_ref() else {
            return Ok(());
        };
        if missing.is_empty() {
            return Ok(());
        }
        let metadata_keys: Vec<_> = missing
            .iter()
            .map(|molecule| metadata_key(molecule))
            .collect();
        let metadata = self
            .entries
            .get_items::<BundleMetadata>(&metadata_keys)
            .await?;
        if metadata.len() != missing.len() {
            return Err(incomplete());
        }
        let mut present = Vec::new();
        for (molecule, metadata) in missing.into_iter().zip(metadata) {
            if let Some(metadata) = metadata {
                if !metadata.is_legacy_for(molecule) {
                    return Err(StorageError::InvalidOperation(
                        "molecule key metadata does not match its identity or format".into(),
                    ));
                }
                present.push(molecule);
            }
        }
        if present.is_empty() {
            return Ok(());
        }
        let wrap_keys: Vec<_> = present
            .iter()
            .map(|molecule| wrap_key_for(molecule, NODE_DOMAIN))
            .collect();
        let wraps = self.entries.get_items::<BundleWrap>(&wrap_keys).await?;
        if wraps.len() != present.len() {
            return Err(incomplete());
        }
        // No cache entry publishes until the complete set validates.
        let bundles = present
            .into_iter()
            .zip(wraps)
            .map(|(molecule, wrap)| {
                let wrap = wrap.ok_or_else(|| {
                    StorageError::InvalidOperation("molecule key metadata has no node wrap".into())
                })?;
                let bundle = open_wrap(&wrap, molecule, NODE_DOMAIN, wrap_key)?;
                Ok((molecule, bundle))
            })
            .collect::<StorageResult<Vec<_>>>()?;
        for (molecule, bundle) in bundles {
            self.remember(molecule, bundle)?;
        }
        Ok(())
    }
}

fn incomplete() -> StorageError {
    StorageError::BackendError("molecule key batch read returned an incomplete result".into())
}
