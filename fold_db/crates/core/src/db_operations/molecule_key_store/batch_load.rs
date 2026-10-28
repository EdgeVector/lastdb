//! Two batch reads resolve all uncached molecule contracts for one schema.

use super::{
    open_wrap, validate_identity, wrap_key_for, BundleWrap, KeyAuthority, MoleculeKeyStore,
    StorageError, StorageResult, NODE_DOMAIN,
};

impl MoleculeKeyStore {
    pub(super) async fn load_existing(&self, molecules: &[String]) -> StorageResult<()> {
        let mut missing = Vec::new();
        for molecule in molecules {
            validate_identity(molecule, NODE_DOMAIN)?;
            if self.cached_contract(molecule)?.is_none() {
                missing.push(molecule.as_str());
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
        for (molecule, metadata) in missing.into_iter().zip(metadata) {
            let contract = metadata
                .map(|metadata| self.metadata_contract(molecule, &metadata))
                .transpose()?;
            present.push((molecule, contract));
        }
        if present.is_empty() {
            return Ok(());
        }
        let wrap_keys: Vec<_> = present
            .iter()
            .map(|(molecule, _)| wrap_key_for(molecule, NODE_DOMAIN))
            .collect();
        let wraps = self.entries.get_items::<BundleWrap>(&wrap_keys).await?;
        if wraps.len() != present.len() {
            return Err(incomplete());
        }
        // No cache entry publishes until the complete set validates.
        let contracts = present
            .into_iter()
            .zip(wraps)
            .map(|((molecule, contract), wrap)| {
                let Some((profile, authority, identity)) = contract else {
                    return if wrap.is_none() {
                        Ok(None)
                    } else {
                        Err(StorageError::InvalidOperation(
                            "a typed molecule wrap has no range contract".into(),
                        ))
                    };
                };
                let bundle = match (authority, wrap) {
                    (KeyAuthority::Bundle, Some(wrap)) => {
                        Some(open_wrap(&wrap, molecule, NODE_DOMAIN, wrap_key)?)
                    }
                    (KeyAuthority::Node, None) => None,
                    _ => {
                        return Err(StorageError::InvalidOperation(
                            "the molecule key authority differs from its stored node wrap".into(),
                        ))
                    }
                };
                Ok(Some((molecule, profile, identity, bundle)))
            })
            .collect::<StorageResult<Vec<_>>>()?;
        for (molecule, profile, identity, bundle) in contracts.into_iter().flatten() {
            self.remember_contract(molecule, profile, identity, bundle)?;
        }
        Ok(())
    }
}

fn incomplete() -> StorageError {
    StorageError::BackendError("molecule key batch read returned an incomplete result".into())
}
