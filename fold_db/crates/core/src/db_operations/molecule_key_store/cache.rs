//! Key material stays private until its complete native write publishes.

use super::{MoleculeKeyBundle, MoleculeKeyStore, StorageError, StorageResult};
use crate::storage::disk_pack::transaction::publication;
use std::sync::Arc;

impl MoleculeKeyStore {
    pub(crate) fn cached_bundle(&self, molecule: &str) -> StorageResult<Option<MoleculeKeyBundle>> {
        if let Some(bundle) = publication::get::<MoleculeKeyBundle>(
            self.runtime.clone(),
            Arc::as_ptr(&self.cache) as usize,
            molecule,
        )? {
            return Ok(Some(bundle.as_ref().clone()));
        }
        Ok(self
            .cache
            .read()
            .map_err(|_| cache_error())?
            .get(molecule)
            .cloned())
    }

    pub(super) fn remember(&self, molecule: &str, bundle: MoleculeKeyBundle) -> StorageResult<()> {
        let cache = Arc::clone(&self.cache);
        let molecule = molecule.to_owned();
        publication::publish(
            self.runtime.clone(),
            Arc::as_ptr(&cache) as usize,
            molecule.clone(),
            bundle.clone(),
            move || {
                cache
                    .write()
                    .map_err(|_| cache_error())?
                    .insert(molecule, bundle);
                Ok(())
            },
        )
    }
}

fn cache_error() -> StorageError {
    StorageError::BackendError("molecule key cache lock is poisoned".into())
}
