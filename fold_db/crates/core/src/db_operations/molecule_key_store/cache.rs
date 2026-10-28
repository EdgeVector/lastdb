//! Key material stays private until its complete native write publishes.

use super::{
    CachedMoleculeContract, MoleculeKeyBundle, MoleculeKeyStore, StorageError, StorageResult,
};
use crate::atom::MoleculeKeyCodec;
use crate::storage::disk_pack::transaction::publication;
use schema_types::molecule_range_profile::MoleculeRangeProfile;
use std::sync::Arc;

impl MoleculeKeyStore {
    pub(super) fn cached_contract(
        &self,
        molecule: &str,
    ) -> StorageResult<Option<CachedMoleculeContract>> {
        if let Some(contract) = publication::get::<CachedMoleculeContract>(
            self.runtime.clone(),
            Arc::as_ptr(&self.cache) as usize,
            molecule,
        )? {
            return Ok(Some(contract.as_ref().clone()));
        }
        Ok(self
            .cache
            .read()
            .map_err(|_| cache_error())?
            .get(molecule)
            .cloned())
    }

    pub(crate) fn cached_bundle(&self, molecule: &str) -> StorageResult<Option<MoleculeKeyBundle>> {
        Ok(self
            .cached_contract(molecule)?
            .map(|contract| contract.bundle))
    }

    pub(crate) fn codec_for_molecule(
        &self,
        molecule: &str,
    ) -> StorageResult<Option<MoleculeKeyCodec>> {
        let Some(contract) = self.cached_contract(molecule)? else {
            return Ok(None);
        };
        Ok(Some(contract.bundle.key_codec(contract.range_profile)))
    }

    pub(super) fn remember_contract(
        &self,
        molecule: &str,
        profile: MoleculeRangeProfile,
        bundle: MoleculeKeyBundle,
    ) -> StorageResult<()> {
        let contract = CachedMoleculeContract {
            range_profile: profile,
            bundle,
        };
        let cache = Arc::clone(&self.cache);
        let molecule = molecule.to_owned();
        publication::publish(
            self.runtime.clone(),
            Arc::as_ptr(&cache) as usize,
            molecule.clone(),
            contract.clone(),
            move || {
                cache
                    .write()
                    .map_err(|_| cache_error())?
                    .insert(molecule, contract);
                Ok(())
            },
        )
    }
}

fn cache_error() -> StorageError {
    StorageError::BackendError("molecule key cache lock is poisoned".into())
}
