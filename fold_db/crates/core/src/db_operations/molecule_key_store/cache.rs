//! Key material stays private until its complete native write publishes.

use super::{
    CachedMoleculeContract, ConflictKeyIdentity, MoleculeKeyBundle, MoleculeKeyStore, StorageError,
    StorageResult,
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
            .and_then(|contract| contract.bundle))
    }

    pub(crate) fn codec_for_molecule(
        &self,
        molecule: &str,
        configured: &MoleculeKeyCodec,
    ) -> StorageResult<Option<MoleculeKeyCodec>> {
        let Some(contract) = self.cached_contract(molecule)? else {
            return Ok(None);
        };
        let codec = contract
            .bundle
            .as_ref()
            .map_or_else(|| configured.clone(), |bundle| bundle.key_codec(configured));
        match (contract.range_profile, contract.conflict_identity) {
            (None, None) => Ok(Some(codec)),
            (Some(profile), Some(identity)) => codec
                .for_typed_home()
                .with_range_profile(profile)
                .map(|codec| Some(codec.with_conflict_identity(identity)))
                .map_err(|_| {
                    StorageError::InvalidOperation("the molecule range profile is invalid".into())
                }),
            _ => Err(StorageError::InvalidOperation(
                "the molecule history identity is missing".into(),
            )),
        }
    }

    pub(super) fn remember_contract(
        &self,
        molecule: &str,
        profile: Option<MoleculeRangeProfile>,
        identity: Option<ConflictKeyIdentity>,
        bundle: Option<MoleculeKeyBundle>,
    ) -> StorageResult<()> {
        if self.is_typed() != profile.is_some()
            || profile.is_some() != identity.is_some()
            || (profile.is_none() && bundle.is_none())
        {
            return Err(StorageError::InvalidOperation(
                "invalid molecule memory contract".into(),
            ));
        }
        if let Some(profile) = profile {
            profile.validate().map_err(StorageError::InvalidOperation)?;
        }
        let contract = CachedMoleculeContract {
            range_profile: profile,
            bundle,
            conflict_identity: identity,
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
