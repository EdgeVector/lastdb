//! Complete membership shares the actual molecule owner with sparse values.
use super::*;
use crate::resident::core_keys::{CoreKeyDomain, CoreKeyLease};

impl CoreWarmSet {
    pub(crate) fn attach_keys(&mut self, domain: Arc<CoreKeyDomain>) {
        let identity = domain.identity();
        let key = MoleculeKey {
            scope: identity.scope.clone(),
            molecule: identity.molecule.clone(),
        };
        let model = self
            .molecules
            .entry(key)
            .or_insert_with(|| MoleculeHashRange::with_partial_uuid(identity.molecule.clone()));
        model.attach_core_keys(domain);
    }

    pub(crate) fn key_lease(&self, scope: Option<&str>, molecule: &str) -> Option<CoreKeyLease> {
        self.molecules
            .get(&MoleculeKey {
                scope: scope.map(str::to_owned),
                molecule: molecule.to_owned(),
            })?
            .core_keys()?
            .lease()
            .ok()
            .flatten()
    }

    pub(crate) fn detach_keys(&mut self, scope: Option<&str>, molecule: &str) {
        if let Some(model) = self.molecules.get_mut(&MoleculeKey {
            scope: scope.map(str::to_owned),
            molecule: molecule.to_owned(),
        }) {
            model.detach_core_keys();
        }
    }
}
