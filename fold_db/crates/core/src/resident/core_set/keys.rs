//! Complete membership shares the actual molecule owner with sparse values.
use super::*;
use crate::resident::core_keys::CoreKeyDomain;

impl CoreWarmSet {
    pub(crate) fn attach_keys(&mut self, domain: Arc<CoreKeyDomain>) {
        let identity = domain.identity();
        let key = MoleculeKey::new(identity.scope.as_deref(), &identity.molecule);
        self.key_domains.insert(key, domain);
    }

    pub(crate) fn detach_keys(&mut self, scope: Option<&str>, molecule: &str) {
        self.key_domains.remove(&MoleculeKey::new(scope, molecule));
    }
}
