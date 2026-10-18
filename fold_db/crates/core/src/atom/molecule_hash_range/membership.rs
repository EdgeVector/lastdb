//! Attach the complete key lifetime to the existing logical molecule.

use super::MoleculeHashRange;
use crate::resident::core_keys::CoreKeyDomain;
use std::sync::Arc;

impl MoleculeHashRange {
    pub(crate) fn attach_core_keys(&mut self, domain: Arc<CoreKeyDomain>) -> bool {
        if domain.identity().molecule != self.uuid {
            return false;
        }
        // Complete membership does not make sparse full-tip state complete.
        self.loaded_is_partial = true;
        self.complete_keys = Some(domain);
        true
    }

    pub(crate) fn core_keys(&self) -> Option<&Arc<CoreKeyDomain>> {
        self.complete_keys.as_ref()
    }

    pub(crate) fn detach_core_keys(&mut self) -> Option<Arc<CoreKeyDomain>> {
        self.complete_keys.take()
    }
}
