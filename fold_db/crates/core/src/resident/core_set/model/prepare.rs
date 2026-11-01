//! Preflight all immutable metadata before the common publication switch.

use super::*;

impl CoreWarmSet {
    /// A committed root publishes only its prepared memory definitions.
    pub(crate) fn prepare_clean(
        &mut self,
        definitions: Vec<PreparedCoreMemory>,
        owner: Option<&Arc<DeferReservation>>,
    ) -> Result<CoreModelUpdate, CoreCapacity> {
        self.observe_model_owner(owner);
        self.memory.reserve_install(definitions.len())?;
        Ok(CoreModelUpdate { definitions })
    }

    pub(in crate::resident::core_set) fn share_model_atom(
        &self,
        key: &MoleculeKey,
        value: &mut CoreWarmSlot,
    ) {
        let atom = AtomKey::new(key.scope.clone(), value.atom.uuid());
        if let Some(existing) = self
            .atoms
            .get(&atom)
            .into_iter()
            .flatten()
            .filter_map(Weak::upgrade)
            .find(|existing| existing.value.as_ref() == value.atom.as_ref())
        {
            value.atom = Arc::clone(&existing.value);
        }
    }
}
