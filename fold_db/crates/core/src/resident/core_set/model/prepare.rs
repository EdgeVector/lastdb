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
}
