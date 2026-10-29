//! Typed core definition ownership and byte reservations.

use super::*;

impl CoreWarmSet {
    pub(crate) fn prepare_memory<T: Any + Send + Sync>(
        &mut self,
        owner: usize,
        name: String,
        value: T,
        sources: Vec<CoreMemorySource>,
        bytes: usize,
    ) -> Result<PreparedCoreMemory, CoreCapacity> {
        self.memory.prepare((owner, name), value, sources, bytes)
    }

    pub(crate) fn memory_get<T: Any + Send + Sync>(
        &mut self,
        owner: usize,
        name: &str,
    ) -> Option<Arc<CoreMemoryValue<T>>> {
        self.memory.get(owner, name)
    }

    pub(crate) fn memory_insert<T: Any + Send + Sync>(
        &mut self,
        owner: usize,
        name: String,
        value: T,
        sources: Vec<CoreMemorySource>,
        bytes: usize,
    ) -> Arc<CoreMemoryValue<T>> {
        self.memory.insert((owner, name), value, sources, bytes)
    }

    pub(crate) fn memory_uncached<T>(value: T) -> Arc<CoreMemoryValue<T>> {
        Arc::new(CoreMemoryValue {
            value,
            _charge: None,
            wire_bytes: Default::default(),
        })
    }

    pub(crate) fn memory_stats(&self) -> CoreMemoryStats {
        self.memory.stats()
    }
}
