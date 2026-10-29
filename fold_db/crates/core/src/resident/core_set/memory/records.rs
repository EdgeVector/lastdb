//! Exact record readers borrow the same charged typed definition allocation.

use super::*;
use std::io::Write;

pub(crate) trait CoreRecordData: Send + Sync + 'static {
    /// None means this definition cannot answer that exact stored record.
    fn record_present(&self, collection: &str) -> Option<bool>;
    fn write_record(&self, collection: &str, out: &mut dyn Write) -> serde_json::Result<()>;
}

impl<T: serde::Serialize + Send + Sync + 'static> CoreRecordData for Option<T> {
    fn record_present(&self, _: &str) -> Option<bool> {
        Some(self.is_some())
    }

    fn write_record(&self, _: &str, out: &mut dyn Write) -> serde_json::Result<()> {
        serde_json::to_writer(out, self.as_ref().expect("present typed record"))
    }
}

pub(crate) trait CoreRecordReader: Send + Sync {
    fn record_present(&self, collection: &str) -> Option<bool>;
    fn write_record(&self, collection: &str, out: &mut dyn Write) -> serde_json::Result<()>;
}

impl<T: CoreRecordData> CoreRecordReader for CoreMemoryValue<T> {
    fn record_present(&self, collection: &str) -> Option<bool> {
        self.value.record_present(collection)
    }

    fn write_record(&self, collection: &str, out: &mut dyn Write) -> serde_json::Result<()> {
        self.value.write_record(collection, out)
    }
}

impl PreparedCoreMemory {
    pub(crate) fn expose_record<T: CoreRecordData>(&mut self) {
        self.reader = self
            .typed::<T>()
            .map(|value| value as Arc<dyn CoreRecordReader>);
    }
}

impl CoreWarmSet {
    pub(crate) fn expose_memory_record<T: CoreRecordData>(
        &mut self,
        owner: usize,
        name: &str,
        value: &Arc<CoreMemoryValue<T>>,
    ) {
        let Some(entry) = self.memory.entries.get_mut(&(owner, name.to_owned())) else {
            return;
        };
        let retained: Arc<dyn Any + Send + Sync> = value.clone();
        if Arc::ptr_eq(&entry.value, &retained) {
            entry.reader = Some(value.clone());
        }
    }

    /// Only exact source edges qualify. A collection dependency is not a value.
    pub(crate) fn memory_record(
        &mut self,
        collection: &str,
        key: &str,
    ) -> Option<Arc<dyn CoreRecordReader>> {
        let source = CoreMemorySource {
            collection: collection.to_owned(),
            key: Some(key.to_owned()),
        };
        let (identity, reader) = self
            .memory
            .sources
            .get(&source)?
            .iter()
            .find_map(|identity| {
                let reader = self.memory.entries.get(identity)?.reader.as_ref()?;
                reader.record_present(collection)?;
                Some((identity.clone(), Arc::clone(reader)))
            })?;
        let entry = self.memory.entries.get_mut(&identity)?;
        self.memory.order.remove(&entry.tick);
        self.memory.clock = self.memory.clock.wrapping_add(1);
        entry.tick = self.memory.clock;
        self.memory.order.insert(entry.tick, identity);
        Some(reader)
    }
}
