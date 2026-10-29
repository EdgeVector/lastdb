//! Exact record readers borrow the same charged typed definition allocation.

use super::*;
use std::io::Write;

pub(crate) trait CoreRecordData: Send + Sync + 'static {
    /// None means this definition cannot answer that exact stored record.
    fn record_present(&self, collection: &str) -> Option<bool>;
    fn write_record(&self, collection: &str, out: &mut dyn Write) -> serde_json::Result<()>;
    fn clone_record(
        &self,
        collection: &str,
        target: std::any::TypeId,
    ) -> serde_json::Result<Option<TypedRecordValue>>;
    fn record_type(&self, collection: &str) -> std::any::TypeId;
    fn record_slot(&self, _: &str) -> usize {
        0
    }
}

pub(crate) type TypedRecordValue = Box<dyn Any + Send + Sync>;

pub(crate) fn clone_record_value<T: serde::Serialize + Clone + Send + Sync + 'static>(
    value: &T,
    target: std::any::TypeId,
) -> serde_json::Result<Option<TypedRecordValue>> {
    if target == std::any::TypeId::of::<T>() {
        Ok(Some(Box::new(value.clone())))
    } else if target == std::any::TypeId::of::<serde_json::Value>() {
        serde_json::to_value(value).map(|value| Some(Box::new(value) as TypedRecordValue))
    } else {
        Ok(None)
    }
}

impl<T: serde::Serialize + Clone + Send + Sync + 'static> CoreRecordData for Option<T> {
    fn record_type(&self, _: &str) -> std::any::TypeId {
        std::any::TypeId::of::<T>()
    }
    fn record_present(&self, _: &str) -> Option<bool> {
        Some(self.is_some())
    }

    fn write_record(&self, _: &str, out: &mut dyn Write) -> serde_json::Result<()> {
        serde_json::to_writer(out, self.as_ref().expect("present typed record"))
    }

    fn clone_record(
        &self,
        _: &str,
        target: std::any::TypeId,
    ) -> serde_json::Result<Option<TypedRecordValue>> {
        clone_record_value(self.as_ref().expect("present typed record"), target)
    }
}

pub(crate) trait CoreRecordReader: Send + Sync {
    fn record_present(&self, collection: &str) -> Option<bool>;
    fn write_record(&self, collection: &str, out: &mut dyn Write) -> serde_json::Result<()>;
    fn clone_record(
        &self,
        collection: &str,
        target: std::any::TypeId,
    ) -> serde_json::Result<Option<TypedRecordValue>>;
    fn wire_bytes(&self, collection: &str) -> serde_json::Result<usize>;
    fn record_type(&self, collection: &str) -> std::any::TypeId;
}

impl<T: CoreRecordData> CoreRecordReader for CoreMemoryValue<T> {
    fn record_type(&self, collection: &str) -> std::any::TypeId {
        self.value.record_type(collection)
    }
    fn record_present(&self, collection: &str) -> Option<bool> {
        self.value.record_present(collection)
    }

    fn write_record(&self, collection: &str, out: &mut dyn Write) -> serde_json::Result<()> {
        self.value.write_record(collection, out)
    }

    fn clone_record(
        &self,
        collection: &str,
        target: std::any::TypeId,
    ) -> serde_json::Result<Option<TypedRecordValue>> {
        self.value.clone_record(collection, target)
    }

    fn wire_bytes(&self, collection: &str) -> serde_json::Result<usize> {
        let cached = &self.wire_bytes[self.value.record_slot(collection)];
        if let Some(bytes) = cached.get() {
            return Ok(*bytes);
        }
        let mut length = RecordLength(0);
        self.value.write_record(collection, &mut length)?;
        let _ = cached.set(length.0);
        Ok(length.0)
    }
}

struct RecordLength(usize);
impl Write for RecordLength {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("typed record length overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
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
