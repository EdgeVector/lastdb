//! Spike: exact counter results share the core's charged metadata owner.
//!
//! Callers check private and accepted records first. These entries describe
//! only the captured disk revision. Exact-source invalidation removes them
//! before another committed revision becomes visible. No atom body is saved.

use super::*;
use crate::resident::{CoreMemorySource, CoreMemoryValue};

type Coordinate = (String, String);
type CounterValue = Option<Vec<u8>>;
static COUNTERS: u8 = 0;
const MAX_COUNTER_BYTES: usize = 16 * 1024;

impl PackRuntime {
    pub(super) fn counter_memory_value(
        &self,
        context: &ReadContext,
        coordinate: &Coordinate,
    ) -> StorageResult<Option<Arc<CoreMemoryValue<CounterValue>>>> {
        if context.write_owner.is_none() || !is_counter(coordinate) {
            return Ok(None);
        }
        let view = self.view.read().map_err(|_| unavailable())?;
        if view.revision != context.revision {
            return Ok(None);
        }
        let value = self
            .core
            .lock()
            .map_err(|_| unavailable())?
            .memory_get(counter_owner(), &counter_name(coordinate));
        drop(view);
        Ok(value)
    }

    pub(super) fn reuse_counter_value(
        &self,
        context: &ReadContext,
        call: &mut PhysicalRead,
        coordinate: &Coordinate,
    ) -> StorageResult<Option<CounterValue>> {
        let Some(value) = self.counter_memory_value(context, coordinate)? else {
            return Ok(None);
        };
        let bytes = (**value).as_ref().map_or(0, Vec::len);
        context.account_memory_record(call, coordinate.0.len() + coordinate.1.len(), bytes)?;
        context.note_counter_memory(true)?;
        Ok(Some((**value).clone()))
    }

    /// True selects core ownership, even when its finite allowance refuses it.
    pub(super) fn remember_counter_value(
        &self,
        context: &ReadContext,
        coordinate: &Coordinate,
        value: Option<&[u8]>,
    ) -> StorageResult<bool> {
        if context.write_owner.is_none() || !is_counter(coordinate) {
            return Ok(false);
        }
        let bytes = value.map_or(0, <[u8]>::len);
        if bytes > MAX_COUNTER_BYTES {
            return Ok(true);
        }
        let view = self.view.read().map_err(|_| unavailable())?;
        if view.revision != context.revision {
            return Ok(true);
        }
        let mut core = self.core.lock().map_err(|_| unavailable())?;
        let name = counter_name(coordinate);
        if core
            .memory_get::<CounterValue>(counter_owner(), &name)
            .is_none()
        {
            core.memory_insert(
                counter_owner(),
                name,
                value.map(<[u8]>::to_vec),
                vec![CoreMemorySource {
                    collection: coordinate.0.clone(),
                    key: Some(coordinate.1.clone()),
                }],
                bytes + std::mem::size_of::<CounterValue>(),
            );
            context.note_counter_memory(false)?;
        }
        Ok(true)
    }
}

fn is_counter(coordinate: &Coordinate) -> bool {
    let Ok(raw) = super::super::native_routing::decode(&coordinate.1) else {
        return false;
    };
    let Ok(key) = std::str::from_utf8(&raw) else {
        return false;
    };
    crate::atom::atom_key_codec::is_counter_key(key)
        && super::super::native_routing::collection_for_key(&raw) == coordinate.0
}

fn counter_owner() -> usize {
    std::ptr::from_ref(&COUNTERS) as usize
}

fn counter_name(coordinate: &Coordinate) -> String {
    serde_json::to_string(coordinate).expect("two counter coordinate strings")
}

fn unavailable() -> StorageError {
    StorageError::BackendError("core counter memory lock is unavailable".into())
}
