//! The floors of the TTL sweep, in process memory.
//!
//! The sweep erases the expired records of a Range or HashRange schema, oldest
//! first, and the erase leaves deleted records at the oldest keys of each
//! hash. A pass that read them again from the start of the hash would pay for
//! them each time. So the sweep keeps a floor for each hash: the key where a
//! page of the leading run of deleted records ended. The next pass of the hash
//! starts after its floor (`lastdb_node/src/jobs/ttl_sweep/discovery.rs`).
//!
//! A write below a floor takes effect at once. After the commit of its batch,
//! each local or peer write notes the keys of the records that it leaves live
//! (`SchemaStore::note_live_writes`):
//!
//! - A key at or below the floor of its hash removes the floor. The next pass
//!   reads the hash from its start and finds the key.
//! - A key written while a pass reads the hash lowers the mark of that pass,
//!   and the pass then cannot set a floor at or above the key
//!   ([`RetentionFloors::keep`]). This covers a key that the pass read past
//!   before the key was there.
//!
//! The note reads nothing. It is one lock of this map, and a schema that the
//! sweep does not read has no entry. The note runs after the commit, so a pass
//! that starts after the note reads the key. Once in [`FULL_PASS_EVERY`] passes
//! of a hash, and after a restart of the node, a pass reads the hash from its
//! start again.

use super::SchemaStore;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::KeyValue;
use crate::schema::{Schema, SchemaError};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// A pass of a hash starts from the start of the hash once in this many
/// passes.
const FULL_PASS_EVERY: u32 = 64;

/// The state of one hash of one schema.
#[derive(Debug, Default)]
struct Floor {
    /// The key where the leading run of deleted records ended. A pass starts
    /// after it.
    after: Option<KeyValue>,
    /// Passes of the hash that started after this floor.
    passes: u32,
    /// The lowest range key written since the current pass of the hash
    /// started.
    low: Option<String>,
}

impl Floor {
    fn clear(&mut self) {
        self.after = None;
        self.passes = 0;
    }
}

/// The floors of the hashes of each schema that the sweep reads.
#[derive(Debug, Default)]
pub struct RetentionFloors {
    schemas: Mutex<HashMap<String, HashMap<String, Floor>>>,
}

impl RetentionFloors {
    /// A pass of `hash` starts: where it starts reading. After the floor of the
    /// hash, or at its start when it has none or a full pass is due.
    pub fn start(&self, schema: &str, hash: &str) -> Option<KeyValue> {
        let mut schemas = self.lock();
        let floor = schemas
            .entry(schema.to_owned())
            .or_default()
            .entry(hash.to_owned())
            .or_default();
        floor.low = None;
        if floor.after.is_some() {
            floor.passes += 1;
            if floor.passes >= FULL_PASS_EVERY {
                floor.clear();
            }
        }
        floor.after.clone()
    }

    /// Move the floor of `hash` to `after`: every key up to it was a deleted
    /// record when this pass read it. A key at or below `after` that a write
    /// noted since the pass started refuses the floor, and the hash then has
    /// none.
    pub fn keep(&self, schema: &str, hash: &str, after: &KeyValue) {
        let mut schemas = self.lock();
        let floor = schemas
            .entry(schema.to_owned())
            .or_default()
            .entry(hash.to_owned())
            .or_default();
        if floor
            .low
            .as_deref()
            .is_some_and(|low| low <= range_of(after))
        {
            floor.clear();
            return;
        }
        floor.after = Some(after.clone());
    }

    /// The pass read the last key of `hash`. A hash with no floor leaves the
    /// map.
    pub fn end(&self, schema: &str, hash: &str) {
        let mut schemas = self.lock();
        let Some(hashes) = schemas.get_mut(schema) else {
            return;
        };
        if hashes.get(hash).is_some_and(|floor| floor.after.is_none()) {
            hashes.remove(hash);
        }
        if hashes.is_empty() {
            schemas.remove(schema);
        }
    }

    /// Note the keys, as (hash, range) pairs, of the live records that a
    /// committed batch wrote in `schema`.
    fn note_written(&self, schema: &str, keys: &[(String, String)]) {
        let mut schemas = self.lock();
        let Some(hashes) = schemas.get_mut(schema) else {
            return;
        };
        for (hash, range) in keys {
            let Some(floor) = hashes.get_mut(hash) else {
                continue;
            };
            if floor
                .after
                .as_ref()
                .is_some_and(|after| range.as_str() <= range_of(after))
            {
                floor.clear();
            }
            if floor.low.as_ref().is_none_or(|low| range < low) {
                floor.low = Some(range.clone());
            }
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, HashMap<String, Floor>>> {
        self.schemas.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The range of a key: the sweep orders the keys of one hash by range, and an
/// expired key starts with its entry time in fixed-width digits, so text
/// order is time order.
fn range_of(key: &KeyValue) -> &str {
    key.range.as_deref().unwrap_or("")
}

impl SchemaStore {
    /// The floors of the TTL sweep.
    #[must_use]
    pub fn retention_floors(&self) -> &RetentionFloors {
        &self.retention_floors
    }

    /// After the commit of the batch in scope, note `live`, the keys of the
    /// records of `schema` that the batch leaves live, in the floors of the
    /// TTL sweep. Only a Range or HashRange schema has floors. The note reads
    /// nothing and cannot fail.
    pub(crate) fn note_live_writes<'k>(
        &self,
        schema: &Schema,
        live: impl Iterator<Item = &'k KeyValue>,
    ) -> Result<(), SchemaError> {
        let partitioned = match schema.schema_type {
            DeclarativeSchemaType::Range => false,
            DeclarativeSchemaType::HashRange => true,
            DeclarativeSchemaType::Hash | DeclarativeSchemaType::Single => return Ok(()),
        };
        // The sweep reads a Range schema as one hash, the empty one.
        let keys: Vec<(String, String)> = live
            .map(|key| {
                let hash = if partitioned {
                    key.hash.clone().unwrap_or_default()
                } else {
                    String::new()
                };
                (hash, range_of(key).to_owned())
            })
            .collect();
        if keys.is_empty() {
            return Ok(());
        }
        let floors = Arc::clone(&self.retention_floors);
        let name = schema.name.clone();
        crate::storage::disk_pack::transaction::after_commit(&self.runtime, async move {
            floors.note_written(&name, &keys);
            Ok(())
        })?;
        Ok(())
    }
}
