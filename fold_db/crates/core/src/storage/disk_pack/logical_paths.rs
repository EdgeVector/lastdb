//! Native logical paths own their immutable atom content in the existing warm set.

use super::*;
use crate::schema::types::Schema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub(super) mod body;
pub(crate) mod field;
mod read;
pub(crate) mod warm;
pub(crate) use read::admit_query_results;

/// A memory identity, never a physical collection or a stored key.
pub(crate) const PATH_COLLECTION: &str = "__native_logical_paths_v1";
const MAX_PATH_BATCH_BYTES: usize = 128 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LogicalPath {
    pub scope: Option<String>,
    pub schema: String,
    pub field: String,
    pub molecule: String,
    pub hash: String,
    pub range: String,
    pub atom: String,
}

impl LogicalPath {
    /// JSON tuple boundaries preserve every byte, including separators and NULs.
    pub(crate) fn id(&self) -> StorageResult<String> {
        if self.schema.is_empty()
            || self.field.is_empty()
            || self.molecule.is_empty()
            || self.atom.is_empty()
        {
            return Err(StorageError::InvalidOperation(
                "native logical path requires complete schema, field, molecule and atom identities"
                    .into(),
            ));
        }
        serde_json::to_string(&(
            &self.scope,
            &self.schema,
            &self.field,
            &self.molecule,
            &self.hash,
            &self.range,
            &self.atom,
        ))
        .map_err(|error| serialization(&error))
    }
}

pub(crate) fn schema_identity(schema: &Schema) -> StorageResult<String> {
    serde_json::to_string(&(&schema.name, &schema.identity_hash))
        .map_err(|error| serialization(&error))
}

#[derive(Clone)]
pub(crate) struct LogicalPathRecord {
    pub path: LogicalPath,
    /// Immutable atom content. Per-tip provenance stays in the captured query.
    pub body: Vec<u8>,
}

impl LogicalPathRecord {
    pub(crate) fn from_value(path: LogicalPath, value: &serde_json::Value) -> StorageResult<Self> {
        Ok(Self {
            path,
            body: serde_json::to_vec(&body::PathBody::content(value))
                .map_err(|error| serialization(&error))?,
        })
    }
}

pub(crate) struct PathProtection {
    logical: Arc<Mutex<LogicalResidentSet>>,
    keys: Vec<(String, String)>,
    pending: Mutex<Option<BTreeMap<String, Vec<u8>>>>,
}

impl PathProtection {
    /// Reserve the whole path set before publication. Failure admits no bodies.
    pub(crate) fn reserve(
        runtime: &PackRuntime,
        records: &[LogicalPathRecord],
    ) -> StorageResult<Arc<Self>> {
        let mut bodies = BTreeMap::new();
        let mut bytes = 0usize;
        for record in records {
            let id = record.path.id()?;
            bytes = bytes
                .checked_add(id.len())
                .and_then(|n| n.checked_add(record.body.len()))
                .filter(|n| *n <= MAX_PATH_BATCH_BYTES)
                .ok_or_else(capacity)?;
            if let Some(previous) = bodies.get(&id) {
                if previous != &record.body {
                    return Err(StorageError::BackendError(
                        "one native atom path supplied different content bytes".into(),
                    ));
                }
            } else {
                bodies.insert(id, record.body.clone());
            }
        }
        let mut owner = Self {
            logical: Arc::clone(&runtime.logical),
            keys: Vec::new(),
            pending: Mutex::new(Some(bodies)),
        };
        let mut logical = owner
            .logical
            .lock()
            .expect("native canonical write protection");
        for id in owner
            .pending
            .lock()
            .expect("native staged path bodies")
            .as_ref()
            .expect("unpublished path bodies")
            .keys()
        {
            if !logical.hold_record(PATH_COLLECTION, id) {
                logical.release_records(std::mem::take(&mut owner.keys));
                return Err(capacity());
            }
            owner.keys.push((PATH_COLLECTION.to_string(), id.clone()));
        }
        drop(logical);
        Ok(Arc::new(owner))
    }

    /// Physical and journal reservations must succeed before this memory publication.
    pub(crate) fn publish(&self) -> StorageResult<()> {
        let Some(bodies) = self
            .pending
            .lock()
            .expect("native staged path bodies")
            .take()
        else {
            return Ok(());
        };
        let mut logical = self
            .logical
            .lock()
            .expect("native canonical write publication");
        for (id, body) in bodies {
            let epoch = logical.record_epoch(&id);
            if !logical.admit_record(PATH_COLLECTION, &id, Some(body), epoch) {
                return Err(StorageError::BackendError(
                    "native protected logical path lost its reserved slot".into(),
                ));
            }
        }
        Ok(())
    }
}

impl Drop for PathProtection {
    fn drop(&mut self) {
        if let Ok(mut logical) = self.logical.lock() {
            logical.release_records(std::mem::take(&mut self.keys));
        }
    }
}

fn capacity() -> StorageError {
    StorageError::WriteResourceLimit {
        detail: "native complete mutation cannot reserve its canonical logical paths".into(),
    }
}

fn serialization(error: &serde_json::Error) -> StorageError {
    StorageError::BackendError(format!("native logical path encoding: {error}"))
}
