//! Native logical paths retain the actual shared core molecule and atom objects.

use super::*;
use crate::atom::{Atom, AtomEntry, KeyMetadata};
use crate::resident::{CorePath, CoreWarmSlot};
use crate::schema::types::Schema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub(crate) mod absence;
pub(super) mod body;
pub(super) mod controls;
pub(crate) mod core_filter;
pub(crate) mod core_read;
mod dependencies;
pub(crate) mod field;
mod read;
pub(crate) mod record;
pub(crate) mod source;

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
    pub(super) fn core(&self) -> CorePath {
        CorePath {
            scope: self.scope.clone(),
            schema: self.schema.clone(),
            field: self.field.clone(),
            molecule: self.molecule.clone(),
            hash: self.hash.clone(),
            range: self.range.clone(),
        }
    }

    pub(crate) fn id(&self) -> StorageResult<String> {
        if self.schema.is_empty() || self.field.is_empty() || self.molecule.is_empty() {
            return Err(StorageError::InvalidOperation(
                "logical path identity is incomplete".into(),
            ));
        }
        serde_json::to_string(&(
            &self.scope,
            &self.schema,
            &self.field,
            &self.molecule,
            &self.hash,
            &self.range,
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
    pub slot: CoreWarmSlot,
    pub bytes: usize,
    sources: Vec<String>,
}

impl LogicalPathRecord {
    pub(crate) fn from_atom(
        path: LogicalPath,
        atom: Arc<Atom>,
        entry: AtomEntry,
        metadata: Option<KeyMetadata>,
    ) -> StorageResult<Self> {
        if path.atom != atom.uuid() || entry.atom_uuid != atom.uuid() {
            return Err(StorageError::BackendError(
                "logical tip and atom identities differ".into(),
            ));
        }
        let bytes = body::encoded_len(atom.as_ref())?;
        Ok(Self {
            path,
            slot: CoreWarmSlot {
                entry,
                metadata,
                atom,
                bytes,
            },
            bytes,
            sources: Vec::new(),
        })
    }

    pub(crate) fn with_sources(mut self, sources: Vec<String>) -> Self {
        self.sources = sources;
        self
    }

    fn source_dependencies(&self) -> (bool, Vec<(String, String)>) {
        let scope = self.path.scope.as_deref();
        let mut candidates = Vec::new();
        let mut sources = Vec::new();
        let mut body = false;
        for source in &self.sources {
            let base = match scope {
                Some(scope) => source
                    .strip_prefix(scope)
                    .and_then(|key| key.strip_prefix(':')),
                None => Some(source.as_str()),
            };
            let Some(base) = base else { continue };
            if crate::atom::molecule_key_codec::molecule_uuid_from_storage_key(base).is_some() {
                candidates.push(base.to_owned());
            } else if crate::atom::atom_key_codec::uuid_of(base) == Some(self.path.atom.as_str()) {
                let collection =
                    crate::storage::laststore::native_routing::write_collection(source.as_bytes());
                let encoded = crate::storage::laststore::native_routing::encode(source.as_bytes());
                sources.push((collection.to_owned(), encoded));
                body = true;
            }
        }
        let complete = body && !candidates.is_empty();
        sources.extend(dependencies::tip_sources(
            scope,
            &self.path.molecule,
            &candidates,
        ));
        (complete, sources)
    }
}

pub(crate) struct PathProtection {
    core: Arc<Mutex<crate::resident::CoreWarmSet>>,
    keys: Vec<CorePath>,
    pending: Mutex<Option<Vec<LogicalPathRecord>>>,
}

impl PathProtection {
    pub(crate) fn reserve(
        runtime: &PackRuntime,
        records: &[LogicalPathRecord],
    ) -> StorageResult<Arc<Self>> {
        let records: BTreeMap<_, _> = records
            .iter()
            .map(|record| (record.path.core(), record.clone()))
            .collect();
        records.values().try_fold(0usize, |n, record| {
            n.checked_add(record.bytes)
                .filter(|n| *n <= 128 * 1024 * 1024)
                .ok_or_else(capacity)
        })?;
        let keys: Vec<_> = records.keys().cloned().collect();
        let keys = runtime
            .core
            .lock()
            .expect("core write reservation")
            .reserve_paths(&keys)
            .map_err(|_| capacity())?;
        Ok(Arc::new(Self {
            core: Arc::clone(&runtime.core),
            keys,
            pending: Mutex::new(Some(records.into_values().collect())),
        }))
    }

    /// The caller holds the complete-view write lock until publication finishes.
    pub(crate) fn publish(&self, sequence: u64) -> StorageResult<()> {
        let Some(records) = self.pending.lock().expect("core prepared paths").take() else {
            return Ok(());
        };
        let mut core = self.core.lock().expect("core write publication");
        for record in records {
            let path = record.path.core();
            let (mut complete, sources) = record.source_dependencies();
            core.admit(&path, record.slot, Some(sequence))
                .map_err(|error| StorageError::BackendError(error.to_string()))?;
            core.clear_sources(&path);
            for (collection, key) in sources {
                complete &= core.link_source(&path, &collection, &key);
            }
            if complete {
                core.authorize_path(&path);
            } else {
                core.invalidate_slot(&path);
            }
        }
        Ok(())
    }
}

impl Drop for PathProtection {
    fn drop(&mut self) {
        if let Ok(mut core) = self.core.lock() {
            core.release_paths(&self.keys);
        }
    }
}

fn capacity() -> StorageError {
    StorageError::WriteResourceLimit {
        detail: "native mutation cannot reserve its logical core paths".into(),
    }
}

fn serialization(error: &serde_json::Error) -> StorageError {
    StorageError::BackendError(format!("logical core path encoding: {error}"))
}
