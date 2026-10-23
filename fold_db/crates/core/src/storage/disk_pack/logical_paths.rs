//! Native logical paths retain the actual shared core molecule and atom objects.

use super::*;
use crate::atom::{Atom, AtomEntry, KeyMetadata};
use crate::resident::{CorePath, CoreWarmSlot};
use crate::schema::types::Schema;
use serde::{Deserialize, Serialize};

pub(crate) mod absence;
pub(super) mod body;
mod completeness;
pub(super) mod controls;
pub(crate) mod core_filter;
pub(crate) mod core_read;
mod dependencies;
pub(crate) mod field;
mod protection;
mod read;
pub(crate) mod source;
mod tips;

pub(crate) use protection::{PathProtection, PathPublication};

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
    coordinates: Vec<(String, String)>,
    complete: bool,
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
            coordinates: Vec::new(),
            complete: false,
        })
    }

    pub(crate) fn with_sources(mut self, sources: Vec<String>) -> Self {
        self.sources = sources;
        (self.complete, self.coordinates) = self.compute_dependencies();
        self
    }

    pub(crate) fn source_coordinates(&self) -> &[(String, String)] {
        &self.coordinates
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        let path = self.path.id().map_or(1024, |id| id.len());
        dependency_bytes(&self.sources, &self.coordinates)
            .saturating_add(self.bytes)
            .saturating_add(path.saturating_mul(3))
    }

    fn source_dependencies(&self) -> (bool, Vec<(String, String)>) {
        (self.complete, self.coordinates.clone())
    }

    fn compute_dependencies(&self) -> (bool, Vec<(String, String)>) {
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
            if crate::atom::atom_key_codec::body_uuid_of(base) == Some(self.path.atom.as_str()) {
                let collection = crate::storage::laststore::native_routing::collection_for_key(
                    source.as_bytes(),
                );
                let encoded = crate::storage::laststore::native_routing::encode(source.as_bytes());
                sources.push((collection.to_owned(), encoded));
                body = true;
            } else if crate::atom::molecule_key_codec::molecule_uuid_from_storage_key(base)
                .is_some()
            {
                candidates.push(base.to_owned());
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

/// A complete resolver proved that this logical path has no visible tip.
#[derive(Clone)]
pub(crate) struct LogicalPathAbsence {
    pub path: LogicalPath,
    sources: Vec<String>,
    coordinates: Vec<(String, String)>,
    complete: bool,
}

impl LogicalPathAbsence {
    pub(crate) fn new(path: LogicalPath, sources: Vec<String>) -> Self {
        let mut absence = Self {
            path,
            sources,
            coordinates: Vec::new(),
            complete: false,
        };
        (absence.complete, absence.coordinates) = absence.compute_dependencies();
        absence
    }

    pub(crate) fn bytes(&self) -> usize {
        let path = self.path.id().map_or(1024, |id| id.len());
        dependency_bytes(&self.sources, &self.coordinates).saturating_add(path.saturating_mul(3))
    }

    pub(crate) fn source_coordinates(&self) -> &[(String, String)] {
        &self.coordinates
    }

    fn source_dependencies(&self) -> (bool, Vec<(String, String)>) {
        (self.complete, self.coordinates.clone())
    }

    fn compute_dependencies(&self) -> (bool, Vec<(String, String)>) {
        let scope = self.path.scope.as_deref();
        let candidates: Vec<_> = self
            .sources
            .iter()
            .filter_map(|source| {
                let bare = match scope {
                    Some(scope) => source.strip_prefix(scope)?.strip_prefix(':')?,
                    None => source.as_str(),
                };
                crate::atom::molecule_key_codec::molecule_uuid_from_storage_key(bare)
                    .is_some()
                    .then(|| bare.to_owned())
            })
            .collect();
        let complete = !candidates.is_empty();
        (
            complete,
            dependencies::tip_sources(scope, &self.path.molecule, &candidates),
        )
    }
}

fn dependency_bytes(sources: &[String], coordinates: &[(String, String)]) -> usize {
    let bytes = sources.iter().fold(1024usize, |bytes, source| {
        bytes
            .saturating_add(source.len().saturating_mul(2))
            .saturating_add(64)
    });
    coordinates.iter().fold(bytes, |bytes, (collection, key)| {
        bytes
            .saturating_add(collection.len())
            .saturating_add(key.len())
            .saturating_add(128)
    })
}

fn serialization(error: &serde_json::Error) -> StorageError {
    StorageError::BackendError(format!("logical core path encoding: {error}"))
}
