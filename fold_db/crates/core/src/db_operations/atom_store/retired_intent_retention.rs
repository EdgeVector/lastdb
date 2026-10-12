//! Permanent source-owned roots for encrypted retired-intent recovery.
//! No expiry or release exists here. Partial roots retain bytes after a failure.

use super::AtomStore;
use crate::schema::SchemaError;
use crate::sync::log::MutationEnvelope;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use tokio::sync::{OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard};

mod dependencies;
mod reclaim;
mod storage;

/// This namespace uses the normal encryption wrapper and default backup role.
pub(crate) const RETIRED_INTENT_RETENTION_NAMESPACE: &str = "retired_intent_retention";

/// Keep this guard through the outcome's durable write and exact readback.
pub(crate) struct RetiredIntentRetentionGuard {
    store: AtomStore,
    _reclaim: OwnedRwLockWriteGuard<()>,
    _atoms: Vec<OwnedMutexGuard<()>>,
    _blobs: Vec<OwnedMutexGuard<()>>,
    attempted: bool,
}

/// Fixed proof facts only; identities and values stay inside the native caller.
pub(crate) struct RetiredIntentRetentionProof {
    pub(crate) source_owner_exact: bool,
    pub(crate) atom_roots: u64,
    pub(crate) blob_roots: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ScopedAtoms {
    storage_prefix: Option<String>,
    atoms: BTreeSet<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SourceRoots {
    version: u8,
    source_id: String,
    scopes: Vec<ScopedAtoms>,
    blobs: BTreeSet<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct TargetOwners {
    version: u8,
    kind: String,
    storage_prefix: Option<String>,
    target: String,
    owners: BTreeSet<String>,
}

fn refused(message: &'static str) -> SchemaError {
    SchemaError::InvalidData(message.into())
}

fn source_id_valid(source_id: &str) -> bool {
    source_id.len() == 64
        && source_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

impl AtomStore {
    /// Reclaim holds a shared fence through its final delete. Retention is exclusive.
    pub(crate) async fn retired_intent_reclaim_guard(&self) -> OwnedRwLockReadGuard<()> {
        Arc::clone(&self.retired_intent_retention_gate)
            .read_owned()
            .await
    }

    /// Fence reclaim before materialization and signature preflight. No roots write.
    pub(crate) async fn retired_intent_retention_guard(
        &self,
    ) -> Result<RetiredIntentRetentionGuard, SchemaError> {
        if self.namespaced_store.is_none() {
            return Err(refused("retired intent retention requires a native store"));
        }
        let reclaim = Arc::clone(&self.retired_intent_retention_gate)
            .write_owned()
            .await;
        Ok(RetiredIntentRetentionGuard {
            store: self.clone(),
            _reclaim: reclaim,
            _atoms: Vec::new(),
            _blobs: Vec::new(),
            attempted: false,
        })
    }

    /// Verify exact durable roots and local dependencies; never write or heal.
    pub(crate) async fn verify_retired_intent_retention(
        &self,
        source_id: &str,
        envelopes: &[MutationEnvelope],
    ) -> Result<RetiredIntentRetentionProof, SchemaError> {
        if !source_id_valid(source_id) || envelopes.is_empty() {
            return Err(refused("retired intent retention source is invalid"));
        }
        let _reclaim = self.retired_intent_reclaim_guard().await;
        let mut roots = dependency_roots(source_id, envelopes);
        let ids = dependency_atom_ids(&roots);
        let _atoms = self.lock_automatic_gc_atoms(&ids).await;
        self.add_referenced_atom_blobs(&mut roots, envelopes)
            .await?;
        let blob_ids: Vec<String> = roots.blobs.iter().cloned().collect();
        let _blobs = self.lock_liveness_blobs(&blob_ids).await;
        self.require_retired_blob_presence(&blob_ids).await?;
        self.verify_retired_source_roots(&roots).await?;
        Ok(RetiredIntentRetentionProof {
            source_owner_exact: true,
            atom_roots: roots
                .scopes
                .iter()
                .map(|scope| scope.atoms.len() as u64)
                .sum(),
            blob_roots: roots.blobs.len() as u64,
        })
    }
}

impl RetiredIntentRetentionGuard {
    /// Call once after signature and aggregate preflight; hold through outcome readback.
    /// File-pointer roots retain the local file bytes, not only their atom body.
    pub(crate) async fn retain(
        &mut self,
        source_id: &str,
        envelopes: &[MutationEnvelope],
    ) -> Result<(), SchemaError> {
        if self.attempted {
            return Err(refused("retired intent retention attempt already started"));
        }
        self.attempted = true;
        if !source_id_valid(source_id) || envelopes.is_empty() {
            return Err(refused("retired intent retention source is invalid"));
        }
        let mut roots = dependency_roots(source_id, envelopes);
        let ids = dependency_atom_ids(&roots);
        self._atoms = self.store.lock_automatic_gc_atoms(&ids).await;
        self.store
            .add_referenced_atom_blobs(&mut roots, envelopes)
            .await?;
        let blob_ids: Vec<String> = roots.blobs.iter().cloned().collect();
        self._blobs = self.store.lock_liveness_blobs(&blob_ids).await;
        self.store.require_retired_blob_presence(&blob_ids).await?;
        self.store.commit_retired_source_roots(&roots).await
    }
}

fn dependency_atom_ids(roots: &SourceRoots) -> Vec<String> {
    roots
        .scopes
        .iter()
        .flat_map(|scope| scope.atoms.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn dependency_roots(source_id: &str, envelopes: &[MutationEnvelope]) -> SourceRoots {
    let mut scopes: BTreeMap<Option<String>, BTreeSet<String>> = BTreeMap::new();
    let mut blobs = BTreeSet::new();
    for envelope in envelopes {
        scopes
            .entry(envelope.storage_prefix.clone())
            .or_default()
            .extend(envelope.field_atom_uuids.values().cloned());
        for value in envelope.fields_and_values.values() {
            blobs.extend(crate::atom::file_pointer::blob_refs_of_atom(
                value,
                envelope.metadata.as_ref(),
            ));
        }
        // Metadata can name a file even when the intent has no inline fields.
        blobs.extend(crate::atom::file_pointer::blob_refs_of_atom(
            &serde_json::Value::Null,
            envelope.metadata.as_ref(),
        ));
    }
    SourceRoots {
        version: 1,
        source_id: source_id.to_string(),
        scopes: scopes
            .into_iter()
            .map(|(storage_prefix, atoms)| ScopedAtoms {
                storage_prefix,
                atoms,
            })
            .collect(),
        blobs,
    }
}

fn inline_atom_is_complete(id: &str, prefix: Option<&str>, envelopes: &[MutationEnvelope]) -> bool {
    envelopes.iter().any(|envelope| {
        envelope.storage_prefix.as_deref() == prefix
            && envelope.field_atom_uuids.iter().any(|(field, expected)| {
                expected == id
                    && envelope.fields_and_values.get(field).is_some_and(|value| {
                        crate::atom::Atom::new(envelope.schema_name.clone(), value.clone()).uuid()
                            == id
                    })
            })
    })
}
