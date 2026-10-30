//! Canonical write paths come from exact schema-field bindings, never raw keys.

use super::super::molecules::DeferredPersistJob;
use super::*;
use crate::atom::{AtomEntry, KeyMetadata};
use crate::db_operations::{ChangedKey, MoleculeData};
use crate::storage::disk_pack::logical_paths::source::CertifiedAtom;
use crate::storage::disk_pack::logical_paths::{schema_identity, LogicalPath, LogicalPathRecord};
use crate::storage::disk_pack::{transaction, PackRuntime};
use crate::write_preparation::measure;
use futures::FutureExt;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

mod bindings;
mod positive;
mod removed;

struct ChangedTip {
    molecule: String,
    hash: String,
    range: String,
    entry: AtomEntry,
    metadata: Option<KeyMetadata>,
}

#[derive(Clone, PartialEq, Eq)]
struct Binding {
    schema: String,
    field: String,
    name: String,
}

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) fn reserve_native_core_memory(
        &self,
        runtime: &PackRuntime,
    ) -> Result<(), SchemaError> {
        let bytes = transaction::memory_quote(runtime)?;
        if let Some(reservation) = self.try_reserve_defer(bytes) {
            transaction::retain_raw_memory_reservation(runtime, reservation)?;
        } else {
            transaction::require_durable(runtime)?;
        }
        Ok(())
    }

    pub(in crate::fold_db_core::mutation_manager) async fn stage_native_paths(
        &self,
        job: &DeferredPersistJob,
        runtime: &PackRuntime,
    ) -> Result<(), SchemaError> {
        self.stage_native_scope(job, runtime, job.storage_prefix.as_deref())
            .boxed()
            .await?;
        if job.storage_prefix.is_none() {
            for scope in &job.share_prefixes {
                self.stage_native_scope(job, runtime, Some(scope))
                    .boxed()
                    .await?;
            }
        }
        Ok(())
    }

    async fn stage_native_scope(
        &self,
        job: &DeferredPersistJob,
        runtime: &PackRuntime,
        scope: Option<&str>,
    ) -> Result<(), SchemaError> {
        self.stage_native_headers(job, runtime, scope)?;
        // Schema-field definitions are global. The logical records
        // below use this request's scope.
        let tips = changed_tips(job);
        measure(
            "path_controls_us",
            self.stage_native_controls(runtime, scope, &changed_molecules(job)),
        )
        .boxed()
        .await?;
        let bindings = measure(
            "path_bindings_us",
            self.native_path_bindings(job, runtime, &tips),
        )
        .await?;
        let Some(atoms) = measure("path_atoms_us", self.path_atoms(&tips, scope)).await? else {
            return Ok(transaction::require_durable(runtime)?);
        };
        let Some(records) = self.native_path_records(scope, &tips, &atoms, &bindings)? else {
            return Ok(transaction::require_durable(runtime)?);
        };
        match transaction::stage_paths(runtime, records) {
            Err(crate::storage::StorageError::WriteResourceLimit { .. }) => {
                transaction::require_durable(runtime)?;
            }
            result => result?,
        }
        self.stage_removed_job_paths(job, runtime, scope).await
    }

    /// Name the exact tip key and the current atom placement.
    fn path_sources(
        &self,
        tip: &ChangedTip,
        scope: Option<&str>,
        atom_key: &str,
    ) -> Result<Vec<String>, SchemaError> {
        let atoms = self.db_ops.atoms();
        let codec = atoms.key_codec_for_molecule(&tip.molecule)?;
        let key = codec
            .api_hash_range_record_key(&tip.molecule, &tip.hash, &tip.range)
            .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
        Ok(vec![
            crate::schema::types::field::build_storage_key(scope, &key),
            atom_key.to_owned(),
        ])
    }

    /// Fetch preferred targets, retaining each body's certified actual copy.
    async fn path_atoms(
        &self,
        tips: &[ChangedTip],
        storage_prefix: Option<&str>,
    ) -> Result<Option<BTreeMap<String, CertifiedAtom>>, SchemaError> {
        let mut targets = BTreeMap::new();
        for tip in tips {
            let key = self
                .db_ops
                .atoms()
                .body_storage_key(&tip.entry.atom_uuid, storage_prefix);
            targets.entry(key).or_insert(tip.entry.atom_uuid.as_str());
        }
        if targets.len() > 10_000 {
            return Ok(None);
        }
        let ids: Vec<&str> = targets.values().copied().collect();
        let fetched = self.db_ops.atoms().get_atoms(&ids, storage_prefix).await?;
        // A body that this scope does not hold makes the write fall back to
        // the durable path. Memory keeps no path for a tip without a body.
        let Some(expected) = fetched.into_iter().collect::<Option<Vec<_>>>() else {
            return Ok(None);
        };
        let runtime = self.db_ops.namespaced_store().disk_pack_runtime();
        let certified = crate::storage::disk_pack::logical_paths::source::certified_atoms(
            runtime,
            storage_prefix,
            expected,
        )?;
        let mut bytes = 0usize;
        let mut atoms = BTreeMap::new();
        for ((target, _), atom) in targets.into_iter().zip(certified) {
            let Some(atom) = atom else { return Ok(None) };
            bytes = bytes.saturating_add(
                serde_json::to_vec(atom.atom.as_ref())
                    .map_err(|error| SchemaError::InvalidData(error.to_string()))?
                    .len(),
            );
            if bytes > 128 * 1024 * 1024 {
                return Ok(None);
            }
            atoms.insert(target, atom);
        }
        Ok(Some(atoms))
    }
}

fn changed_molecules(job: &DeferredPersistJob) -> Vec<String> {
    job.modified_fields
        .keys()
        .filter_map(|name| {
            job.schema
                .runtime_fields
                .get(name)?
                .common()
                .molecule_uuid()
        })
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Slots that have molecule data.
fn changed_field_slots<'a>(
    job: &'a DeferredPersistJob,
) -> impl Iterator<Item = (&'a str, &'a MoleculeData, &'a HashSet<ChangedKey>)> + 'a {
    job.modified_fields.iter().filter_map(|(name, keys)| {
        let field = job.schema.runtime_fields.get(name)?;
        Some((
            field.common().molecule_uuid()?.as_str(),
            field.molecule_data()?,
            keys,
        ))
    })
}

fn changed_tips(job: &DeferredPersistJob) -> Vec<ChangedTip> {
    let mut tips = BTreeMap::new();
    for (molecule, data, keys) in changed_field_slots(job) {
        for key in keys {
            let (hash, range) = (key.hash_or_empty(), key.range_or_empty());
            if let Some(entry) = data.get_atom_entry(hash, range) {
                tips.insert(
                    (molecule.to_string(), hash.to_string(), range.to_string()),
                    (entry.clone(), data.get_key_metadata(hash, range).cloned()),
                );
            }
        }
    }
    tips.into_iter()
        .map(|((molecule, hash, range), (entry, metadata))| ChangedTip {
            molecule,
            hash,
            range,
            entry,
            metadata,
        })
        .collect()
}
