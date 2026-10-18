//! Canonical write paths come from exact schema-field bindings, never raw keys.

use super::super::molecules::DeferredPersistJob;
use super::*;
use crate::atom::{AtomEntry, KeyMetadata};
use crate::storage::disk_pack::logical_paths::source::CertifiedAtom;
use crate::storage::disk_pack::logical_paths::{schema_identity, LogicalPath, LogicalPathRecord};
use crate::storage::disk_pack::{transaction, PackRuntime};
use futures::FutureExt;
use std::collections::BTreeMap;
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
    envelope: bool,
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
        self.stage_native_scope(job, runtime, job.storage_prefix.as_deref(), true)
            .boxed()
            .await?;
        if job.storage_prefix.is_none() {
            for scope in &job.share_prefixes {
                self.stage_native_scope(job, runtime, Some(scope), false)
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
        siblings: bool,
    ) -> Result<(), SchemaError> {
        self.stage_native_headers(job, runtime, scope, siblings)?;
        let mut atom_ids: Vec<_> = job
            .deferred_atoms
            .iter()
            .flatten()
            .map(|(atom, _)| atom.uuid().to_owned())
            .collect();
        atom_ids.sort();
        atom_ids.dedup();
        self.db_ops
            .atoms()
            .stage_atom_locators(runtime, scope, &atom_ids)
            .await?;
        if !self
            .db_ops
            .atoms()
            .molecule_ref_edges_v2_complete(scope)
            .await?
        {
            transaction::require_durable(runtime)?;
        }
        if !runtime.write_has_feature(laststore::disk_pack::FEATURE_SCHEMA_RECORD_REFS) {
            transaction::require_durable(runtime)?;
        }
        let tips = changed_tips(job, siblings);
        self.stage_native_controls(runtime, scope, &tips)
            .boxed()
            .await?;
        let bindings = self
            .native_path_bindings(job, runtime, scope, &tips)
            .await?;
        let Some(atoms) = self.path_atoms(&tips, scope).await? else {
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
        self.stage_removed_job_paths(job, runtime, scope, siblings)
            .await
    }

    /// Name each accepted tip spelling and the current atom placement candidate.
    fn path_sources(
        &self,
        tip: &ChangedTip,
        scope: Option<&str>,
        atom_key: &str,
    ) -> Result<Vec<String>, SchemaError> {
        let atoms = self.db_ops.atoms();
        let codec = atoms.key_codec_for_molecule(&tip.molecule);
        let mut sources: Vec<_> = codec
            .api_hash_range_record_keys_for_read(&tip.molecule, &tip.hash, &tip.range)
            .map_err(|error| SchemaError::InvalidData(error.to_string()))?
            .into_iter()
            .map(|key| crate::schema::types::field::build_storage_key(scope, &key))
            .collect();
        sources.push(atom_key.to_owned());
        Ok(sources)
    }

    fn path_atom_target(
        &self,
        tip: &ChangedTip,
        scope: Option<&str>,
    ) -> Result<(String, crate::atom::AtomPartition), SchemaError> {
        let atoms = self.db_ops.atoms();
        let hash = atoms.storage_hash(&tip.molecule, &tip.hash)?;
        let partition = crate::atom::AtomPartition::for_slot(&tip.molecule, &hash);
        let key = atoms.primary_atom_key(&tip.entry.atom_uuid, Some(&partition), scope);
        Ok((key, partition))
    }

    /// Fetch preferred targets, retaining each body's certified actual copy.
    async fn path_atoms(
        &self,
        tips: &[ChangedTip],
        storage_prefix: Option<&str>,
    ) -> Result<Option<BTreeMap<String, CertifiedAtom>>, SchemaError> {
        let mut targets = BTreeMap::new();
        for tip in tips {
            let (key, partition) = self.path_atom_target(tip, storage_prefix)?;
            targets
                .entry(key)
                .or_insert((tip.entry.atom_uuid.as_str(), partition));
        }
        if targets.len() > 10_000 {
            return Ok(None);
        }
        let slots: Vec<_> = targets
            .values()
            .map(|(id, partition)| (*id, Some(partition.clone())))
            .collect();
        let fetched = self
            .db_ops
            .atoms()
            .get_atoms_located(&slots, storage_prefix)
            .await?;
        let expected: Vec<_> = fetched
            .into_iter()
            .map(|atom| {
                atom.ok_or_else(|| SchemaError::StorageRead {
                    detail: "native write path has no staged or stored atom content".into(),
                })
            })
            .collect::<Result<_, _>>()?;
        let runtime = self.db_ops.namespaced_store().disk_pack_runtime();
        let certified = crate::storage::disk_pack::logical_paths::source::certified_atoms(
            runtime,
            storage_prefix,
            &expected,
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

fn changed_tips(job: &DeferredPersistJob, include_siblings: bool) -> Vec<ChangedTip> {
    let entry = job.modified_fields.iter().filter_map(|(name, keys)| {
        let field = job.schema.runtime_fields.get(name)?;
        Some((
            field.common().molecule_uuid()?.as_str(),
            field.molecule_data()?,
            keys,
        ))
    });
    let siblings = job
        .sibling_updates
        .iter()
        .filter(|_| include_siblings)
        .map(|(molecule, data, keys)| (molecule.as_str(), data, keys));
    let mut tips = BTreeMap::new();
    for (molecule, data, keys) in entry.chain(siblings) {
        for key in keys {
            let (hash, range) = (key.disk_hash(), key.disk_range());
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
