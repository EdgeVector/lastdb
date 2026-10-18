//! Canonical write paths come from exact schema-field bindings, never raw keys.

use super::super::molecules::DeferredPersistJob;
use super::*;
use crate::atom::{Atom, AtomEntry, KeyMetadata};
use crate::storage::disk_pack::logical_paths::{schema_identity, LogicalPath, LogicalPathRecord};
use crate::storage::disk_pack::{transaction, PackRuntime};
use std::collections::BTreeMap;
use std::sync::Arc;

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
    pub(super) async fn stage_native_paths(
        &self,
        job: &DeferredPersistJob,
        runtime: &PackRuntime,
    ) -> Result<(), SchemaError> {
        if !self.resident_mode.acks_on_resident() {
            return Ok(());
        }
        if job.storage_prefix.is_some() || !job.share_prefixes.is_empty() {
            return Ok(transaction::require_durable(runtime)?);
        }
        if !self
            .db_ops
            .atoms()
            .molecule_ref_edges_v2_complete(None)
            .await?
        {
            return Ok(transaction::require_durable(runtime)?);
        }
        if !runtime.write_has_feature(laststore::disk_pack::FEATURE_SCHEMA_RECORD_REFS) {
            return Ok(transaction::require_durable(runtime)?);
        }
        let tips = changed_tips(job);
        let molecules: std::collections::BTreeSet<_> =
            tips.iter().map(|tip| tip.molecule.clone()).collect();
        let bindings = futures::future::try_join_all(molecules.iter().map(|molecule| async {
            Ok::<_, SchemaError>((molecule.clone(), self.path_bindings(job, molecule).await?))
        }))
        .await?;
        let bindings: BTreeMap<_, _> = bindings.into_iter().collect();
        if bindings.values().any(Option::is_none) {
            return Ok(transaction::require_durable(runtime)?);
        }
        let Some(atoms) = self
            .path_atoms(&tips, job.storage_prefix.as_deref())
            .await?
        else {
            return Ok(transaction::require_durable(runtime)?);
        };
        let mut records = Vec::new();
        for tip in &tips {
            let (atom_key, _) = self.path_atom_target(tip, job.storage_prefix.as_deref())?;
            let atom = &atoms[&atom_key];
            let sources = self.path_sources(tip, job.storage_prefix.as_deref())?;
            for binding in bindings[&tip.molecule]
                .as_ref()
                .expect("complete path bindings")
            {
                if binding.envelope
                    && !crate::record_molecule::parse_document_fields(atom.content())
                        .is_some_and(|fields| fields.contains_key(&binding.name))
                {
                    continue;
                }
                let record = LogicalPathRecord::from_atom(
                    LogicalPath {
                        scope: job.storage_prefix.clone(),
                        schema: binding.schema.clone(),
                        field: binding.field.clone(),
                        molecule: tip.molecule.clone(),
                        hash: tip.hash.clone(),
                        range: tip.range.clone(),
                        atom: tip.entry.atom_uuid.clone(),
                    },
                    Arc::clone(atom),
                    tip.entry.clone(),
                    tip.metadata.clone(),
                )?
                .with_sources(sources.clone());
                if records.len() >= 10_000 {
                    return Ok(transaction::require_durable(runtime)?);
                }
                records.push(record);
            }
        }
        match transaction::stage_paths(runtime, records) {
            Err(crate::storage::StorageError::WriteResourceLimit { .. }) => {
                Ok(transaction::require_durable(runtime)?)
            }
            result => Ok(result?),
        }
    }

    /// Name each accepted tip spelling and the current atom placement candidate.
    fn path_sources(
        &self,
        tip: &ChangedTip,
        scope: Option<&str>,
    ) -> Result<Vec<String>, SchemaError> {
        let atoms = self.db_ops.atoms();
        let codec = atoms.key_codec_for_molecule(&tip.molecule);
        let mut sources: Vec<_> = codec
            .api_hash_range_record_keys_for_read(&tip.molecule, &tip.hash, &tip.range)
            .map_err(|error| SchemaError::InvalidData(error.to_string()))?
            .into_iter()
            .map(|key| crate::schema::types::field::build_storage_key(scope, &key))
            .collect();
        sources.push(self.path_atom_target(tip, scope)?.0);
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

    /// Fetch exact physical targets, then share only identical full core values.
    async fn path_atoms(
        &self,
        tips: &[ChangedTip],
        storage_prefix: Option<&str>,
    ) -> Result<Option<BTreeMap<String, Arc<Atom>>>, SchemaError> {
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
        let mut bytes = 0usize;
        let mut atoms = BTreeMap::new();
        let mut shared: BTreeMap<String, Vec<Arc<Atom>>> = BTreeMap::new();
        let runtime = self.db_ops.namespaced_store().disk_pack_runtime();
        for ((target, (id, partition)), atom) in targets.into_iter().zip(fetched) {
            let atom = atom.ok_or_else(|| SchemaError::StorageRead {
                detail: "native write path has no staged or stored atom content".into(),
            })?;
            if !crate::storage::disk_pack::logical_paths::source::certified_atom(
                runtime.clone(),
                storage_prefix,
                id,
                Some(&partition),
                &target,
                &atom,
            )? {
                return Ok(None);
            }
            bytes = bytes.saturating_add(
                serde_json::to_vec(&atom)
                    .map_err(|error| SchemaError::InvalidData(error.to_string()))?
                    .len(),
            );
            if bytes > 128 * 1024 * 1024 {
                return Ok(None);
            }
            let variants = shared.entry(atom.uuid().to_owned()).or_default();
            let atom = if let Some(existing) = variants.iter().find(|value| value.as_ref() == &atom)
            {
                Arc::clone(existing)
            } else {
                let atom = Arc::new(atom);
                variants.push(Arc::clone(&atom));
                atom
            };
            atoms.insert(target, atom);
        }
        Ok(Some(atoms))
    }

    async fn path_bindings(
        &self,
        job: &DeferredPersistJob,
        molecule: &str,
    ) -> Result<Option<Vec<Binding>>, SchemaError> {
        let mut bindings = schema_bindings(&job.schema, molecule)?;
        if bindings.is_empty() && job.schema.molecule_uuid.as_deref() == Some(molecule) {
            return Ok(None);
        }
        let edges = self
            .db_ops
            .atoms()
            .molecule_ref_edges_for_molecule(molecule, None)
            .await?;
        if !edges.complete {
            return Ok(None);
        }
        if edges.edges.iter().any(|edge| {
            !matches!(
                edge.edge_type.as_str(),
                "schema-field" | "schema-record" | "protein-member"
            )
        }) {
            return Ok(None);
        }
        let sources = edges
            .edges
            .iter()
            .filter(|edge| matches!(edge.edge_type.as_str(), "schema-field" | "schema-record"));
        let resolved = futures::future::try_join_all(sources.map(|edge| async move {
            if edge.edge_type == "schema-record" {
                self.resolve_record_source(&edge.source, molecule).await
            } else {
                Ok(self
                    .resolve_path_source(&edge.source, molecule)
                    .await?
                    .map(|binding| vec![binding]))
            }
        }))
        .await?;
        for resolved in resolved {
            let Some(resolved) = resolved else {
                return Ok(None);
            };
            for binding in resolved {
                if !bindings.contains(&binding) {
                    bindings.push(binding);
                    if bindings.len() > 10_000 {
                        return Ok(None);
                    }
                }
            }
        }
        Ok((!bindings.is_empty()).then_some(bindings))
    }

    async fn resolve_record_source(
        &self,
        source: &str,
        molecule: &str,
    ) -> Result<Option<Vec<Binding>>, SchemaError> {
        let Some(name) = source
            .strip_prefix("schema:")
            .and_then(|source| source.strip_suffix(":record"))
        else {
            return Ok(None);
        };
        let snapshot = self.db_ops.schemas().read_snapshot(name).await?;
        let Some(schema) = snapshot
            .schema
            .filter(|schema| schema.molecule_uuid.as_deref() == Some(molecule))
        else {
            return Ok(None);
        };
        let bindings = schema_bindings(&schema, molecule)?;
        Ok((!bindings.is_empty()).then_some(bindings))
    }

    async fn resolve_path_source(
        &self,
        source: &str,
        molecule: &str,
    ) -> Result<Option<Binding>, SchemaError> {
        let Some(source) = source.strip_prefix("schema:") else {
            return Ok(None);
        };
        let candidates: Vec<_> = source
            .match_indices(":field:")
            .map(|(at, _)| (&source[..at], &source[at + 7..]))
            .collect();
        if candidates.len() > 64 {
            return Ok(None);
        }
        let values = futures::future::try_join_all(candidates.into_iter().map(
            |(schema, field)| async move {
                let snapshot = self.db_ops.schemas().read_snapshot(schema).await?;
                let Some(schema) = snapshot.schema else {
                    return Ok::<_, SchemaError>(None);
                };
                Ok(schema_bindings(&schema, molecule)?
                    .into_iter()
                    .find(|binding| binding.name == field))
            },
        ))
        .await?;
        let mut found = values.into_iter().flatten();
        let Some(binding) = found.next() else {
            return Ok(None);
        };
        Ok(found.next().is_none().then_some(binding))
    }
}

fn schema_bindings(
    schema: &crate::schema::types::Schema,
    molecule: &str,
) -> Result<Vec<Binding>, SchemaError> {
    let envelope = schema.molecule_uuid.as_deref() == Some(molecule);
    if schema.runtime_fields.len() > 10_000 {
        return Ok(Vec::new());
    }
    let mut bindings = Vec::new();
    for (name, field) in &schema.runtime_fields {
        if crate::record_molecule::is_record_molecule_field(name) {
            continue;
        }
        if !envelope && field.common().molecule_uuid().map(String::as_str) != Some(molecule) {
            continue;
        }
        let Some(identity) = schema.field_hashes.get(name).filter(|id| !id.is_empty()) else {
            return Ok(Vec::new());
        };
        bindings.push(Binding {
            schema: schema_identity(schema)?,
            field: identity.clone(),
            name: name.clone(),
            envelope,
        });
    }
    Ok(bindings)
}

fn changed_tips(job: &DeferredPersistJob) -> Vec<ChangedTip> {
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
