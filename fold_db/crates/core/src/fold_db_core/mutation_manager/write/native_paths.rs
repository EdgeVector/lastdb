//! Canonical write paths come from exact schema-field bindings, never raw keys.

use super::super::molecules::DeferredPersistJob;
use super::*;
use crate::storage::disk_pack::logical_paths::{schema_identity, LogicalPath, LogicalPathRecord};
use crate::storage::disk_pack::{transaction, PackRuntime};
use std::collections::BTreeMap;

struct ChangedTip {
    molecule: String,
    hash: String,
    range: String,
    atom: String,
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
        let atoms: Vec<_> = tips.iter().map(|tip| (tip.atom.as_str(), None)).collect();
        let atoms = self
            .db_ops
            .atoms()
            .get_atoms_located(&atoms, job.storage_prefix.as_deref())
            .await?;
        let mut bytes = 0usize;
        let mut records = Vec::new();
        for (tip, atom) in tips.iter().zip(atoms) {
            let atom = atom.ok_or_else(|| SchemaError::StorageRead {
                detail: "native write path has no staged or stored atom content".into(),
            })?;
            for binding in bindings[&tip.molecule]
                .as_ref()
                .expect("complete path bindings")
            {
                let value = if binding.envelope {
                    crate::record_molecule::parse_document_fields(atom.content())
                        .and_then(|fields| fields.get(&binding.name))
                } else {
                    Some(atom.content())
                };
                let Some(value) = value else { continue };
                let record = LogicalPathRecord::from_value(
                    LogicalPath {
                        scope: job.storage_prefix.clone(),
                        schema: binding.schema.clone(),
                        field: binding.field.clone(),
                        molecule: tip.molecule.clone(),
                        hash: tip.hash.clone(),
                        range: tip.range.clone(),
                        atom: tip.atom.clone(),
                    },
                    value,
                )?;
                bytes = bytes.saturating_add(record.body.len());
                if bytes > 128 * 1024 * 1024 || records.len() >= 10_000 {
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
            if let Some(atom) = data.get_atom_uuid(hash, range) {
                tips.insert(
                    (molecule.to_string(), hash.to_string(), range.to_string()),
                    atom.clone(),
                );
            }
        }
    }
    tips.into_iter()
        .map(|((molecule, hash, range), atom)| ChangedTip {
            molecule,
            hash,
            range,
            atom,
        })
        .collect()
}
