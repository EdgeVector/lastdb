//! An absent model entry requires a complete post-write winner read.

use super::*;
use crate::schema::types::Schema;
use crate::storage::disk_pack::logical_paths::LogicalPathAbsence;

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) async fn stage_purge_core_paths(
        &self,
        job: &super::super::super::molecules::PurgeEnvelope,
        runtime: &PackRuntime,
    ) -> Result<(), SchemaError> {
        let Some(name) = job
            .erasures
            .first()
            .map(|mutation| mutation.schema_name.as_str())
        else {
            return Ok(());
        };
        let Some(schema) = self.schema_manager.get_schema_metadata(name)? else {
            return Ok(transaction::require_durable(runtime)?);
        };
        let mut slots = Vec::new();
        for mutation in &job.erasures {
            for field in schema.runtime_fields.values() {
                let Some(molecule) = field.common().molecule_uuid() else {
                    continue;
                };
                let Some((hash, range)) = field.disk_slot_for_key(&mutation.key_value) else {
                    continue;
                };
                slots.push((molecule.clone(), hash, range));
            }
        }
        let molecules: Vec<_> = slots
            .iter()
            .map(|(molecule, _, _)| molecule.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        self.stage_removed_controls(runtime, job.storage_prefix.as_deref(), &molecules)
            .boxed()
            .await?;
        self.stage_removed_core_paths(&schema, runtime, job.storage_prefix.as_deref(), slots)
            .await
    }

    async fn stage_removed_controls(
        &self,
        runtime: &PackRuntime,
        scope: Option<&str>,
        molecules: &[String],
    ) -> Result<(), SchemaError> {
        tokio::try_join!(
            self.db_ops
                .atoms()
                .stage_current_molecule_headers(runtime, molecules, scope)
                .boxed(),
            self.stage_native_controls(runtime, scope, molecules)
                .boxed(),
        )?;
        Ok(())
    }

    pub(super) async fn stage_removed_job_paths(
        &self,
        job: &DeferredPersistJob,
        runtime: &PackRuntime,
        scope: Option<&str>,
        include_siblings: bool,
    ) -> Result<(), SchemaError> {
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
        let mut slots = Vec::new();
        if include_siblings {
            for movement in &job.sibling_moves {
                slots.push((
                    movement.molecule_uuid.clone(),
                    movement.old_key.hash.clone().unwrap_or_default(),
                    movement.old_key.range.clone().unwrap_or_default(),
                ));
            }
        }
        for (molecule, data, keys) in entry.chain(siblings) {
            for key in keys {
                let (hash, range) = (key.disk_hash(), key.disk_range());
                if data.get_atom_entry(hash, range).is_none() {
                    slots.push((molecule.to_owned(), hash.to_owned(), range.to_owned()));
                }
            }
        }
        self.stage_removed_core_paths(&job.schema, runtime, scope, slots)
            .await
    }

    pub(in crate::fold_db_core::mutation_manager) async fn stage_removed_core_paths(
        &self,
        schema: &Schema,
        runtime: &PackRuntime,
        scope: Option<&str>,
        mut slots: Vec<(String, String, String)>,
    ) -> Result<(), SchemaError> {
        slots.sort();
        slots.dedup();
        if slots.len() > 10_000 {
            return Ok(transaction::require_durable(runtime)?);
        }
        let groups = slots
            .iter()
            .map(|(molecule, hash, range)| {
                self.db_ops
                    .atoms()
                    .key_codec_for_molecule(molecule)
                    .api_hash_range_record_keys_for_read(molecule, hash, range)
                    .map_err(|error| SchemaError::InvalidData(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let winners = self
            .db_ops
            .atoms()
            .load_first_present_candidate(scope, &groups)
            .await?;
        if winners.len() != slots.len() {
            return Err(SchemaError::StorageRead {
                detail: "native absence winner count differs".into(),
            });
        }
        let molecules: std::collections::BTreeSet<_> = slots
            .iter()
            .zip(&winners)
            .filter(|(_, winner)| winner.is_none())
            .map(|((molecule, _, _), _)| molecule.clone())
            .collect();
        let bindings = self
            .removed_path_bindings(schema, scope, &molecules)
            .await?;
        let mut absent = Vec::new();
        for (((molecule, hash, range), candidates), winner) in
            slots.into_iter().zip(groups).zip(winners)
        {
            if winner.is_some() {
                continue;
            }
            let sources: Vec<_> = candidates
                .iter()
                .map(|key| crate::schema::types::field::build_storage_key(scope, key))
                .collect();
            for binding in &bindings[&molecule] {
                absent.push(LogicalPathAbsence::new(
                    LogicalPath {
                        scope: scope.map(str::to_owned),
                        schema: binding.schema.clone(),
                        field: binding.field.clone(),
                        molecule: molecule.clone(),
                        hash: hash.clone(),
                        range: range.clone(),
                        atom: String::new(),
                    },
                    sources.clone(),
                ));
            }
        }
        match transaction::stage_absences(runtime, absent) {
            Err(crate::storage::StorageError::WriteResourceLimit { .. }) => {
                Ok(transaction::require_durable(runtime)?)
            }
            result => Ok(result?),
        }
    }

    async fn removed_path_bindings(
        &self,
        schema: &Schema,
        scope: Option<&str>,
        molecules: &std::collections::BTreeSet<String>,
    ) -> Result<BTreeMap<String, Vec<Binding>>, SchemaError> {
        let bindings = futures::future::try_join_all(molecules.iter().map(|molecule| async move {
            let bindings = self
                .path_bindings(schema, scope, molecule)
                .await?
                .unwrap_or(bindings::schema_bindings(schema, molecule)?);
            Ok::<_, SchemaError>((molecule.clone(), bindings))
        }))
        .await?;
        Ok(bindings.into_iter().collect())
    }
}
