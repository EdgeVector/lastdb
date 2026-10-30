//! Typed positive effects share the exact immutable Atom values.

use super::*;

impl MutationManager {
    pub(super) async fn stage_native_controls(
        &self,
        runtime: &PackRuntime,
        scope: Option<&str>,
        molecules: &[String],
    ) -> Result<(), SchemaError> {
        tokio::try_join!(
            self.db_ops
                .atoms()
                .stage_current_molecule_selectors(runtime, molecules, scope)
                .boxed(),
            async {
                self.db_ops
                    .molecule_keys()
                    .stage_current_metadata(runtime, molecules)
                    .await?;
                Ok::<_, SchemaError>(())
            }
            .boxed(),
            async {
                self.db_ops
                    .stage_current_conflict_control(runtime, scope)
                    .await?;
                Ok::<_, SchemaError>(())
            }
            .boxed(),
        )?;
        Ok(())
    }

    pub(super) fn stage_native_headers(
        &self,
        job: &DeferredPersistJob,
        runtime: &PackRuntime,
        scope: Option<&str>,
    ) -> Result<(), SchemaError> {
        for name in job.modified_fields.keys() {
            let Some(field) = job.schema.runtime_fields.get(name) else {
                continue;
            };
            let Some(molecule) = field.common().molecule_uuid() else {
                continue;
            };
            let Some(data) = field.molecule_data() else {
                continue;
            };
            self.db_ops
                .atoms()
                .stage_molecule_header(runtime, molecule, data, scope)?;
        }
        Ok(())
    }

    pub(super) async fn native_path_bindings(
        &self,
        job: &DeferredPersistJob,
        runtime: &PackRuntime,
        tips: &[ChangedTip],
    ) -> Result<BTreeMap<String, Vec<Binding>>, SchemaError> {
        let molecules: std::collections::BTreeSet<_> =
            tips.iter().map(|tip| tip.molecule.clone()).collect();
        self.path_bindings_batch(&job.schema, &molecules)
            .await?
            .into_iter()
            .map(|(molecule, binding)| {
                let binding = if let Some(binding) = binding {
                    binding
                } else {
                    transaction::require_durable(runtime)?;
                    bindings::schema_bindings(&job.schema, &molecule)?
                };
                Ok((molecule, binding))
            })
            .collect()
    }

    pub(super) fn native_path_records(
        &self,
        scope: Option<&str>,
        tips: &[ChangedTip],
        atoms: &BTreeMap<String, CertifiedAtom>,
        bindings: &BTreeMap<String, Vec<Binding>>,
    ) -> Result<Option<Vec<LogicalPathRecord>>, SchemaError> {
        let mut records = Vec::new();
        for tip in tips {
            let atom_key = self
                .db_ops
                .atoms()
                .body_storage_key(&tip.entry.atom_uuid, scope);
            let certified = &atoms[&atom_key];
            let atom = &certified.atom;
            let sources = self.path_sources(tip, scope, &certified.key)?;
            for binding in &bindings[&tip.molecule] {
                let record = LogicalPathRecord::from_atom(
                    LogicalPath {
                        scope: scope.map(str::to_owned),
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
                    return Ok(None);
                }
                records.push(record);
            }
        }
        Ok(Some(records))
    }
}
