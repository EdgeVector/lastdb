//! Molecule restoration before applying a mutation batch.

use std::collections::{HashMap, HashSet};

use crate::db_operations::ChangedKey;
use crate::schema::types::Schema;
use crate::schema::SchemaError;

use super::super::plan::molecules::{cold_field_loads, FieldLoadPlan};
use super::super::MutationManager;

impl MutationManager {
    /// Discard a prior preparation before resolving its current native keys.
    pub(in crate::fold_db_core::mutation_manager) async fn reload_changed_for_prepare(
        &self,
        schema: &mut Schema,
        changed_keys: &HashMap<String, HashSet<ChangedKey>>,
    ) -> Result<(), SchemaError> {
        for name in changed_keys.keys() {
            if let Some(field) = schema.runtime_fields.get_mut(name) {
                field.molecule = None;
            }
        }
        self.restore_missing_molecules(schema, changed_keys).await?;
        Ok(())
    }

    pub(in crate::fold_db_core::mutation_manager) async fn restore_missing_molecules(
        &self,
        schema: &mut Schema,
        changed_keys: &HashMap<String, HashSet<ChangedKey>>,
    ) -> Result<(), SchemaError> {
        // Fetch only the fields and keys this request changes. The native
        // captured view includes accepted logical writes and the current disk root.
        let plans = cold_field_loads(schema, changed_keys);
        if plans.is_empty() {
            return Ok(());
        }

        let fields_without_headers = self.load_fields_for_write(schema, plans).await?;
        if !fields_without_headers.is_empty() {
            self.load_fields_full(schema, fields_without_headers)
                .await?;
        }
        Ok(())
    }

    /// Fetch every changed field in bounded exact-key batches. A missing
    /// exact-scope header still requires the existing full load fallback.
    async fn load_fields_for_write(
        &self,
        schema: &mut Schema,
        plans: Vec<FieldLoadPlan<'_>>,
    ) -> Result<Vec<String>, SchemaError> {
        let requests: Vec<_> = plans
            .iter()
            .map(|plan| crate::db_operations::atom_store::MoleculeWriteLoad {
                molecule: &plan.molecule_uuid,
                scope: plan.storage_prefix.as_deref(),
                changed: plan.changed_keys,
            })
            .collect();
        let store_loads = self
            .db_ops
            .atoms()
            .load_molecules_for_write(&requests)
            .await?;

        let mut fields_without_headers = Vec::new();
        for (plan, data) in plans.into_iter().zip(store_loads) {
            let field_name = plan.field_name;
            if let Some(data) = data {
                if let Some(field) = schema.runtime_fields.get_mut(&field_name) {
                    field.set_molecule_data(data);
                }
            } else {
                // No per-key header — fall through to the full load path below.
                fields_without_headers.push(field_name);
            }
        }

        Ok(fields_without_headers)
    }

    /// Full-load the fields that have no per-key header and set their molecules.
    async fn load_fields_full(
        &self,
        schema: &mut Schema,
        fields_without_headers: Vec<String>,
    ) -> Result<(), SchemaError> {
        let full_loads =
            futures::future::try_join_all(fields_without_headers.into_iter().map(|field_name| {
                let db_ops = self.db_ops.clone();
                let plan = schema.runtime_fields.get(&field_name).and_then(|field| {
                    let uuid = field.common().molecule_uuid()?.clone();
                    let prefix = field.common().storage_prefix().map(str::to_string);
                    Some((uuid, prefix))
                });
                async move {
                    let Some((uuid, prefix)) = plan else {
                        return Ok((field_name, None));
                    };
                    let data = db_ops
                        .atoms()
                        .load_molecule_per_key(&uuid, prefix.as_deref())
                        .await?;
                    Ok::<_, SchemaError>((field_name, data))
                }
            }))
            .await?;

        for (field_name, data) in full_loads {
            let Some(data) = data else {
                continue;
            };
            if let Some(field) = schema.runtime_fields.get_mut(&field_name) {
                field.set_molecule_data(data);
            }
        }
        Ok(())
    }
}
