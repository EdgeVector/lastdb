//! Molecule restoration before applying a mutation batch.

use std::collections::{HashMap, HashSet};

use crate::db_operations::ChangedKey;
use crate::schema::types::Schema;
use crate::schema::SchemaError;

use super::super::MutationManager;

/// Field name, molecule uuid, storage prefix and the changed keys to hydrate.
type FieldLoadPlan = (String, String, Option<String>, HashSet<ChangedKey>);

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
        let plans = Self::plan_cold_field_loads(schema, changed_keys);
        let need_store = plans;
        if need_store.is_empty() {
            return Ok(());
        }

        let need_full = self.load_fields_for_write(schema, need_store).await?;
        if !need_full.is_empty() {
            self.load_fields_full(schema, need_full).await?;
        }
        Ok(())
    }

    /// Plan a cold load for each field that has a molecule uuid but no loaded
    /// molecule and is touched by this batch.
    fn plan_cold_field_loads(
        schema: &Schema,
        changed_keys: &HashMap<String, HashSet<ChangedKey>>,
    ) -> Vec<FieldLoadPlan> {
        let mut plans: Vec<FieldLoadPlan> = Vec::new();
        for (name, field) in &schema.runtime_fields {
            if field.common().molecule_uuid().is_none()
                || field.has_molecule()
                || !changed_keys.contains_key(name.as_str())
            {
                continue;
            }
            let Some(uuid) = field.common().molecule_uuid().cloned() else {
                continue;
            };
            let storage_prefix = field.common().storage_prefix().map(ToString::to_string);
            let Some(keys) = changed_keys.get(name.as_str()) else {
                continue;
            };
            if keys.is_empty() {
                continue;
            }
            plans.push((name.clone(), uuid, storage_prefix, keys.clone()));
        }
        plans
    }

    /// Load the per-key molecule headers for write concurrently; returns the
    /// fields with no per-key header, which need the full load.
    async fn load_fields_for_write(
        &self,
        schema: &mut Schema,
        need_store: Vec<FieldLoadPlan>,
    ) -> Result<Vec<String>, SchemaError> {
        let store_loads = futures::future::try_join_all(need_store.iter().map(
            |(field_name, uuid, storage_prefix, keys)| {
                let db_ops = self.db_ops.clone();
                let field_name = field_name.clone();
                let uuid = uuid.clone();
                let storage_prefix = storage_prefix.clone();
                let keys = keys.clone();
                async move {
                    let data = db_ops
                        .atoms()
                        .load_molecule_for_write(&uuid, storage_prefix.as_deref(), &keys)
                        .await?;
                    Ok::<_, SchemaError>((field_name, uuid, storage_prefix, data))
                }
            },
        ))
        .await?;

        let mut need_full: Vec<String> = Vec::new();
        for (field_name, _uuid, _prefix, data) in store_loads {
            if let Some(data) = data {
                if let Some(field) = schema.runtime_fields.get_mut(&field_name) {
                    field.set_molecule_data(data);
                }
            } else {
                // No per-key header — fall through to the full load path below.
                need_full.push(field_name);
            }
        }

        Ok(need_full)
    }

    /// Full-load the fields that have no per-key header and set their molecules.
    async fn load_fields_full(
        &self,
        schema: &mut Schema,
        need_full: Vec<String>,
    ) -> Result<(), SchemaError> {
        let full_loads = futures::future::try_join_all(need_full.iter().map(|field_name| {
            let db_ops = self.db_ops.clone();
            let field_name = field_name.clone();
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
