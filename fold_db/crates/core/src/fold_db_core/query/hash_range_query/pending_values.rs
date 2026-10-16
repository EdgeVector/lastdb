//! Resolve complete field paths before a predicate or projection discards them.

use super::*;
use crate::schema::types::field::fetch_atoms_with_key_metadata_async_with_prefix;
use crate::storage::disk_pack::logical_paths::field::FieldScope;

impl HashRangeQueryProcessor {
    pub(super) async fn resolve_pending_values(
        &self,
        schema: &Schema,
        pending: &[SecondaryPending],
    ) -> Result<Vec<Option<FieldValue>>, SchemaError> {
        let mut batches: HashMap<(String, KeySource), Vec<usize>> = HashMap::new();
        for (slot, entry) in pending.iter().enumerate() {
            batches
                .entry((entry.0.clone(), entry.5.clone()))
                .or_default()
                .push(slot);
        }
        let mut out = vec![None; pending.len()];
        for ((name, prefix), slots) in batches {
            let field = schema.runtime_fields.get(&name).ok_or_else(|| {
                SchemaError::InvalidField(format!("resolved field '{name}' has no captured schema"))
            })?;
            let binding = FieldScope::for_runtime(
                self.db_ops.namespaced_store().disk_pack_runtime(),
                schema,
                &name,
            )?;
            let matches = slots.iter().map(|slot| {
                let entry = &pending[*slot];
                (
                    entry.1.clone(),
                    entry.2.clone(),
                    entry.3.clone(),
                    entry.4.clone(),
                    entry.6.clone(),
                )
            });
            let mut values = binding
                .run(fetch_atoms_with_key_metadata_async_with_prefix(
                    &self.db_ops,
                    matches,
                    prefix.as_deref(),
                    field.common().molecule_uuid().map(String::as_str),
                    Some(&schema.name),
                    Some(&name),
                ))
                .await?;
            field.stamp_molecule_meta(&mut values);
            for slot in slots {
                out[slot] = values.get(&pending[slot].1).cloned();
            }
        }
        Ok(out)
    }
}
