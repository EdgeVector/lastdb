//! Resolve complete field paths before a predicate or projection discards them.

use super::*;
use crate::schema::types::field::filter_utils::{fetch_field_atom_batches, FieldAtomBatch};
use crate::storage::disk_pack::logical_paths::field::FieldScope;

impl HashRangeQueryProcessor {
    pub(super) async fn resolve_pending_values(
        &self,
        schema: &Schema,
        pending: &[SecondaryPending],
    ) -> Result<Vec<Option<FieldValue>>, SchemaError> {
        let mut scopes: HashMap<KeySource, HashMap<String, Vec<usize>>> = HashMap::new();
        for (slot, entry) in pending.iter().enumerate() {
            scopes
                .entry(entry.5.clone())
                .or_default()
                .entry(entry.0.clone())
                .or_default()
                .push(slot);
        }
        let mut out = vec![None; pending.len()];
        for (prefix, fields) in scopes {
            let mut batches = Vec::with_capacity(fields.len());
            let mut batch_slots = Vec::with_capacity(fields.len());
            for (name, slots) in fields {
                let field = schema.runtime_fields.get(&name).ok_or_else(|| {
                    SchemaError::InvalidField(format!(
                        "resolved field '{name}' has no captured schema"
                    ))
                })?;
                let binding = FieldScope::new(schema, &name)?;
                let matches = slots
                    .iter()
                    .map(|slot| {
                        let entry = &pending[*slot];
                        (
                            entry.1.clone(),
                            entry.2.clone(),
                            entry.3.clone(),
                            entry.4.clone(),
                        )
                    })
                    .collect();
                batches.push(FieldAtomBatch {
                    binding,
                    molecule: field.common().molecule_uuid().cloned(),
                    name,
                    matches,
                });
                batch_slots.push(slots);
            }
            let values =
                fetch_field_atom_batches(&self.db_ops, &schema.name, prefix.as_deref(), &batches)
                    .await?;
            for ((batch, slots), mut values) in batches.into_iter().zip(batch_slots).zip(values) {
                schema.runtime_fields[&batch.name].stamp_molecule_meta(&mut values);
                for slot in slots {
                    out[slot] = values.get(&pending[slot].1).cloned();
                }
            }
        }
        Ok(out)
    }
}
