//! Resolve complete field paths before a predicate or projection discards them.

use crate::schema::types::field::filter_utils::{fetch_field_atom_batches, FieldAtomBatch};
use crate::schema::types::field::FieldValue;
use crate::schema::{Schema, SchemaError};
use crate::storage::disk_pack::logical_paths::field::FieldScope;

use super::{secondary_rules::pending_slots_by_prefix, HashRangeQueryProcessor, SecondaryPending};

impl HashRangeQueryProcessor {
    pub(super) async fn resolve_pending_values(
        &self,
        schema: &Schema,
        pending: &[SecondaryPending],
    ) -> Result<Vec<Option<FieldValue>>, SchemaError> {
        let scopes = pending_slots_by_prefix(pending);
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
                            entry.key.clone(),
                            entry.atom_uuid.clone(),
                            entry.metadata.clone(),
                            entry.writer_pubkey.clone(),
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
                fetch_field_atom_batches(&self.db_ops, prefix.as_deref(), &batches).await?;
            for ((batch, slots), mut values) in batches.into_iter().zip(batch_slots).zip(values) {
                schema.runtime_fields[&batch.name].stamp_molecule_meta(&mut values);
                // The final field map keeps the last duplicate. Move its full
                // value there instead of cloning it into every pending slot.
                for slot in slots.into_iter().rev() {
                    out[slot] = values.remove(&pending[slot].key);
                }
            }
        }
        Ok(out)
    }
}
