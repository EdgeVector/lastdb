//! Collect every field's selected atom before one body read per storage scope.

use super::*;
use crate::storage::disk_pack::logical_paths::field::{self, FieldScope};

pub(crate) struct FieldAtomBatch {
    pub binding: FieldScope,
    pub molecule: Option<String>,
    pub name: String,
    pub matches: Vec<KeyedAtomMatch>,
}

pub(crate) async fn fetch_field_atom_batches(
    db_ops: &Arc<DbOperations>,
    schema: &str,
    prefix: Option<&str>,
    batches: &[FieldAtomBatch],
) -> Result<Vec<HashMap<KeyValue, FieldValue>>, SchemaError> {
    let runtime = db_ops.namespaced_store().disk_pack_runtime();
    let mut values = vec![HashMap::new(); batches.len()];
    let mut misses = Vec::new();
    for (index, batch) in batches.iter().enumerate() {
        batch
            .binding
            .run(async {
                for entry in &batch.matches {
                    if let Some(value) = field::lookup(
                        runtime.clone(),
                        prefix,
                        batch.molecule.as_deref(),
                        &field::AtomSelection {
                            entry,
                            primary: db_ops.atoms().primary_atom_key(
                                &entry.1,
                                entry.4.as_ref(),
                                prefix,
                            ),
                        },
                    )? {
                        values[index].insert(entry.0.clone(), value);
                    } else {
                        misses.push((index, entry));
                    }
                }
                Ok::<_, SchemaError>(())
            })
            .await?;
    }
    let slots: Vec<_> = misses
        .iter()
        .map(|(_, entry)| (entry.1.as_str(), entry.4.clone()))
        .collect();
    let atoms = crate::db_operations::tip_atoms::get_atoms_for_tips(db_ops, &slots, prefix).await?;
    for ((index, entry), atom) in misses.into_iter().zip(atoms) {
        let batch = &batches[index];
        let Some(atom) = atom else {
            handle_unresolved_atom(
                db_ops,
                prefix,
                batch.molecule.as_deref(),
                Some(schema),
                Some(&batch.name),
                &UnresolvedMatch {
                    key: &entry.0,
                    atom_uuid: &entry.1,
                    partition: entry.4.as_ref(),
                },
            );
            continue;
        };
        let value = field_value_from_atom(&atom, entry.1.clone(), entry.2.clone(), entry.3.clone());
        batch
            .binding
            .run(async {
                field::admit(
                    runtime.clone(),
                    prefix,
                    batch.molecule.as_deref(),
                    &entry.0,
                    &value,
                    entry.4.as_ref(),
                )
            })
            .await?;
        values[index].insert(entry.0.clone(), value);
    }
    Ok(values)
}
