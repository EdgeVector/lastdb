use chrono::{DateTime, Utc};
use std::sync::Arc;

use crate::atom::AtomEntry;
use crate::db_operations::DbOperations;
use crate::schema::types::SchemaError;

use super::{FieldKind, FieldVariant};

impl FieldVariant {
    /// Rewinds the in-memory molecule to its state at `as_of` by walking each
    /// tip's version chain (`prev_tip_id` → `tv:{id}`).
    ///
    /// Falls back to legacy `history:` MutationEvents when:
    /// - there are no tip heads to walk, or
    /// - heads exist but none link a tip-version chain (`prev_tip_id` empty)
    ///   **and** MutationEvents are present for this molecule (legacy rows /
    ///   pre-tip-chain data). Depth-1 thin tips without history still use the
    ///   tip path so `written_at` alone can answer "before first write".
    pub(super) async fn rewind_to(
        &mut self,
        db_ops: &Arc<DbOperations>,
        as_of: DateTime<Utc>,
    ) -> Result<(), SchemaError> {
        let as_of_nanos = as_of.timestamp_nanos_opt().unwrap_or(0).max(0) as u64;
        let prefix = self.common().storage_prefix().map(str::to_string);

        // Snapshot heads first so we can mutate the molecule while walking.
        let heads: Vec<(String, String, AtomEntry)> = match &self.molecule {
            Some(mol) => mol
                .per_key_records()
                .into_iter()
                .map(|(h, r, e, _)| (h, r, e))
                .collect(),
            None => return Ok(()),
        };

        if heads.is_empty() {
            return self.rewind_via_legacy_history(db_ops, as_of).await;
        }

        // Depth-1 heads (no `prev_tip_id`) cannot recover prior atom values via
        // the tip walk. Prefer MutationEvent history when it exists so legacy
        // molecules (and tests that seed `history:`) still rewind correctly.
        let has_tip_chain = heads.iter().any(|(_, _, e)| !e.prev_tip_id.is_empty());
        if !has_tip_chain {
            if let Some(mol_uuid) = self.common().molecule_uuid() {
                let events = db_ops
                    .atoms()
                    .get_mutation_events(mol_uuid, prefix.as_deref())
                    .await?;
                if events
                    .iter()
                    .any(|event| event.kind == crate::atom::MutationEventKind::Transition)
                {
                    return self.rewind_via_legacy_history(db_ops, as_of).await;
                }
            }
        }

        let mut resolved_slots: Vec<(String, String, Option<AtomEntry>)> =
            Vec::with_capacity(heads.len());
        for (hash, range, head) in heads {
            let resolved = db_ops
                .atoms()
                .tip_entry_at_as_of(&head, as_of_nanos, prefix.as_deref())
                .await?;
            resolved_slots.push((hash, range, resolved));
        }

        for (hash, range, resolved) in resolved_slots {
            match (&mut self.molecule, resolved) {
                (Some(mol), Some(entry)) => {
                    mol.set_atom_uuid_from_values_unsigned(hash, range, entry.atom_uuid);
                }
                (Some(mol), None) => {
                    mol.remove_atom_uuid(&hash, &range);
                }
                _ => {}
            }
        }

        Ok(())
    }

    async fn rewind_via_legacy_history(
        &mut self,
        db_ops: &Arc<DbOperations>,
        as_of: DateTime<Utc>,
    ) -> Result<(), SchemaError> {
        use crate::atom::MutationEvent;

        let mol_uuid = match self.common().molecule_uuid() {
            Some(uuid) => uuid.clone(),
            None => return Ok(()),
        };

        let events = db_ops
            .atoms()
            .get_mutation_events(&mol_uuid, self.common().storage_prefix())
            .await
            .map_err(|e| SchemaError::InvalidData(format!("Failed to load history: {e}")))?;

        let mut events_to_undo: Vec<&MutationEvent> = events
            .iter()
            .filter(|e| e.kind == crate::atom::MutationEventKind::Transition && e.timestamp > as_of)
            .collect();
        events_to_undo.sort_by_key(|e| std::cmp::Reverse(e.timestamp));

        for event in events_to_undo {
            let fk = &event.field_key;
            match (&self.kind, &mut self.molecule) {
                (FieldKind::Single, mol_slot @ Some(_)) if fk.is_single() => {
                    match &event.old_atom_uuid {
                        Some(old) => {
                            if let Some(mol) = mol_slot {
                                mol.set_atom_uuid_from_values_unsigned(
                                    String::new(),
                                    String::new(),
                                    old.clone(),
                                );
                            }
                        }
                        None => {
                            *mol_slot = None;
                        }
                    }
                }
                (FieldKind::Hash, Some(mol)) => {
                    let Some(hash) = fk.hash.as_ref() else {
                        continue;
                    };
                    let range = fk.range.as_deref().unwrap_or("");
                    match &event.old_atom_uuid {
                        Some(old) => mol.set_atom_uuid_from_values_unsigned(
                            hash.clone(),
                            range.to_string(),
                            old.clone(),
                        ),
                        None => {
                            mol.remove_atom_uuid(hash, range);
                        }
                    }
                }
                (FieldKind::Range, Some(mol)) => {
                    let Some(range) = fk.range.as_ref() else {
                        continue;
                    };
                    let hash = fk.hash.as_deref().unwrap_or("");
                    match &event.old_atom_uuid {
                        Some(old) => mol.set_atom_uuid_from_values_unsigned(
                            hash.to_string(),
                            range.clone(),
                            old.clone(),
                        ),
                        None => {
                            mol.remove_atom_uuid(hash, range);
                        }
                    }
                }
                (FieldKind::HashRange, Some(mol)) => {
                    let (Some(hash), Some(range)) = (fk.hash.as_ref(), fk.range.as_ref()) else {
                        continue;
                    };
                    match &event.old_atom_uuid {
                        Some(old) => {
                            mol.set_atom_uuid_from_values_unsigned(
                                hash.clone(),
                                range.clone(),
                                old.clone(),
                            );
                        }
                        None => {
                            mol.remove_atom_uuid(hash, range);
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }
}
