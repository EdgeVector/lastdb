use super::*;
use crate::db_operations::atom_store::counter::{event_atoms, RecordRefs, TipChangeSet};
use crate::db_operations::atom_store::RecordSlot;

impl AtomStore {
    /// Keep imported Puts that lost to a Delete without changing a tip.
    /// The caller supplies stable keys so replay of one source mutation is
    /// idempotent. Each history row holds one reference to each atom it names,
    /// and the planner writes the rows, the references and the bodies of atoms
    /// that gain their first reference in one batch.
    pub(crate) async fn batch_store_suppressed_put_events(
        &self,
        events: Vec<(String, MutationEvent)>,
        bodies: &[SealedAtom],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        if events.is_empty() {
            return Ok(());
        }
        let mut rows = Vec::with_capacity(events.len());
        let mut records = Vec::with_capacity(events.len());
        let mut keys = Vec::with_capacity(events.len());
        for (key, event) in events {
            if event.kind != crate::atom::MutationEventKind::SuppressedPut {
                return Err(SchemaError::InvalidData(
                    "suppressed Put history requires a suppressed Put event".into(),
                ));
            }
            // The slot of the Put, in storage form, for the slot directory.
            let molecule = event.molecule_uuid.as_str();
            let hash =
                self.storage_hash(molecule, event.field_key.hash.as_deref().unwrap_or(""))?;
            let range =
                self.storage_range(molecule, event.field_key.range.as_deref().unwrap_or(""))?;
            let new_rows = self.retention_rows(
                storage_prefix,
                &key,
                &RecordSlot {
                    molecule,
                    hash: &hash,
                    range: &range,
                },
                None,
            )?;
            records.push(RecordRefs {
                key: key.clone(),
                atoms: event_atoms(&event),
                new_rows,
                molecule: event.molecule_uuid.clone(),
            });
            rows.push((
                key.clone(),
                serde_json::to_value(event).map_err(|error| {
                    SchemaError::InvalidData(format!("serialize suppressed Put history: {error}"))
                })?,
            ));
            keys.push(key);
        }
        // Replay calls this outside a write request. The planner needs one
        // native scope, so the history rows, the references and the bodies
        // commit as one batch. Inside an open scope the call joins that scope.
        let commit = self
            .in_write_scope(self.commit_tip_changes(TipChangeSet {
                scope: storage_prefix,
                tips: Vec::new(),
                bodies,
                body_source: None,
                records,
                releases: Vec::new(),
                rows,
                deletes: Vec::new(),
            }))
            .await?;
        self.account_stored_bodies(bodies, &commit);
        for key in keys {
            crate::atom::history_event_memo::invalidate_for_event_key(&key);
        }
        Ok(())
    }
}
