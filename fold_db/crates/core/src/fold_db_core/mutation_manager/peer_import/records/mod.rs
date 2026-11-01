//! Peer import of a format 7 home: version 2 record outcomes merge field by
//! field into the stored records, in the native request of the import.

mod admission;
mod merge;
mod publication;

use super::*;
use crate::db_operations::atom_store::record_scope;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::Schema;
use merge::PageMerge;
use std::collections::HashSet;

impl MutationManager {
    /// Merge the record outcomes of the admitted page. One read by key of the
    /// records, one read by key of the conflict rows and markers, and one
    /// batch for the records, their alternate entries, the conflict rows and
    /// the file blob counters.
    pub(super) async fn stage_peer_records(
        &self,
        events: &[CaptureEvent],
        schemas: &BTreeMap<String, Schema>,
        runtime: &Arc<PackRuntime>,
    ) -> Result<(), SchemaError> {
        let atoms = self.db_ops.atoms();
        let mut keys = Vec::new();
        let mut placed = Vec::new();
        let mut seen = HashSet::new();
        for event in events {
            for outcome in &event.records {
                let schema = &schemas[&outcome.schema_name];
                let record =
                    atoms.record_key(schema, &outcome.key, record_scope(schema).as_deref())?;
                if seen.insert(record.clone()) {
                    keys.push((record.clone(), schema.name.clone(), outcome.key.clone()));
                }
                placed.push(record);
            }
        }
        if keys.is_empty() {
            return Ok(());
        }
        let storage_keys: Vec<String> = keys.iter().map(|(record, ..)| record.clone()).collect();
        let stored = atoms.read_records(&storage_keys).await?;
        let mut page = PageMerge::new(keys, stored);
        let outcomes = events.iter().flat_map(|event| &event.records);
        for (record, outcome) in placed.iter().zip(outcomes) {
            page.apply(record, outcome);
        }
        let (changed, lost) = page.finish();
        if changed.is_empty() && lost.is_empty() {
            return Ok(());
        }
        let mut batch = alternate_changes(atoms, schemas, &changed)?;
        batch.extend(atoms.record_conflict_mutations(lost).await?);
        atoms
            .stage_records(
                changed.iter().map(merge::PeerRecord::change).collect(),
                batch,
            )
            .await?;
        for schema in schemas.values() {
            if schema.schema_type != DeclarativeSchemaType::HashRange {
                continue;
            }
            let keys: Vec<_> = changed
                .iter()
                .filter(|record| record.schema == schema.name)
                .map(|record| record.key.clone())
                .collect();
            if !keys.is_empty() {
                self.db_ops
                    .schemas()
                    .record_schema_retention_hash_partitions(&schema.name, &keys)
                    .await?;
            }
        }
        self.stage_peer_record_notifications(&changed, schemas, runtime)
            .await
    }
}

/// The alternate entry changes of the merged records of each schema.
fn alternate_changes(
    atoms: &crate::db_operations::atom_store::AtomStore,
    schemas: &BTreeMap<String, Schema>,
    changed: &[merge::PeerRecord],
) -> Result<Vec<crate::storage::KvMutation>, SchemaError> {
    let mut batch = Vec::new();
    for schema in schemas.values() {
        let mine: Vec<_> = changed
            .iter()
            .filter(|record| record.schema == schema.name)
            .collect();
        if mine.is_empty() {
            continue;
        }
        let keys: Vec<_> = mine.iter().map(|record| record.key.clone()).collect();
        let before: Vec<_> = mine
            .iter()
            .map(|record| {
                record
                    .before
                    .as_ref()
                    .map(|stored| stored.fields.clone())
                    .unwrap_or_default()
            })
            .collect();
        let after: Vec<_> = mine
            .iter()
            .map(|record| record.after.fields.clone())
            .collect();
        batch.extend(atoms.alternate_entry_changes(
            schema,
            &keys,
            &before,
            &after,
            record_scope(schema).as_deref(),
        )?);
    }
    Ok(batch)
}
