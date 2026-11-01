//! Peer import of a format 7 home: version 2 record outcomes merge field by
//! field into the stored records, in the native request of the import.

mod admission;
mod effects;
mod merge;
mod publication;

use super::*;
use crate::db_operations::atom_store::blob_counter::BlobDeltas;
use crate::db_operations::atom_store::record_scope;
use crate::schema::types::{KeyValue, Schema};
use merge::{PageMerge, PeerRecord};
use std::collections::HashSet;

impl MutationManager {
    /// Merge the record outcomes of the admitted page. One read by key of the
    /// records and one of the conflict rows and markers, then the records of
    /// each schema with their alternate entries, field history and retention
    /// rows, all in the native request of the import.
    pub(super) async fn stage_peer_records(
        &self,
        events: &[CaptureEvent],
        schemas: &BTreeMap<String, Schema>,
        runtime: &Arc<PackRuntime>,
    ) -> Result<(), SchemaError> {
        let atoms = self.db_ops.atoms();
        let (keys, placed) = page_keys(atoms, events, schemas)?;
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
        let mut conflicts = atoms.record_conflict_mutations(lost).await?;
        for schema in schemas.values() {
            let mine: Vec<&PeerRecord> = changed
                .iter()
                .filter(|record| record.schema == schema.name)
                .collect();
            if !mine.is_empty() {
                let entries = std::mem::take(&mut conflicts);
                Box::pin(self.stage_peer_schema_records(schema, &mine, entries)).await?;
            }
        }
        if !conflicts.is_empty() {
            // Each losing value kept the stored record: only the rows change.
            atoms
                .stage_records(Vec::new(), conflicts, BlobDeltas::default())
                .await?;
        }
        self.stage_peer_record_notifications(&changed, schemas, runtime)
            .await
    }
}

/// The storage key, schema and key of each distinct record of a page, in the
/// order of its first outcome, and the storage key of each outcome.
type PageKeys = (Vec<(String, String, KeyValue)>, Vec<String>);

fn page_keys(
    atoms: &crate::db_operations::atom_store::AtomStore,
    events: &[CaptureEvent],
    schemas: &BTreeMap<String, Schema>,
) -> Result<PageKeys, SchemaError> {
    let mut keys = Vec::new();
    let mut placed = Vec::new();
    let mut seen = HashSet::new();
    for outcome in events.iter().flat_map(|event| &event.records) {
        let schema = schemas
            .get(&outcome.schema_name)
            .ok_or_else(|| unsupported("a record outcome of an unadmitted schema"))?;
        let record = atoms.record_key(schema, &outcome.key, record_scope(schema).as_deref())?;
        if seen.insert(record.clone()) {
            keys.push((record.clone(), schema.name.clone(), outcome.key.clone()));
        }
        placed.push(record);
    }
    Ok((keys, placed))
}
