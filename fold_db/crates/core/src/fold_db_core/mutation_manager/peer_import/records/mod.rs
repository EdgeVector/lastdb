//! The merge of version 2 record outcomes, field by field, into the stored
//! records, in the native request of the import.

mod admission;
mod cap;
mod effects;
mod merge;
mod publication;

use super::*;
use crate::db_operations::record_store::blob_counter::BlobDeltas;
use crate::db_operations::record_store::record_scope;
use crate::schema::types::{KeyValue, Schema};
use crate::storage::disk_pack::logical_capture::SkippedPeerRecord;
use merge::{PageMerge, PeerRecord};
use std::collections::HashSet;

impl MutationManager {
    /// Merge the record outcomes of the admitted page. One read by key of the
    /// records and one of the conflict rows and markers, then the records of
    /// each schema with their alternate entries, field history and retention
    /// rows, all in the native request of the import. A merged record above
    /// the record cap keeps its stored value and comes back as skipped.
    pub(super) async fn stage_peer_records(
        &self,
        events: &[CaptureEvent],
        schemas: &BTreeMap<String, Schema>,
        runtime: &Arc<PackRuntime>,
    ) -> Result<Vec<SkippedPeerRecord>, SchemaError> {
        let records = self.db_ops.records();
        let (keys, placed) = page_keys(records, events, schemas)?;
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let storage_keys: Vec<String> = keys.iter().map(|(record, ..)| record.clone()).collect();
        let stored = records.read_records(&storage_keys).await?;
        let mut page = PageMerge::new(keys, stored);
        let outcomes = events.iter().flat_map(|event| &event.records);
        for (record, outcome) in placed.iter().zip(outcomes) {
            page.apply(record, outcome);
        }
        let (changed, lost) = page.finish();
        let (changed, lost, skipped) = cap::split_over_cap(changed, lost);
        let mut conflicts = records.record_conflict_mutations(lost).await?;
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
            records
                .stage_records(Vec::new(), conflicts, BlobDeltas::default())
                .await?;
        }
        self.stage_peer_record_notifications(&changed, schemas, runtime)
            .await?;
        Ok(skipped)
    }
}

/// The storage key, schema and key of each distinct record of a page, in the
/// order of its first outcome, and the storage key of each outcome.
type PageKeys = (Vec<(String, String, KeyValue)>, Vec<String>);

fn page_keys(
    records: &crate::db_operations::record_store::RecordStore,
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
        let record = records.record_key(schema, &outcome.key, record_scope(schema).as_deref())?;
        if seen.insert(record.clone()) {
            keys.push((record.clone(), schema.name.clone(), outcome.key.clone()));
        }
        placed.push(record);
    }
    Ok((keys, placed))
}
