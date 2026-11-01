//! The blob step of the recount: count the references of each blob, then let
//! `blob_counter::recount` compare the count with the blob counters.
//!
//! A blob has one reference for each record that names it, and one for each
//! field history entry whose copied value names it, in every listed scope.

use super::report::{RecountOptions, RecountReport};
use super::PAGE_ROWS;
use crate::db_operations::atom_store::{AtomStore, RangeWalk};
use crate::records::atom_history::{RecordHistoryEntry, RECORD_HISTORY_PREFIX};
use crate::schema::types::field::{build_storage_key, FilterUtils};
use crate::schema::SchemaError;
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::StoredRecord;
use std::collections::HashMap;

impl AtomStore {
    /// Count the blob references of the listed scopes and recount the blob
    /// counters against them.
    pub(in crate::db_operations::atom_store) async fn recount_blobs(
        &self,
        options: &RecountOptions,
        report: &mut RecountReport,
    ) -> Result<(), SchemaError> {
        let mut holders: HashMap<String, u64> = HashMap::new();
        let mut unreadable = 0u64;
        for scope in &options.scopes {
            self.tally_record_blobs(scope.as_deref(), &mut holders, &mut unreadable, report)
                .await?;
        }
        let mut exact = options.blobs_exact;
        if exact && unreadable > 0 {
            exact = false;
            report.blobs.notes.push(format!(
                "{unreadable} record(s) or field history entries do not decode, so the blob counters were not set to the exact count"
            ));
        }
        self.recount_blob_counters(holders, exact, options.execute, &mut report.blobs)
            .await
    }

    /// A record names its blobs once each, as the write path counts them
    /// (`AtomStore::stage_records`). A tombstone names the blobs of the
    /// metadata that outlived the delete. A field history entry names the
    /// blobs of its copied value once each, as its capture counts them.
    async fn tally_record_blobs(
        &self,
        scope: Option<&str>,
        holders: &mut HashMap<String, u64>,
        unreadable: &mut u64,
        report: &mut RecountReport,
    ) -> Result<(), SchemaError> {
        for prefix in [RECORD_PREFIX, RECORD_HISTORY_PREFIX] {
            let start = build_storage_key(scope, prefix);
            let end = FilterUtils::create_prefix_end(&start)?;
            let mut walk = RangeWalk::new(start.as_bytes(), end.as_bytes(), PAGE_ROWS);
            while !walk.done() {
                let step = self.walk_step(&mut walk).await?;
                for (_, value) in &step.rows {
                    report.blobs.records_read += 1;
                    let blobs = if prefix == RECORD_PREFIX {
                        StoredRecord::decode(value)
                            .map(|record| record.blob_refs())
                            .ok()
                    } else {
                        serde_json::from_slice::<RecordHistoryEntry>(value)
                            .map(|entry| entry.blobs())
                            .ok()
                    };
                    match blobs {
                        Some(blobs) => {
                            for blob in blobs {
                                *holders.entry(blob).or_insert(0) += 1;
                            }
                        }
                        None => *unreadable += 1,
                    }
                }
            }
        }
        Ok(())
    }
}
