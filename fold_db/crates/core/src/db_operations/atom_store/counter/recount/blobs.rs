//! The blob step of the recount: count the references of each blob, then let
//! `blob_counter::recount` compare the count with the blob counters.
//!
//! A blob has one reference for each atom counter that lists it, one for each
//! live tip that names it in its metadata, and one for each record of a format
//! 7 home that names it, in every listed scope. The walks read the atom
//! counters and the tips after the atom step, so an executed recount counts the
//! exact atom counters it just wrote.

use super::report::{RecountOptions, RecountReport};
use crate::db_operations::atom_store::blob_counter::blobs_of_tip_row;
use crate::db_operations::atom_store::counter::scan::{family_range, Family};
use crate::db_operations::atom_store::counter::AtomCounter;
use crate::db_operations::atom_store::{AtomStore, RangeWalk};
use crate::schema::types::field::{build_storage_key, FilterUtils};
use crate::schema::SchemaError;
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::StoredRecord;
use std::collections::HashMap;

/// Rows asked for in each read of the atom counters and the tips.
const PAGE_ROWS: usize = 1_000;

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
            self.tally_counter_blobs(scope.as_deref(), &mut holders, &mut unreadable, report)
                .await?;
            for family in [Family::Tips, Family::Records] {
                self.tally_tip_blobs(
                    scope.as_deref(),
                    family,
                    &mut holders,
                    &mut unreadable,
                    report,
                )
                .await?;
            }
            self.tally_record_blobs(scope.as_deref(), &mut holders, &mut unreadable, report)
                .await?;
        }
        let mut exact = options.blobs_exact;
        if exact && unreadable > 0 {
            exact = false;
            report.blobs.notes.push(format!(
                "{unreadable} atom counter(s), tip(s) or record(s) do not decode, so the blob counters were not set to the exact count"
            ));
        }
        self.recount_blob_counters(holders, exact, options.execute, &mut report.blobs)
            .await
    }

    async fn tally_counter_blobs(
        &self,
        scope: Option<&str>,
        holders: &mut HashMap<String, u64>,
        unreadable: &mut u64,
        report: &mut RecountReport,
    ) -> Result<(), SchemaError> {
        let (start, end) = family_range(Family::Atoms, scope);
        let mut walk = RangeWalk::new(&start, &end, PAGE_ROWS);
        while !walk.done() {
            let step = self.walk_step(&mut walk).await?;
            for (key, value) in &step.rows {
                if !crate::atom::atom_key_codec::is_counter_key(&String::from_utf8_lossy(key)) {
                    continue;
                }
                report.blobs.atom_counters_read += 1;
                match serde_json::from_slice::<AtomCounter>(value) {
                    Ok(counter) => {
                        for blob in counter.blobs {
                            *holders.entry(blob).or_insert(0) += 1;
                        }
                    }
                    Err(_) => *unreadable += 1,
                }
            }
        }
        Ok(())
    }

    async fn tally_tip_blobs(
        &self,
        scope: Option<&str>,
        family: Family,
        holders: &mut HashMap<String, u64>,
        unreadable: &mut u64,
        report: &mut RecountReport,
    ) -> Result<(), SchemaError> {
        let (start, end) = family_range(family, scope);
        let mut walk = RangeWalk::new(&start, &end, PAGE_ROWS);
        while !walk.done() {
            let step = self.walk_step(&mut walk).await?;
            for (key, value) in &step.rows {
                report.blobs.tips_read += 1;
                let blobs = if crate::db_operations::atom_store::atom_history::is_archive(
                    &String::from_utf8_lossy(key),
                ) {
                    crate::db_operations::atom_store::atom_history::files(value)
                } else if family == Family::Records {
                    continue;
                } else {
                    blobs_of_tip_row(value)
                };
                match blobs {
                    Ok(blobs) => {
                        for blob in blobs {
                            *holders.entry(blob).or_insert(0) += 1;
                        }
                    }
                    Err(_) => *unreadable += 1,
                }
            }
        }
        Ok(())
    }

    /// A record names its blobs once each, as the write path counts them
    /// (`AtomStore::stage_records`). A tombstone names the blobs of the
    /// metadata that outlived the delete.
    async fn tally_record_blobs(
        &self,
        scope: Option<&str>,
        holders: &mut HashMap<String, u64>,
        unreadable: &mut u64,
        report: &mut RecountReport,
    ) -> Result<(), SchemaError> {
        let start = build_storage_key(scope, RECORD_PREFIX);
        let end = FilterUtils::create_prefix_end(&start)?;
        let mut walk = RangeWalk::new(start.as_bytes(), end.as_bytes(), PAGE_ROWS);
        while !walk.done() {
            let step = self.walk_step(&mut walk).await?;
            for (_, value) in &step.rows {
                report.blobs.records_read += 1;
                match StoredRecord::decode(value) {
                    Ok(record) => {
                        for blob in record.blob_refs() {
                            *holders.entry(blob).or_insert(0) += 1;
                        }
                    }
                    Err(_) => *unreadable += 1,
                }
            }
        }
        Ok(())
    }
}
