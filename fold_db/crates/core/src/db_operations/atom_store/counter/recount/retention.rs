//! The retention pass of the recount: records that no queue entry names, and
//! entries in a bucket that the directory does not list.
//!
//! After the counters, the recount surveys the retention queue once
//! (`time_queue/survey.rs`: the entries of every listed bucket, and of the
//! slices that hold a live key and no directory row), then walks the conflict
//! records of the scope once (not once for each shard) and compares their ids
//! with the entries. The home is stopped, so no writer moves between the two
//! reads. With `--execute` it writes the missing rows again, through the planner
//! (`retention/repair.rs`). The pass writes no counter, no body and no tip.

use super::report::RecountReport;
use super::{unsealed_refusal, TALLY_PAGE_ROWS};
use crate::db_operations::atom_store::conflict_record::is_counted_record_key;
use crate::db_operations::atom_store::counter::scan::{family_range, Family};
use crate::db_operations::atom_store::time_queue::QueueSurvey;
use crate::db_operations::atom_store::{record_digest, AtomStore, RangeWalk};
use crate::schema::SchemaError;

/// Ids of entries the survey keeps, at most.
const SURVEY_IDS: usize = 5_000_000;

impl AtomStore {
    /// Check, and with `execute` repair, the retention rows of the counted
    /// records of `scope`.
    pub(super) async fn recount_retention(
        &self,
        scope: Option<&str>,
        execute: bool,
        report: &mut RecountReport,
    ) -> Result<(), SchemaError> {
        let state = self.retention_state();
        let mut survey = QueueSurvey::new(
            state.queue(),
            state.clock_nanos(),
            state.window_nanos(),
            TALLY_PAGE_ROWS,
            SURVEY_IDS,
        );
        while !survey.step(self).await? {}
        report.queue_entries_read += survey.entries();
        let unlisted = survey.confirmed_unlisted(self).await?;
        report.queue_entries_without_directory +=
            unlisted.iter().map(|(_, b)| b.entries).sum::<u64>();
        if !survey.complete() {
            report.notes.push(
                "the queue holds more entries than the survey keeps; records were not compared with it"
                    .into(),
            );
        }
        let mut orphans: Vec<String> = Vec::new();
        if survey.complete() {
            let (start, end) = family_range(Family::Records, scope);
            let mut walk = RangeWalk::new(&start, &end, TALLY_PAGE_ROWS);
            while !walk.done() {
                let step = self.walk_step(&mut walk).await?;
                if step.unsealed > 0 && execute {
                    return Err(unsealed_refusal(step.unsealed));
                }
                for (key, _) in &step.rows {
                    let key = String::from_utf8_lossy(key);
                    if !is_counted_record_key(&key) {
                        continue;
                    }
                    if record_digest(&key).is_some_and(|digest| !survey.has(digest)) {
                        orphans.push(key.into_owned());
                    }
                }
            }
        }
        report.records_without_queue_entry += orphans.len() as u64;
        if execute && (!orphans.is_empty() || !unlisted.is_empty()) {
            let buckets: Vec<u64> = unlisted.iter().map(|(bucket, _)| *bucket).collect();
            let repaired = self
                .repair_record_retention(scope, &orphans, &buckets)
                .await?;
            report.retention_records_repaired += repaired.records;
            report.retention_buckets_repaired += repaired.buckets;
        }
        Ok(())
    }
}
