//! The retention step of the recount: entries in a bucket that the directory of
//! the retention queue does not list.
//!
//! The recount surveys the retention queue once (`time_queue/survey.rs`: the
//! entries of every listed bucket, and of the slices that hold a live key and no
//! directory row). The queue is global, so one survey covers every scope. With
//! `--execute` it writes the missing directory rows again
//! (`TimeQueue::register_buckets`). The step writes nothing else.

use super::report::RecountReport;
use super::PAGE_ROWS;
use crate::db_operations::atom_store::retention::RepairWindow;
use crate::db_operations::atom_store::time_queue::QueueSurvey;
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;

impl AtomStore {
    /// The queue layout and the window of the retention step. The recount is a
    /// process of its own, and the window of its environment may differ from the
    /// one the node used when it wrote the entries. The node records its window
    /// (`retention/window.rs`), and the recount uses that. A home whose janitor
    /// never ran has no record, and the recount then uses its own environment.
    async fn recount_window(
        &self,
        report: &mut RecountReport,
    ) -> Result<RepairWindow, SchemaError> {
        let state = self.retention_state();
        let own_secs = state.window_nanos() / 1_000_000_000;
        let Some(recorded) = self.recorded_retention_window().await? else {
            return Ok(RepairWindow {
                queue: state.queue(),
                window_nanos: state.window_nanos(),
            });
        };
        if recorded != own_secs {
            report.notes.push(format!(
                "the node recorded a retention window of {recorded} s, and this process has {own_secs} s (LASTDB_CONFLICT_RETENTION_SECS); the retention step uses {recorded} s"
            ));
        }
        let window_nanos = recorded.saturating_mul(1_000_000_000);
        Ok(RepairWindow {
            queue: crate::db_operations::atom_store::retention::queue_for_window(window_nanos),
            window_nanos,
        })
    }

    /// Check, and with `execute` repair, the directory rows of the retention
    /// queue.
    pub(super) async fn recount_retention(
        &self,
        execute: bool,
        report: &mut RecountReport,
    ) -> Result<(), SchemaError> {
        let window = self.recount_window(report).await?;
        let mut survey = QueueSurvey::new(
            window.queue,
            self.retention_state().clock_nanos(),
            window.window_nanos,
            PAGE_ROWS,
        );
        while !survey.step(self).await? {}
        report.queue_entries_read += survey.entries();
        let unlisted = survey.confirmed_unlisted(self).await?;
        report.queue_entries_without_directory +=
            unlisted.iter().map(|(_, b)| b.entries).sum::<u64>();
        if !survey.complete() {
            report.notes.push(
                "the queue directory lists more buckets than the survey walks; the rest were not checked"
                    .into(),
            );
        }
        if execute && !unlisted.is_empty() {
            let buckets: Vec<u64> = unlisted.iter().map(|(bucket, _)| *bucket).collect();
            report.retention_buckets_repaired +=
                self.repair_queue_buckets(&buckets, &window).await?;
        }
        Ok(())
    }
}
