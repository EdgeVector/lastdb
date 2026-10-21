//! The queue phase of `verify-refs`: survey the retention queue, then compare
//! the entries with the conflict records that the records phase counted.

use super::pages::phase_range;
use super::report::{Kind, Phase};
use super::state::{Run, StepGuard};
use crate::db_operations::atom_store::time_queue::QueueSurvey;
use crate::db_operations::atom_store::{AtomStore, RangeWalk};
use crate::schema::SchemaError;

impl AtomStore {
    /// One page of the queue phase: the first call lists the directory and the
    /// unlisted slices, each later call reads one page of one bucket. The last
    /// call judges the survey.
    pub(super) async fn verify_queue_page(
        &self,
        guard: &mut StepGuard<'_>,
    ) -> Result<(), SchemaError> {
        let (run, _) = guard.split();
        if run.survey.is_none() {
            let state = self.retention_state();
            run.survey = Some(QueueSurvey::new(
                state.queue(),
                state.clock_nanos(),
                state.window_nanos(),
                run.page_rows,
                run.max_ids,
            ));
        }
        let survey = run.survey.as_mut().expect("the survey was just made");
        let done = survey.step(self).await?;
        run.report.pages += 1;
        if done {
            self.finish_queue(run).await?;
        }
        Ok(())
    }

    /// Judge the survey: buckets without a directory row, and the records that
    /// no entry names. The records are read again in the last phase.
    async fn finish_queue(&self, run: &mut Run) -> Result<(), SchemaError> {
        let Some(survey) = run.survey.take() else {
            return Ok(());
        };
        run.report.queue_entries_read = survey.entries();
        for (bucket, found) in survey.confirmed_unlisted(self).await? {
            let sample = found.sample.first().cloned().unwrap_or_default();
            run.note_faults(
                Kind::QueueEntryWithoutDirectory,
                &sample.replace('\0', "\\0"),
                format!(
                    "bucket {bucket} holds {} entries and the directory of the queue has no row for it",
                    found.entries
                ),
                found.entries,
            );
        }
        if survey.unknown() > 0 {
            run.report.notes.push(format!(
                "the probe of {} queue slices gave up; they were not judged",
                survey.unknown()
            ));
        }
        let judge = survey.complete() && run.ids_complete;
        if !judge {
            run.report.notes.push(
                "more records or queue entries than --max-atoms; records were not compared with the queue"
                    .into(),
            );
        }
        run.suspects = if judge {
            run.record_ids
                .iter()
                .filter(|id| !survey.has(**id))
                .copied()
                .collect()
        } else {
            std::collections::HashSet::new()
        };
        run.record_ids = std::collections::HashSet::new();
        if run.suspects.is_empty() {
            // The blob counters are global. The personal run checks them once,
            // after the retention queue.
            run.phase = if run.report.scope.is_none() {
                Phase::Blobs
            } else {
                Phase::Done
            };
            if let Some((start, end)) = phase_range(run.phase, run.report.scope.as_deref()) {
                run.walk = RangeWalk::new(&start, &end, run.page_rows);
            }
            if run.phase == Phase::Done && !run.tally.complete {
                run.report.notes.push(format!(
                    "more than {} atoms are referenced; counters were not compared with the totals of the pass",
                    run.tally.len()
                ));
            }
        } else {
            run.phase = Phase::Suspects;
            if let Some((start, end)) = phase_range(Phase::Suspects, run.report.scope.as_deref()) {
                run.walk = RangeWalk::new(&start, &end, run.page_rows);
            }
        }
        Ok(())
    }
}
