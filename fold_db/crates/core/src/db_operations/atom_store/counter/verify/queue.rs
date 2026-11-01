//! The queue phase of `verify-refs`: survey the retention queue and report the
//! buckets that hold entries and have no directory row.

use super::report::{Kind, Phase};
use super::run::enter_phase;
use super::state::{Run, StepGuard};
use crate::db_operations::atom_store::time_queue::QueueSurvey;
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;

impl AtomStore {
    /// One page of the queue phase: the first call lists the directory and the
    /// unlisted slices, each later call reads one page of one bucket. The last
    /// call judges the survey.
    pub(super) async fn verify_queue_page(
        &self,
        guard: &mut StepGuard<'_>,
    ) -> Result<(), SchemaError> {
        let run = guard.run();
        if run.survey.is_none() {
            let state = self.retention_state();
            run.survey = Some(QueueSurvey::new(
                state.queue(),
                state.clock_nanos(),
                state.window_nanos(),
                run.page_rows,
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

    /// Judge the survey: buckets without a directory row. Then the personal run
    /// checks the blob counters, which are global, once.
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
        if !survey.complete() {
            run.report.notes.push(
                "the queue directory lists more buckets than the survey walks; the rest were not judged"
                    .into(),
            );
        }
        let next = if run.report.scope.is_none() {
            Phase::Blobs
        } else {
            Phase::Done
        };
        enter_phase(run, next);
        Ok(())
    }
}
