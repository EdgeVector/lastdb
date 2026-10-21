//! The blob phases of `verify-refs`: read one page of blob counters or of queue
//! entries, and judge it. Nothing here writes.

use super::blobs::{apply_blobs, apply_queue};
use super::report::Phase;
use super::state::StepGuard;
use crate::db_operations::atom_store::{unsealed_run_error, AtomStore, Progress};
use crate::schema::SchemaError;

impl AtomStore {
    /// One page of the blobs phase (the counters) or of the blob queue phase
    /// (the entries of all buckets, one bounded range walk).
    pub(super) async fn verify_blob_page(
        &self,
        guard: &mut StepGuard<'_>,
    ) -> Result<(), SchemaError> {
        let (run, _) = guard.split();
        let request = run
            .walk
            .request()
            .expect("a walk that is not done has a read");
        let outcome = if run.phase == Phase::Blobs {
            self.read_blobs_page(request).await.map(|page| {
                run.note_unsealed(page.scan_unsealed + page.point_unsealed);
                let moved =
                    run.walk
                        .finish_read(page.rows, page.scan_unsealed, page.last_key.as_deref());
                if moved == Progress::Moved {
                    apply_blobs(run, page);
                }
                moved
            })
        } else {
            self.read_queue_page(request).await.map(|page| {
                run.note_unsealed(page.scan_unsealed + page.point_unsealed);
                let moved =
                    run.walk
                        .finish_read(page.rows, page.scan_unsealed, page.last_key.as_deref());
                if moved == Progress::Moved {
                    apply_queue(run, page);
                }
                moved
            })
        };
        match outcome {
            Ok(Progress::Moved) => {}
            Ok(Progress::Widened) => run.report.page_retries += 1,
            Ok(Progress::Stuck) => return Err(unsealed_run_error()),
            Err(SchemaError::ReadResourceLimit { .. }) => {
                // The read hit the limit of one call. Ask for fewer rows or a
                // narrower key range, in a new read scope.
                run.report.page_retries += 1;
                if !run.walk.read_too_big() {
                    return Err(run.walk.failure(SchemaError::ReadResourceLimit {
                        detail: "a key range is too dense to read in pieces".into(),
                    }));
                }
            }
            Err(error) => return Err(error),
        }
        let stats = run.walk.take_stats();
        run.report.range_reads.absorb(stats);
        self.range_walks().add(stats);
        Ok(())
    }
}
