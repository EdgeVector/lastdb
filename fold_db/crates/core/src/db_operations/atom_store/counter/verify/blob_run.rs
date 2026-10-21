//! The blob phases of `verify-refs`: read one page of blob counters or of queue
//! entries, and judge it. Nothing here writes.

use super::blobs::{apply_blobs, apply_queue};
use super::report::Phase;
use super::state::StepGuard;
use crate::db_operations::atom_store::AtomStore;
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
                run.walk
                    .finish_yield(page.rows, page.scan_unsealed, page.resume.as_deref());
                apply_blobs(run, page);
            })
        } else {
            self.read_queue_page(request).await.map(|page| {
                run.note_unsealed(page.scan_unsealed + page.point_unsealed);
                run.walk
                    .finish_yield(page.rows, page.scan_unsealed, page.resume.as_deref());
                apply_queue(run, page);
            })
        };
        match outcome {
            Ok(()) => {}
            Err(SchemaError::ReadResourceLimit { .. }) => {
                // The read hit the limit of one call. Ask for fewer rows or a
                // narrower key range, in a new read scope.
                run.report.page_retries += 1;
                if !run.walk.read_too_big() {
                    return Err(SchemaError::ReadResourceLimit {
                        detail: "a key range is too dense to read in pieces".into(),
                    });
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
