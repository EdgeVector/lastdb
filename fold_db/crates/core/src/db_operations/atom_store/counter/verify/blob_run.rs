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
        let run = guard.run();
        let request = run
            .walk
            .request()
            .expect("a walk that is not done has a read");
        let outcome = if run.phase == Phase::Blobs {
            self.read_blobs_page(request).await.map(|page| {
                run.walk.finish_yield(page.rows, page.resume.as_deref());
                apply_blobs(run, page);
            })
        } else {
            self.read_queue_page(request).await.map(|page| {
                run.walk.finish_yield(page.rows, page.resume.as_deref());
                apply_queue(run, page);
            })
        };
        self.settle_verify_page(run, outcome)
    }
}
