use super::records::RecordCandidates;
use super::*;
use std::sync::Mutex;

/// The facts of one native request: the records it staged.
#[derive(Default)]
pub(in crate::fold_db_core::mutation_manager) struct Collected {
    pub records: RecordCandidates,
}

tokio::task_local! {
    static CANDIDATES: Mutex<RecordCandidates>;
}

pub(in crate::fold_db_core::mutation_manager) async fn collect<T>(
    runtime: &PackRuntime,
    future: impl std::future::Future<Output = Result<T, SchemaError>>,
) -> Result<(T, Collected), SchemaError> {
    if !runtime.logical_capture_enabled() {
        return Ok((future.await?, Collected::default()));
    }
    CANDIDATES
        .scope(Mutex::new(RecordCandidates::default()), async {
            let result = future.await?;
            let records = CANDIDATES.with(|state| {
                std::mem::take(&mut *state.lock().expect("logical capture candidates"))
            });
            Ok((result, Collected { records }))
        })
        .await
}

/// Run `apply` on the record candidates of the current capture scope.
pub(super) fn with_record_candidates<T>(
    apply: impl FnOnce(&mut RecordCandidates) -> Result<T, SchemaError>,
) -> Result<T, SchemaError> {
    CANDIDATES
        .try_with(|state| apply(&mut state.lock().expect("logical capture candidates")))
        .map_err(|_| unsupported("an uncaptured native preparation"))?
}
