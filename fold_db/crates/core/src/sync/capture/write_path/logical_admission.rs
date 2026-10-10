//! Keep the logical request admission with its queued durable capture.

use super::*;

pub(crate) async fn capture_logical_commit_with_policy_and_author_clock<F, T, E>(
    router: Option<Arc<MutationLogCaptureRouter>>,
    envelopes: Vec<MutationEnvelope>,
    policy: CloudCapturePolicy,
    author_clock_barrier: Option<AuthorClockPersistBarrier>,
    future: F,
) -> Result<(T, Option<CloudMutationReceipt>), E>
where
    F: Future<Output = Result<T, E>>,
    E: From<StorageError>,
{
    with_mutation_admission(
        router.clone(),
        super::logical_commit::capture_logical_commit_admitted(
            router,
            envelopes,
            policy,
            author_clock_barrier,
            future,
        ),
    )
    .await
}
