//! Logical-commit scopes: wrap one local mutation and stage its capture.

use super::logical_commit_finish::finish_logical_commit;
use super::*;

/// Run `future` without recording the mutation log. Used by replay so applying
/// a captured envelope cannot append a second copy of the same write.
pub(crate) async fn with_capture_suppressed<F, T>(future: F) -> T
where
    F: Future<Output = T>,
{
    LOGICAL_COMMIT_SCOPE
        .scope(
            RefCell::new(LogicalCommitScope {
                suppress_kv: true,
                record_intent: false,
                published: false,
                envelopes: Vec::new(),
            }),
            future,
        )
        .await
}

/// Capture one logical commit with an optional crash-safe or off-box receipt.
///
/// Only the durable-delete owner route selects a non-async policy. The normal
/// path remains the bounded queue handoff above.
pub(crate) async fn capture_logical_commit_with_policy<F, T, E>(
    router: Option<Arc<MutationLogCaptureRouter>>,
    envelopes: Vec<MutationEnvelope>,
    policy: CloudCapturePolicy,
    future: F,
) -> Result<(T, Option<CloudMutationReceipt>), E>
where
    F: Future<Output = Result<T, E>>,
    E: From<StorageError>,
{
    capture_logical_commit_with_policy_and_author_clock(router, envelopes, policy, None, future)
        .await
}

/// Capture one logical commit after its author clock reaches durable metadata.
pub(super) async fn capture_logical_commit_admitted<F, T, E>(
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
    // Router-wide `kv_suppress_depth` exists only so leftover KvStore puts on
    // a different task (persist/replay run their body on `spawn_blocking`,
    // which does not inherit `LOGICAL_COMMIT_SCOPE`) still see suppress — see
    // `MutationLogCaptureRouter::kv_suppress_depth`. A product write reaches
    // this function on its own request task, so a store-wide guard held for
    // the whole persist/replay/compact/reseal body must not skip it: only the
    // current task's own scope (nested replay on the same task) may.
    if task_local_suppressed() {
        return future.await.map(|value| (value, None));
    }
    // A live sync activation must wait for local-only commits which began
    // before the engine was attached. Later commits wait behind activation
    // and reserve capture against the new engine before resident apply.
    let _activation_guard = match router.as_ref() {
        Some(router) => router.enter_mutation().await,
        None => None,
    };
    let reservation = match router.as_ref() {
        Some(router) if !envelopes.is_empty() => router.reserve_capture().await.map_err(E::from)?,
        _ => None,
    };
    LOGICAL_COMMIT_SCOPE
        .scope(
            RefCell::new(LogicalCommitScope {
                suppress_kv: true,
                record_intent: true,
                published: false,
                envelopes,
            }),
            finish_logical_commit(future, reservation, policy, author_clock_barrier),
        )
        .await
}
