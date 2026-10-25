//! Local mutation admission after legacy cloud capture retirement.
//! The engine-absent path writes no log, outbox, marker or cloud request.
use crate::fold_db_core::mutation_manager::{
    AuthorClockPersistBarrier, CloudCapturePolicy, CloudCaptureState, CloudMutationReceipt,
    CloudPublicationState,
};
use crate::storage::error::{StorageError, StorageResult};
use crate::storage::traits::{KvStore, NamespacedStore, ReadCostStats};
use crate::sync::log::MutationEnvelope;
use async_trait::async_trait;
use std::future::Future;
use std::sync::Arc;

mod mutation_fence;
mod namespaced;
pub(crate) use mutation_fence::*;
pub(crate) use namespaced::MutationLogCaptureNamespacedStore;

#[derive(Default)]
pub(crate) struct MutationLogCaptureRouter {
    activation_gate: Arc<tokio::sync::RwLock<()>>,
}
pub(crate) struct RouterKvSuppressGuard;
impl MutationLogCaptureRouter {
    pub(crate) async fn fence_mutations(&self) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        Arc::clone(&self.activation_gate).write_owned().await
    }
    pub(crate) fn enter_kv_suppress(&self) -> RouterKvSuppressGuard {
        RouterKvSuppressGuard
    }
    pub(crate) async fn wait_for_completion(&self, _timeout: std::time::Duration) -> bool {
        std::future::ready(true).await
    }
}

pub(crate) fn mark_logical_commit_published() {}

pub(crate) async fn with_capture_suppressed<F: Future>(future: F) -> F::Output {
    future.await
}

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

pub(crate) async fn capture_logical_commit_with_policy_and_author_clock<F, T, E>(
    router: Option<Arc<MutationLogCaptureRouter>>,
    envelopes: Vec<MutationEnvelope>,
    policy: CloudCapturePolicy,
    _author_clock_barrier: Option<AuthorClockPersistBarrier>,
    future: F,
) -> Result<(T, Option<CloudMutationReceipt>), E>
where
    F: Future<Output = Result<T, E>>,
    E: From<StorageError>,
{
    with_mutation_admission(router, async move {
        let value = future.await?;
        let cloud = (!matches!(policy, CloudCapturePolicy::Async)).then(|| CloudMutationReceipt {
            mutation_uuid: envelopes
                .iter()
                .find_map(|envelope| {
                    (!envelope.mutation_uuid.is_empty()).then(|| envelope.mutation_uuid.clone())
                })
                .unwrap_or_default(),
            capture_state: CloudCaptureState::Failed,
            publication_state: CloudPublicationState::Failed,
            targets: Vec::new(),
            error: Some("cloud sync capture is unavailable".to_string()),
        });
        Ok((value, cloud))
    })
    .await
}
