//! One mutation admission shared by the request and its durable clock lane.

use super::*;

#[derive(Clone)]
pub(crate) struct MutationAdmission {
    activation_gate: Arc<tokio::sync::RwLock<()>>,
    // An admitted clock job retains the same read guard until its durable put.
    _guard: Arc<tokio::sync::OwnedRwLockReadGuard<()>>,
}

tokio::task_local! {
    static MUTATION_ADMISSION: MutationAdmission;
}

impl MutationLogCaptureRouter {
    pub(crate) async fn enter_mutation(&self) -> Option<tokio::sync::OwnedRwLockReadGuard<()>> {
        let already_admitted = MUTATION_ADMISSION
            .try_with(|admission| Arc::ptr_eq(&admission.activation_gate, &self.activation_gate))
            .unwrap_or(false);
        if already_admitted {
            None
        } else {
            Some(Arc::clone(&self.activation_gate).read_owned().await)
        }
    }
}

/// Enter before any pending task is submitted. Nested capture reuses this
/// admission instead of waiting behind a snapshot writer while holding a read.
pub(crate) async fn with_mutation_admission<F, T>(
    router: Option<Arc<MutationLogCaptureRouter>>,
    future: F,
) -> T
where
    F: Future<Output = T>,
{
    let Some(router) = router else {
        return future.await;
    };
    let Some(guard) = router.enter_mutation().await else {
        return future.await;
    };
    let admission = MutationAdmission {
        activation_gate: Arc::clone(&router.activation_gate),
        _guard: Arc::new(guard),
    };
    MUTATION_ADMISSION.scope(admission, future).await
}

pub(crate) fn current_mutation_admission() -> Option<MutationAdmission> {
    MUTATION_ADMISSION.try_with(Clone::clone).ok()
}

/// The queued clock write keeps FIFO capture while reusing its request guard.
pub(crate) async fn with_existing_mutation_admission<F, T>(
    admission: Option<MutationAdmission>,
    future: F,
) -> T
where
    F: Future<Output = T>,
{
    match admission {
        Some(admission) => MUTATION_ADMISSION.scope(admission, future).await,
        None => future.await,
    }
}
