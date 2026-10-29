//! One mutation admission shared by the request and its durable clock lane.

use std::future::Future;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct MutationAdmission {
    gate: Arc<tokio::sync::RwLock<()>>,
    // An admitted clock job retains the same read guard until its durable put.
    _guard: Arc<tokio::sync::OwnedRwLockReadGuard<()>>,
}

tokio::task_local! {
    static MUTATION_ADMISSION: MutationAdmission;
}

/// One node-local fence shared by serving writes and deferred clock work.
#[derive(Default)]
pub(crate) struct MutationAdmissionGate {
    gate: Arc<tokio::sync::RwLock<()>>,
}

impl MutationAdmissionGate {
    pub(crate) async fn fence_mutations(&self) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        Arc::clone(&self.gate).write_owned().await
    }

    fn current_task_has_mutation_admission(&self) -> bool {
        MUTATION_ADMISSION
            .try_with(|admission| Arc::ptr_eq(&admission.gate, &self.gate))
            .unwrap_or(false)
    }

    pub(crate) async fn acquire_maintenance_admission(&self) -> Option<MutationAdmission> {
        let guard = self.enter_mutation().await?;
        Some(MutationAdmission {
            gate: Arc::clone(&self.gate),
            _guard: Arc::new(guard),
        })
    }

    async fn enter_mutation(&self) -> Option<tokio::sync::OwnedRwLockReadGuard<()>> {
        if self.current_task_has_mutation_admission() {
            None
        } else {
            Some(Arc::clone(&self.gate).read_owned().await)
        }
    }
}

/// Enter before any pending task is submitted. Nested writes reuse this
/// admission instead of waiting behind a snapshot writer while holding a read.
pub(crate) async fn with_mutation_admission<F, T>(gate: &MutationAdmissionGate, future: F) -> T
where
    F: Future<Output = T>,
{
    let Some(guard) = gate.enter_mutation().await else {
        return future.await;
    };
    let admission = MutationAdmission {
        gate: Arc::clone(&gate.gate),
        _guard: Arc::new(guard),
    };
    MUTATION_ADMISSION.scope(admission, future).await
}

pub(crate) fn current_mutation_admission() -> Option<MutationAdmission> {
    MUTATION_ADMISSION.try_with(Clone::clone).ok()
}

/// The queued clock write keeps FIFO admission while reusing its request guard.
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
