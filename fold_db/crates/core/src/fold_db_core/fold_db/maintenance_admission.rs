//! Admission that a node maintenance caller can cancel before it starts work.

use super::FoldDB;
use std::future::Future;

pub struct LocalMutationAdmission {
    admission: Option<crate::sync::capture::MutationAdmission>,
}

impl LocalMutationAdmission {
    /// Once admitted, finish the complete operation. Its durable clock jobs
    /// retain this same guard; a selected source cut waits for their writes.
    pub async fn run<F: Future>(self, future: F) -> F::Output {
        crate::sync::capture::with_existing_mutation_admission(self.admission, future).await
    }
}

impl FoldDB {
    /// Waiting for this permit does not perform a write and can be cancelled.
    pub async fn acquire_local_mutation_admission(&self) -> LocalMutationAdmission {
        let admission = match &self.mutation_log_capture {
            Some(router) => router.acquire_maintenance_admission().await,
            None => None,
        };
        LocalMutationAdmission { admission }
    }
}
