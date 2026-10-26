//! Local mutation admission after legacy cloud capture retirement.
//! The engine-absent path writes no log, outbox, marker or cloud request.
use std::future::Future;
use std::sync::Arc;

mod mutation_fence;
pub(crate) use mutation_fence::*;

#[derive(Default)]
pub(crate) struct MutationLogCaptureRouter {
    activation_gate: Arc<tokio::sync::RwLock<()>>,
}

impl MutationLogCaptureRouter {
    pub(crate) async fn fence_mutations(&self) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        Arc::clone(&self.activation_gate).write_owned().await
    }
}
