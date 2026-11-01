//! The creation intent of a new node home.
//!
//! A new home commits its key-format proofs before its node marker (see
//! `run_node`). Until logical construction checks them, the runtime admits
//! only registry writes.

use super::*;

impl PackRuntime {
    pub(crate) fn fresh_initialization_pending(&self) -> bool {
        self.initializing.load(Ordering::Acquire)
    }

    pub(in crate::storage) fn require_initialized(&self) -> StorageResult<()> {
        if self.fresh_initialization_pending() {
            return Err(refusal(
                "current creation requires its durable key proofs first",
            ));
        }
        Ok(())
    }

    /// Every native source mutation reaches this check before queue admission.
    /// Registry-only puts cannot create user data. Creation commits its key
    /// proofs before the node marker, so every other write waits for the intent.
    pub(super) fn validate_initialization_write(
        &self,
        operations: &[HomeMutation],
    ) -> StorageResult<()> {
        if !self.fresh_initialization_pending() {
            return Ok(());
        }
        let registry_only = operations.iter().all(|operation| {
            operation.collection == REGISTRY
                && operation.key.starts_with(NAMESPACES)
                && operation.body.as_deref() == Some(&[1])
        });
        if !registry_only {
            return Err(refusal(
                "creation already holds its durable proofs; source writes must wait",
            ));
        }
        Ok(())
    }

    /// The caller first checks the key-format proofs. Cancellation retains the intent and the write guard.
    pub(crate) async fn finish_fresh_initialization(self: &Arc<Self>) -> StorageResult<()> {
        if !self.fresh_initialization_pending() {
            return Ok(());
        }
        if transaction::PendingBatch::current(self).is_some() {
            return Err(refusal(
                "current creation needs its own durable transaction",
            ));
        }
        let gate = self.enter_write().await;
        let runtime = Arc::clone(self);
        super::run_blocking(move || {
            let _gate = gate;
            if runtime.failed.load(Ordering::Acquire) {
                return Err(refusal("current creation requires a snapshot reopen"));
            }
            if runtime.fresh_initialization_pending() {
                super::run_node::complete_initialization(runtime.physical.path())?;
                runtime.initializing.store(false, Ordering::Release);
            }
            Ok(())
        })
        .await
    }
}

fn refusal(message: &str) -> StorageError {
    StorageError::InvalidOperation(format!("native home creation refused: {message}"))
}
