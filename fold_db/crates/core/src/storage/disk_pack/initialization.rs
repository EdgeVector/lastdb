//! The first batch of a new node home: the key-format proofs.
//!
//! A new v2 home holds its creation intent until that batch is durable (see
//! `run_node`). Until then the runtime admits only that batch.

use super::*;

impl PackRuntime {
    pub(crate) fn fresh_initialization_pending(&self) -> bool {
        self.initializing.load(Ordering::Acquire)
    }

    pub(super) fn require_initialized(&self) -> StorageResult<()> {
        if self.fresh_initialization_pending() {
            return Err(refusal(
                "current creation requires its durable key proofs first",
            ));
        }
        Ok(())
    }

    /// Every native source mutation reaches this check before queue admission.
    /// Registry-only puts cannot create user data. The sole other permitted
    /// batch contains every current initialization key, its optional account
    /// proof, and no source records.
    pub(super) fn validate_initialization_write(
        &self,
        operations: &[HomeMutation],
    ) -> StorageResult<()> {
        if !self.fresh_initialization_pending() {
            return Ok(());
        }
        let keys = crate::db_operations::atom_store::CURRENT_NATIVE_INITIALIZATION_KEYS;
        let expected: Vec<_> = keys
            .iter()
            .map(|key| kv::coordinate("main", key.as_bytes()))
            .collect();
        let mut found = vec![false; expected.len()];
        let mut proof_seen = false;
        for operation in operations {
            if operation.collection == REGISTRY
                && operation.key.starts_with(NAMESPACES)
                && operation.body.as_deref() == Some(&[1])
            {
                continue;
            }
            if self.range_order() == schema_types::key_comparison::KeyOrderVersion::TypedScalarV1 {
                return Err(refusal(
                    "typed creation already requires its durable proofs; source writes must wait",
                ));
            }
            if operation.collection == crate::crypto::content_key_proof::NAMESPACE
                && operation.key.as_bytes() == crate::crypto::content_key_proof::KEY
                && operation.body.is_some()
                && !proof_seen
            {
                proof_seen = true;
                continue;
            }
            let at = expected.iter().position(|(collection, key)| {
                collection == &operation.collection && key == &operation.key
            });
            match at {
                Some(at) if operation.body.is_some() && !found[at] => found[at] = true,
                _ => return Err(refusal("source writes are refused during current creation")),
            }
        }
        if (proof_seen || found.iter().any(|found| *found)) && !found.iter().all(|found| *found) {
            return Err(refusal(
                "current creation requires one complete key-proof batch",
            ));
        }
        Ok(())
    }

    /// The caller first validates the complete logical format batch. Cancellation retains the intent and the write guard.
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
