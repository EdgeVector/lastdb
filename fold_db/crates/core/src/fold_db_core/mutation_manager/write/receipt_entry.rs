//! The batch write entry point that answers with the full receipt.

use super::*;

impl MutationManager {
    /// Write a batch of mutations and answer with the full
    /// [`ResidentCommitReceipt`].
    ///
    /// This is the real body; the id-returning entry point is a thin
    /// projection of it, so the two cannot drift.
    pub async fn write_mutations_receipt(
        &self,
        mutations: Vec<Mutation>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        // The write owns the complete mutation pipeline. Keep that future off
        // callers' default request and maintenance thread stacks. Erase its
        // concrete type here so spawned callers need not resolve the complete
        // nested pipeline again to prove that their future is Send.
        // One wall-clock sample for the batch, taken when the pipeline runs.
        // Author-clock allocation uses this value.
        let future: futures::future::BoxFuture<'_, Result<ResidentCommitReceipt, SchemaError>> =
            Box::pin(async move {
                let now = crate::clock::unix_nanos();
                self.write_mutations_batch_with_receipt_admitted(mutations, now)
                    .await
            });
        future.await
    }
}
