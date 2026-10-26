//! Admit the request before it submits durable author-clock work.

use super::*;

impl MutationManager {
    /// [`Self::write_mutations_batch_async`] answering with the full
    /// [`ResidentCommitReceipt`] instead of only the mutation ids.
    ///
    /// This is the real body; the id-returning entry point is a thin
    /// projection of it, so the two cannot drift.
    pub async fn write_mutations_batch_with_receipt(
        &self,
        mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        // The admitted write owns the complete mutation pipeline. Keep that
        // future off callers' default request and maintenance thread stacks.
        // Erase its concrete type here so spawned callers need not resolve the
        // complete nested pipeline again to prove that their future is Send.
        let future: futures::future::BoxFuture<'_, Result<ResidentCommitReceipt, SchemaError>> =
            Box::pin(self.write_mutations_batch_with_receipt_admitted(mutations, storage_prefix));
        crate::sync::capture::with_mutation_admission(self.capture_router(), future).await
    }
}
