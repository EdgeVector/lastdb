//! Admit the request before it submits durable author-clock work.

use super::*;

impl MutationManager {
    pub(super) async fn write_mutations_batch_with_receipt_cloud(
        &self,
        mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
        cloud_policy: CloudCapturePolicy,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        // The admitted write owns the complete mutation pipeline. Keep that
        // future off callers' default request and maintenance thread stacks.
        let future = Box::pin(self.write_mutations_batch_with_receipt_cloud_admitted(
            mutations,
            storage_prefix,
            cloud_policy,
        ));
        crate::sync::capture::with_mutation_admission(self.capture_router(), future).await
    }
}
