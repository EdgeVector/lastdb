//! Admit the request before it submits durable author-clock work.

use super::*;

impl MutationManager {
    pub(super) async fn write_mutations_batch_with_receipt_cloud(
        &self,
        mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
        cloud_policy: CloudCapturePolicy,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        let future = self.write_mutations_batch_with_receipt_cloud_admitted(
            mutations,
            storage_prefix,
            cloud_policy,
        );
        #[cfg(feature = "cloud-sync")]
        return crate::sync::capture::with_mutation_admission(self.capture_router(), future).await;
        #[cfg(not(feature = "cloud-sync"))]
        future.await
    }
}
