//! One validated batch owns its receipt identities across every write verb.

use super::*;

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) async fn write_mutations_batch_inner_with_clock(
        &self,
        mutations: Vec<Mutation>,
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        if mutations.is_empty() {
            return Ok(ResidentCommitReceipt::empty());
        }
        self.validate_mutation_batch(&mutations)?;
        self.prepare_batch_field_keys(&mutations).await?;
        self.reject_blocked_mutation_targets(&mutations)?;
        for mutation in &mutations {
            mutation.validate_record_intent()?;
        }
        // Complete replacements and erasures share one native request.
        // Every revision condition succeeds before any record is published.
        self.write_create_update_batch_async(mutations, author_clock_barrier)
            .await
    }
}
