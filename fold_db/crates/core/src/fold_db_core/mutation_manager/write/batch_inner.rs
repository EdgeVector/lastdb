//! One validated batch owns its receipt identities across every write verb.

use super::*;

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) async fn write_mutations_batch_inner_with_clock(
        &self,
        mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        if mutations.is_empty() {
            return Ok(ResidentCommitReceipt::empty());
        }
        self.validate_mutation_batch(&mutations)?;
        let mutation_ids = mutations
            .iter()
            .map(|mutation| mutation.uuid.clone())
            .collect();
        self.prepare_batch_field_keys(&mutations).await?;
        self.reject_blocked_mutation_targets(&mutations)?;
        for mutation in &mutations {
            mutation.reject_illegal_must_exist()?;
        }
        // Every verb merges into its record inside the native request: a
        // delete or a purge merges as the delete floor, in caller order with
        // the other writes (write/records.rs).
        let mut receipt = self
            .write_create_update_batch_async(mutations, storage_prefix, author_clock_barrier)
            .await?;
        // Identities describe the accepted caller inputs, including idempotent
        // repeats. Internal grouping and filtering cannot change the positional
        // association between an input and its receipt identity.
        receipt.mutation_ids = mutation_ids;
        Ok(receipt)
    }
}
