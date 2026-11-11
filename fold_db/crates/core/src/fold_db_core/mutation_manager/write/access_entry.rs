//! Stamp author clocks and execute one validated mutation batch.

use super::*;
use crate::schema::mutation_author::sign_mutation_author_clock;

impl MutationManager {
    pub(super) async fn write_mutations_batch_with_receipt_admitted(
        &self,
        mut mutations: Vec<Mutation>,
        now: u64,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        self.validate_mutation_batch(&mutations)?;
        self.prepare_mutation_author_clocks(&mut mutations, now)?;
        // One logical counter per caller batch. Empty input has none.
        let revision = mutations.first().map(|mutation| mutation.logical_counter);
        let mut receipt = self.write_mutations_batch_inner(mutations).await?;
        receipt.revision = revision;
        Ok(receipt)
    }

    fn prepare_mutation_author_clocks(
        &self,
        mutations: &mut [Mutation],
        now: u64,
    ) -> Result<(), SchemaError> {
        if mutations.is_empty() {
            return Ok(());
        }
        let local_writer_id = self.signer.public_key_base64();
        let mut resident_state = self
            .author_clock_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut state = *resident_state;
        let (mut times, logical_counter) = state
            .advance_local_batch(now, mutations.len())
            .ok_or_else(|| {
                SchemaError::InvalidData("mutation author-clock range exhausted".into())
            })?;
        for mutation in mutations {
            let written_at = times.next().expect("one reserved time per local mutation");
            mutation.imported_written_at = Some(written_at);
            mutation.logical_counter = logical_counter;
            mutation.author_clock_writer_id = local_writer_id.clone();
            mutation.author_clock_signature_version =
                crate::schema::types::MUTATION_AUTHOR_SIGNATURE_VERSION;
            mutation.author_clock_signature =
                sign_mutation_author_clock(mutation, self.signer.as_ref());
        }
        *resident_state = state;
        Ok(())
    }
}
