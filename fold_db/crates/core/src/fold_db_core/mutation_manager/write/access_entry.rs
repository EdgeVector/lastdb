//! Moved verbatim out of the parent module; see the parent for context.

use super::*;
use crate::schema::types::MutationType;

impl MutationManager {
    pub(super) async fn write_mutations_batch_with_receipt_admitted(
        &self,
        mut mutations: Vec<Mutation>,
        now: u64,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        self.validate_mutation_batch(&mutations)?;
        // Which mutations arrive WITHOUT a clock is the only signal that says
        // which ones this node is authoring, and stamping erases it. Capture it
        // first so the receipt reports the counter this batch allocated rather
        // than an imported peer's, which would name a revision no local read
        // can be checked against.
        let authored_locally: Vec<bool> = mutations
            .iter()
            .map(|mutation| mutation.imported_written_at.is_none())
            .collect();
        let wait_for_author_clock = mutations
            .iter()
            .any(|mutation| mutation.synchronous == Some(true));
        let author_clock_persist = self.prepare_mutation_author_clocks(&mut mutations, now)?;
        // Submit before resident apply. Each data envelope waits on a clone of
        // this barrier, so durable LWW data cannot pass its durable clock.
        let inline_clock = mutations.iter().all(|mutation| {
            matches!(
                mutation.mutation_type,
                MutationType::Create | MutationType::Update
            )
        });
        let author_clock_barrier = author_clock_persist.and_then(|(reservation, state)| {
            if inline_clock {
                drop(reservation);
                None
            } else {
                Some(reservation.submit_with_barrier(state))
            }
        });
        // One logical counter per caller batch, so its first local mutation
        // carries the common revision. Per-item author times preserve order.
        let revision = authored_locally
            .iter()
            .zip(mutations.iter())
            .find(|(authored, _)| **authored)
            .map(|(_, mutation)| mutation.logical_counter);
        // A synchronous receipt includes the author clock. Persist it before
        // data, so a process loss cannot reopen below a durable LWW value.
        // A later data failure leaves a harmless counter gap.
        if wait_for_author_clock {
            if let Some(barrier) = &author_clock_barrier {
                barrier.wait().await?;
            }
        }
        let mut receipt = self
            .write_mutations_batch_inner_with_clock(mutations, author_clock_barrier)
            .await?;
        receipt.revision = revision;
        Ok(receipt)
    }
}
