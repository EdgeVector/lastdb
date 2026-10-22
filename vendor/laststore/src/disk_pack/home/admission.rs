//! Owned payload and map capacity before the complete native append.

use super::{HomeMutation, HomeSnapshot, MutationQuote};
use crate::disk_pack::corrupt;
use crate::disk_pack::snapshot::SnapshotReservation;
use crate::Result;

/// Non-cloneable complete store quota. The exclusive native write gate must
/// remain owned until the corresponding complete root is published.
/// Physical space and protected logical records remain separate owned tokens.
pub struct MutationReservation {
    payload: SnapshotReservation,
    quote: MutationQuote,
    generation: u64,
}

impl MutationReservation {
    /// Complete dimensions held by this ticket.
    pub fn quote(&self) -> MutationQuote {
        self.quote
    }

    /// Transfer the admitted prefix after the caller accepts its extension.
    /// Preparation and transfer keep the same exclusive native write gate.
    pub fn absorb(&mut self, previous: MutationReservation) {
        assert_eq!(self.generation, previous.generation);
        self.payload.absorb(previous.payload);
    }
}

impl HomeSnapshot {
    /// Publish one complete immutable snapshot before durable success.
    pub fn commit_mutation(
        &mut self,
        operations: Vec<HomeMutation>,
        _accepted_ms: u64,
        reservation: MutationReservation,
    ) -> Result<crate::disk_pack::PublishedGeneration> {
        if reservation.generation != self.current().generation {
            return Err(corrupt("complete mutation reservation root changed"));
        }
        let actual = self
            .descriptor
            .quote_mutation_with_limit(&operations, self.current().batch_operation_limit())?;
        if actual.map_pages() > reservation.quote.map_pages()
            || actual.dependency_parts() > reservation.quote.dependency_parts()
            || actual.physical_bytes() > reservation.quote.physical_bytes()
        {
            return Err(corrupt(
                "complete mutation exceeded its admitted dimensions",
            ));
        }
        self.writer
            .commit_reserved(self.address(operations)?, reservation.payload)
    }

    /// Reserve payload and directory dependency capacity for all stored parts.
    /// This method does not authorize memory publication or a success ack.
    pub fn reserve_mutation(&self, operations: &[HomeMutation]) -> Result<MutationReservation> {
        let quote = self
            .descriptor
            .quote_mutation_with_limit(operations, self.current().batch_operation_limit())?;
        self.writer
            .check_dependency_capacity(quote.dependency_parts())?;
        let payload = self.writer.reserve_quote(quote.payload())?;
        Ok(MutationReservation {
            payload,
            quote,
            generation: self.current().generation,
        })
    }

    /// Reserve a replacement prefix using only the new request's dimensions.
    /// The caller retains the previous ticket until complete admission succeeds,
    /// then absorbs it into the returned ticket before append. Cancellation
    /// releases only the added charge and leaves the previous ticket intact.
    pub fn reserve_mutation_extension(
        &self,
        previous: &MutationReservation,
        incoming: &[HomeMutation],
    ) -> Result<MutationReservation> {
        if previous.generation != self.current().generation {
            return Err(corrupt("complete mutation reservation root changed"));
        }
        let limit = self.current().batch_operation_limit();
        let incoming = self.descriptor.quote_mutation_with_limit(incoming, limit)?;
        let quote = previous.quote.extended(incoming, limit)?;
        self.writer
            .check_dependency_capacity(quote.dependency_parts())?;
        let payload = self
            .writer
            .reserve_quote_extension(&previous.payload, quote.payload())?;
        Ok(MutationReservation {
            payload,
            quote,
            generation: previous.generation,
        })
    }
}
