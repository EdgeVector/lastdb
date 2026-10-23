//! Owned payload and map capacity before the complete native snapshot commit.

use super::{HomeMutation, HomeSnapshot, MutationQuote};
use crate::disk_pack::corrupt;
use crate::disk_pack::snapshot::SnapshotReservation;
use crate::Result;

/// Non-cloneable complete store quota bound to one logical frontier.
/// An equivalent compaction may replace physical addresses before commit.
/// Physical space and protected logical records remain separate owned tokens.
pub struct MutationReservation {
    payload: SnapshotReservation,
    quote: MutationQuote,
    frontier: u64,
    required_features: u64,
}

impl MutationReservation {
    /// Complete dimensions held by this ticket.
    pub fn quote(&self) -> MutationQuote {
        self.quote
    }

    /// Transfer the admitted prefix after the caller accepts its extension.
    /// Preparation and transfer keep the same exclusive native write gate.
    pub fn absorb(&mut self, previous: MutationReservation) {
        assert_eq!(self.frontier, previous.frontier);
        assert_eq!(self.required_features, previous.required_features);
        self.payload.absorb(previous.payload);
    }
}

impl HomeSnapshot {
    /// Publish one complete immutable snapshot before durable success.
    pub fn commit_mutation(
        &mut self,
        operations: Vec<HomeMutation>,
        reservation: MutationReservation,
    ) -> Result<crate::disk_pack::PublishedGeneration> {
        if reservation.frontier != self.current().frontier
            || reservation.required_features != self.current().required_features
        {
            return Err(corrupt(
                "complete mutation reservation logical root changed",
            ));
        }
        self.writer
            .check_dependency_capacity(reservation.quote.dependency_parts())?;
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
            frontier: self.current().frontier,
            required_features: self.current().required_features,
        })
    }

    /// Reserve a replacement prefix using only the new request's dimensions.
    /// The caller retains the previous ticket until complete admission succeeds,
    /// then absorbs it into the returned ticket before commit. Cancellation
    /// releases only the added charge and leaves the previous ticket intact.
    pub fn reserve_mutation_extension(
        &self,
        previous: &MutationReservation,
        incoming: &[HomeMutation],
    ) -> Result<MutationReservation> {
        if previous.frontier != self.current().frontier
            || previous.required_features != self.current().required_features
        {
            return Err(corrupt(
                "complete mutation reservation logical root changed",
            ));
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
            frontier: previous.frontier,
            required_features: previous.required_features,
        })
    }
}
