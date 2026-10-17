//! Owned journal and map capacity before the complete native append.

use super::{HomeJournal, HomeMutation, MutationQuote};
use crate::disk_pack::corrupt;
use crate::disk_pack::journal::{DurableBatch, JournalReservation};
use crate::Result;

/// Non-cloneable complete store quota. The exclusive native write gate must
/// remain owned until the corresponding complete root is published.
/// Physical space and protected logical records remain separate owned tokens.
pub struct MutationReservation {
    journal: JournalReservation,
    quote: MutationQuote,
    generation: u64,
}

impl MutationReservation {
    /// Complete dimensions held by this ticket.
    pub fn quote(&self) -> MutationQuote {
        self.quote
    }
}

impl HomeJournal {
    /// Complete the reserved stored batch and publish its durable read boundary.
    /// Failure retains the journal's existing uncertain state for recovery.
    pub fn commit_mutation(
        &mut self,
        operations: Vec<HomeMutation>,
        accepted_ms: u64,
        reservation: MutationReservation,
    ) -> Result<crate::disk_pack::PublishedGeneration> {
        self.append_mutation(operations, accepted_ms, reservation)?;
        self.publish()
    }

    /// Reserve journal and directory dependency capacity for all stored parts.
    /// This method does not authorize memory publication or a success ack.
    pub fn reserve_mutation(&self, operations: &[HomeMutation]) -> Result<MutationReservation> {
        let quote = self.descriptor.quote_mutation(operations)?;
        self.writer
            .check_dependency_capacity(quote.dependency_parts())?;
        let journal = self.writer.reserve_quote(quote.journal())?;
        Ok(MutationReservation {
            journal,
            quote,
            generation: self.current().generation,
        })
    }

    /// Consume the complete quota only for the same still-current disk root.
    pub fn append_mutation(
        &mut self,
        operations: Vec<HomeMutation>,
        accepted_ms: u64,
        reservation: MutationReservation,
    ) -> Result<DurableBatch> {
        if reservation.generation != self.current().generation {
            return Err(corrupt("complete mutation reservation root changed"));
        }
        let actual = self.descriptor.quote_mutation(&operations)?;
        if actual.map_pages() > reservation.quote.map_pages()
            || actual.dependency_parts() > reservation.quote.dependency_parts()
            || actual.physical_bytes() > reservation.quote.physical_bytes()
        {
            return Err(corrupt(
                "complete mutation exceeded its admitted dimensions",
            ));
        }
        self.append_reserved(operations, accepted_ms, reservation.journal)
    }
}
