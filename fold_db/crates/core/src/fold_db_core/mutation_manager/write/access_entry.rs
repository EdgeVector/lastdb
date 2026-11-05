//! Moved verbatim out of the parent module; see the parent for context.

use super::*;
use crate::schema::types::MutationType;
use lastdb_model::portable_mutation::{Position, StreamId};
use sha2::{Digest, Sha256};

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
        let imports = mutations
            .iter()
            .map(legacy_import_stamp)
            .collect::<Result<Vec<_>, _>>()?;
        let wait_for_author_clock = mutations
            .iter()
            .any(|mutation| mutation.synchronous == Some(true));
        let author_clock_persist = self.prepare_mutation_author_clocks(&mut mutations, now)?;
        // Submit before resident apply. Each data envelope waits on a clone of
        // this barrier, so durable LWW data cannot pass its durable clock.
        let inline_clock = mutations.iter().all(|mutation| {
            matches!(
                mutation.mutation_type,
                MutationType::Create | MutationType::Replace
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
        let revision = imports
            .iter()
            .zip(mutations.iter())
            .find(|(imported, _)| imported.is_none())
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
            .write_mutations_batch_inner_with_clock(mutations, author_clock_barrier, imports)
            .await?;
        receipt.revision = revision;
        Ok(receipt)
    }
}

/// A legacy import has an authenticated timestamp and mutation identity but
/// no portable source sequence. Give it a separate, deterministic position.
fn legacy_import_stamp(mutation: &Mutation) -> Result<Option<Stamp>, SchemaError> {
    let Some(nanos) = mutation.imported_written_at else {
        return Ok(None);
    };
    let mut digest = Sha256::new();
    digest.update(b"lastdb:legacy-record-import:v1\0");
    digest.update(mutation.content_hash());
    let bytes: [u8; 16] = digest.finalize()[..16].try_into().expect("digest prefix");
    let source_stream = StreamId::try_from(uuid::Uuid::from_bytes(bytes).to_string())
        .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
    Ok(Some(Stamp {
        authored_at_ms: nanos / 1_000_000,
        position: Position {
            source_stream,
            sequence: 1,
        },
    }))
}
