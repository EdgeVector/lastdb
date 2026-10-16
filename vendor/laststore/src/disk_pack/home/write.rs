use super::{DiskHome, HomeDescriptor};
use crate::disk_pack::journal::{
    DurableBatch, JournalMutation, JournalMutationShape, JournalPressure, JournalQuote,
    JournalReservation, JournalWriter,
};
use crate::disk_pack::{Current, PublishedGeneration};
use crate::Result;

/// Complete native stored mutation. The home derives its physical group.
pub struct HomeMutation {
    /// Exact namespace identity, never a filesystem path.
    pub collection: String,
    /// Complete encoded LastStore document id.
    pub key: String,
    /// Opaque stored body, or None for a tombstone.
    pub body: Option<Vec<u8>>,
}

/// Exclusive native journal owner. A durable append is distinct from root publication.
/// This API does not acknowledge a logical memory mutation or release its token.
pub struct HomeJournal {
    pub(super) descriptor: HomeDescriptor,
    pub(super) writer: JournalWriter,
}

impl HomeDescriptor {
    /// Derive the native group before a journal-only capacity calculation.
    pub fn mutation_shape(
        &self,
        collection: &str,
        key: &str,
        body_bytes: Option<u64>,
    ) -> Result<JournalMutationShape> {
        Ok(JournalMutationShape::new(
            collection.len(),
            self.group(key)?,
            key.len(),
            body_bytes,
        ))
    }

    /// Quote a complete native batch without copying or retaining its bodies.
    pub fn quote(&self, operations: &[HomeMutation]) -> Result<JournalQuote> {
        if operations.is_empty() || operations.len() > 4096 {
            return Err(crate::Error::Config(
                "native batch requires 1..4096 operations".into(),
            ));
        }
        let mut shapes = Vec::new();
        shapes
            .try_reserve_exact(operations.len())
            .map_err(|_| super::super::resource())?;
        for operation in operations {
            shapes.push(self.mutation_shape(
                &operation.collection,
                &operation.key,
                operation.body.as_ref().map(|body| body.len() as u64),
            )?);
        }
        JournalQuote::from_shapes(&shapes)
    }
}

impl DiskHome {
    /// Recover complete pending work and checkpoint a fresh stream before admission.
    pub fn journal(&self) -> Result<HomeJournal> {
        let on_disk = HomeDescriptor::read(&self.root)?;
        if on_disk != self.descriptor {
            return Err(crate::disk_pack::corrupt(
                "native descriptor changed before writer admission",
            ));
        }
        let writer = JournalWriter::open(&self.root)?;
        self.descriptor.check_current(writer.current())?;
        Ok(HomeJournal {
            descriptor: self.descriptor.clone(),
            writer,
        })
    }
}

impl HomeJournal {
    /// Last complete readable directory boundary.
    pub fn current(&self) -> &Current {
        self.writer.current()
    }

    /// Own journal capacity before the batch transfers its stored bodies.
    /// This does not reserve directory space or authorize a memory ack.
    pub fn reserve(&self, operations: &[HomeMutation]) -> Result<JournalReservation> {
        self.writer
            .reserve_quote(self.descriptor.quote(operations)?)
    }

    /// Include unappended reservations in the bounded suffix pressure.
    pub fn pressure(&self) -> Result<JournalPressure> {
        self.writer.pressure()
    }

    /// Make one complete batch recoverable. It becomes readable only at publication.
    pub fn append(
        &mut self,
        operations: Vec<HomeMutation>,
        accepted_ms: u64,
    ) -> Result<DurableBatch> {
        let reservation = self.reserve(&operations)?;
        self.append_reserved(operations, accepted_ms, reservation)
    }

    /// Consume only capacity owned by this exact writer instance.
    pub fn append_reserved(
        &mut self,
        operations: Vec<HomeMutation>,
        accepted_ms: u64,
        reservation: JournalReservation,
    ) -> Result<DurableBatch> {
        if operations.is_empty() || operations.len() > 4096 {
            return Err(crate::Error::Config(
                "native batch requires 1..4096 operations".into(),
            ));
        }
        let addressed: Result<Vec<_>> = operations
            .into_iter()
            .map(|operation| {
                Ok(JournalMutation {
                    group: self.descriptor.group(&operation.key)?,
                    collection: operation.collection,
                    key: operation.key,
                    body: operation.body,
                })
            })
            .collect();
        self.writer
            .append_reserved(addressed?, accepted_ms, reservation)
    }

    /// Commit the complete directory boundary before a caller releases write protection.
    pub fn publish(&mut self) -> Result<PublishedGeneration> {
        self.writer.publish()
    }
}
