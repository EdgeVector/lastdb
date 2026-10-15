use super::{DiskHome, HomeDescriptor};
use crate::disk_pack::journal::{DurableBatch, JournalMutation, JournalWriter};
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
    descriptor: HomeDescriptor,
    writer: JournalWriter,
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

    /// Make one complete batch recoverable. It becomes readable only at publication.
    pub fn append(
        &mut self,
        operations: Vec<HomeMutation>,
        accepted_ms: u64,
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
        self.writer.append(addressed?, accepted_ms)
    }

    /// Commit the complete directory boundary before a caller releases write protection.
    pub fn publish(&mut self) -> Result<PublishedGeneration> {
        self.writer.publish()
    }
}
