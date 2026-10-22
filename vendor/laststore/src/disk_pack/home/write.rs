use super::{DiskHome, HomeDescriptor};
use crate::disk_pack::snapshot::{
    SnapshotMutation, SnapshotMutationShape, SnapshotPressure, SnapshotQuote, SnapshotReservation,
    SnapshotWriter,
};
use crate::disk_pack::Current;
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

/// Exclusive native snapshot owner. Each stored batch publishes one complete root.
/// This API does not acknowledge a logical memory mutation or release its token.
pub struct HomeSnapshot {
    pub(super) descriptor: HomeDescriptor,
    pub(super) writer: SnapshotWriter,
}

impl HomeDescriptor {
    /// Derive the native group before a snapshot-only capacity calculation.
    pub fn mutation_shape(
        &self,
        collection: &str,
        key: &str,
        body_bytes: Option<u64>,
    ) -> Result<SnapshotMutationShape> {
        Ok(SnapshotMutationShape::new(
            collection.len(),
            self.group(key)?,
            key.len(),
            body_bytes,
        ))
    }

    /// Quote a complete native batch without copying or retaining its bodies.
    pub fn quote(&self, operations: &[HomeMutation]) -> Result<SnapshotQuote> {
        self.quote_with_limit(operations, crate::disk_pack::LEGACY_BATCH_OPERATIONS)
    }

    pub(super) fn quote_with_limit(
        &self,
        operations: &[HomeMutation],
        operation_limit: usize,
    ) -> Result<SnapshotQuote> {
        if operations.is_empty() || operations.len() > operation_limit {
            return Err(crate::Error::Config(format!(
                "native batch requires 1..{operation_limit} operations"
            )));
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
        SnapshotQuote::from_iter_with_limit(shapes, operation_limit)
    }
}

impl DiskHome {
    pub(crate) fn snapshot_conversion(&self, token: uuid::Uuid) -> Result<HomeSnapshot> {
        let writer = SnapshotWriter::open_conversion(&self.root, token)?;
        self.descriptor.check_current(writer.current())?;
        Ok(HomeSnapshot {
            descriptor: self.descriptor.clone(),
            writer,
        })
    }

    /// Open the selected complete snapshot without mutation replay.
    pub fn snapshot(&self) -> Result<HomeSnapshot> {
        if self
            .root
            .join(crate::disk_pack::conversion::REVERSE_FENCE)
            .try_exists()?
        {
            return Err(crate::Error::Config(
                "native source is sealed after reverse conversion".into(),
            ));
        }
        let on_disk = HomeDescriptor::read(&self.root)?;
        if on_disk != self.descriptor {
            return Err(crate::disk_pack::corrupt(
                "native descriptor changed before writer admission",
            ));
        }
        let writer = SnapshotWriter::open(&self.root)?;
        self.descriptor.check_current(writer.current())?;
        Ok(HomeSnapshot {
            descriptor: self.descriptor.clone(),
            writer,
        })
    }
}

impl HomeSnapshot {
    /// Pack live records without a logical mutation; retain held and grace-period files.
    pub fn compact(
        &mut self,
        policy: &crate::disk_pack::maintenance::SpacePolicy,
    ) -> Result<crate::disk_pack::maintenance::MaintenanceReceipt> {
        self.writer.compact(policy)
    }

    /// Reclaim only the exact files no retained generation still needs.
    pub fn reclaim(
        &mut self,
        policy: &crate::disk_pack::maintenance::SpacePolicy,
    ) -> Result<crate::disk_pack::maintenance::MaintenanceReceipt> {
        self.writer.reclaim(policy)
    }

    /// Remove part files that no receipt names; files younger than the grace period stay.
    pub fn reclaim_orphans(
        &mut self,
        policy: &crate::disk_pack::maintenance::SpacePolicy,
    ) -> Result<crate::disk_pack::maintenance::MaintenanceReceipt> {
        self.writer.reclaim_orphans(policy)
    }

    /// Reserve all native output bytes under the same exclusive writer lease.
    pub fn reserve_space(
        &self,
        bytes: u64,
        policy: &crate::disk_pack::maintenance::SpacePolicy,
    ) -> Result<crate::disk_pack::maintenance::SpaceReservation> {
        self.writer.reserve_space(bytes, policy)
    }

    /// Last complete readable directory boundary.
    pub fn current(&self) -> &Current {
        self.writer.current()
    }

    /// Dependency parts named by the receipt of `current` (limit: `MAX_DEPENDENCY_PARTS`).
    pub fn dependency_parts(&self) -> u64 {
        self.writer.dependency_parts()
    }

    /// True when a failed publication or maintenance requires a new writer.
    pub fn is_poisoned(&self) -> bool {
        self.writer.is_poisoned()
    }

    /// Own snapshot capacity before the batch transfers its stored bodies.
    /// This does not reserve directory space or authorize a memory ack.
    pub fn reserve(&self, operations: &[HomeMutation]) -> Result<SnapshotReservation> {
        self.writer.reserve_quote(
            self.descriptor
                .quote_with_limit(operations, self.current().batch_operation_limit())?,
        )
    }

    /// Include unappended reservations in the bounded suffix pressure.
    pub fn pressure(&self) -> Result<SnapshotPressure> {
        self.writer.pressure()
    }

    pub(super) fn address(&self, operations: Vec<HomeMutation>) -> Result<Vec<SnapshotMutation>> {
        operations
            .into_iter()
            .map(|operation| {
                Ok(SnapshotMutation {
                    group: self.descriptor.group(&operation.key)?,
                    collection: operation.collection,
                    key: operation.key,
                    body: operation.body,
                })
            })
            .collect()
    }
}
