//! One private/accepted/core/disk view for every exact preparation read.
//!
//! The core metadata owner retains exact stored records, including absences.
//! A view never reads a shared entry from another logical revision. Private
//! and accepted writes always precede the core. No scope-local value map exists.

use super::*;
use crate::resident::{CoreMemorySource, CoreMemoryValue, CoreWarmSet};
use transaction::{PendingBatch, PendingRecord};

mod stats;
pub(super) use stats::{WriteView, WriteViewCounts};

pub(super) type Coordinate = (String, String);
pub(super) type Observation<'a> = (&'a Coordinate, Option<&'a [u8]>);
type Record = Arc<PendingRecord>;
static RECORDS: u8 = 0;
const MAX_VALUE_BYTES: usize = 16 * 1024;

/// A shared answer keeps its byte charge alive until the caller copies it.
pub(super) enum WriteRecord {
    Overlay(Record),
    Core(Arc<CoreMemoryValue<Record>>),
}

impl WriteRecord {
    pub(super) fn record(&self) -> &PendingRecord {
        match self {
            Self::Overlay(record) => record,
            Self::Core(record) => record,
        }
    }

    pub(super) fn is_core(&self) -> bool {
        matches!(self, Self::Core(_))
    }
}

impl PackRuntime {
    /// Resolve a complete batch with one publication lock and one core lock.
    pub(super) fn write_view_records(
        &self,
        context: &ReadContext,
        pending: Option<&Arc<PendingBatch>>,
        coordinates: &[Coordinate],
    ) -> StorageResult<Vec<Option<WriteRecord>>> {
        let mut records: Vec<_> = coordinates
            .iter()
            .map(|(collection, key)| {
                context
                    .overlay_record(pending, collection, key)
                    .map(WriteRecord::Overlay)
            })
            .collect();
        if context.write_view.is_none() {
            return Ok(records);
        }
        let view = self.view.read().map_err(|_| unavailable())?;
        if view.revision != context.revision {
            return Ok(records);
        }
        let mut core = self.core.lock().map_err(|_| unavailable())?;
        for (coordinate, selected) in coordinates.iter().zip(&mut records) {
            if selected.is_none() {
                *selected = core
                    .memory_get(record_owner(), &record_name(coordinate))
                    .map(WriteRecord::Core);
            }
        }
        Ok(records)
    }

    /// Only successful results from this view's fixed disk root reach here.
    pub(super) fn remember_write_view(
        &self,
        context: &ReadContext,
        observations: &[Observation<'_>],
    ) -> StorageResult<()> {
        if context.write_view.is_none() || observations.is_empty() {
            return Ok(());
        }
        let view = self.view.read().map_err(|_| unavailable())?;
        if view.revision != context.revision {
            return Ok(());
        }
        let mut core = self.core.lock().map_err(|_| unavailable())?;
        for ((collection, key), body) in observations {
            if collection == super::super::physical::CAS_COLLECTION
                || body.map_or(0, <[u8]>::len) > MAX_VALUE_BYTES
            {
                continue;
            }
            retain_record(
                &mut core,
                Arc::new(PendingRecord {
                    collection: collection.clone(),
                    key: key.clone(),
                    body: body.map(<[u8]>::to_vec),
                }),
            );
        }
        context.note_write_view_offers(observations)
    }

    /// The caller holds the publication write lock after the disk commit.
    /// Retain the existing staged owner, rather than copying prepared bodies.
    pub(in crate::storage::disk_pack) fn publish_write_view(
        &self,
        pending: &PendingBatch,
    ) -> StorageResult<()> {
        let records = pending.final_records();
        let mut core = self.core.lock().map_err(|_| unavailable())?;
        for record in records {
            retain_record(&mut core, record);
        }
        Ok(())
    }
}

fn retain_record(core: &mut CoreWarmSet, record: Record) {
    let body_bytes = record.body.as_ref().map_or(0, Vec::len);
    // File contents stay outside core memory. Large records use the disk route.
    if record.collection == super::super::physical::CAS_COLLECTION || body_bytes > MAX_VALUE_BYTES {
        return;
    }
    let coordinate = (record.collection.clone(), record.key.clone());
    let name = record_name(&coordinate);
    if core.memory_get::<Record>(record_owner(), &name).is_some() {
        return;
    }
    let bytes = record.body.as_ref().map_or(0, Vec::capacity)
        + record.collection.capacity()
        + record.key.capacity()
        + std::mem::size_of::<PendingRecord>();
    core.memory_insert(
        record_owner(),
        name,
        record,
        vec![CoreMemorySource {
            collection: coordinate.0,
            key: Some(coordinate.1),
        }],
        bytes,
    );
}

fn record_owner() -> usize {
    std::ptr::from_ref(&RECORDS) as usize
}

fn record_name(coordinate: &Coordinate) -> String {
    serde_json::to_string(coordinate).expect("exact stored record coordinate")
}

fn unavailable() -> StorageError {
    StorageError::BackendError("common write view memory lock is unavailable".into())
}
