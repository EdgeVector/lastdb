//! One private/accepted/typed-core/disk view, with no retained encoded values.

use super::*;
use transaction::{PendingBatch, PendingRecord};

mod facts;
mod stats;
mod values;
pub(super) use stats::{WriteView, WriteViewCounts};

pub(super) type Coordinate = (String, String);
pub(super) type Observation<'a> = (&'a Coordinate, Option<&'a [u8]>);

#[derive(Clone, Copy)]
pub(super) enum WriteOwner {
    Fact,
    Tip,
    Atom,
    Definition,
}

pub(super) enum WriteRecord {
    Overlay(Arc<PendingRecord>),
    /// These bytes belong only to the result, never to the shared warm set.
    Core {
        body: Option<Vec<u8>>,
        owner: WriteOwner,
    },
}

impl PackRuntime {
    /// Borrow typed owners under one publication/core lock per batch. Account
    /// the result before its temporary serialization allocation.
    pub(super) fn write_view_records(
        &self,
        context: &ReadContext,
        call: &mut PhysicalRead,
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
                *selected = values::read(&mut core, context, call, coordinate)?;
            }
        }
        Ok(records)
    }

    /// Presence here is only a hint to the paired probe. The consuming raw read
    /// selects and charges the actual result through the common view.
    pub(super) fn known_write_counters(
        &self,
        context: &ReadContext,
        pending: &Arc<PendingBatch>,
        coordinates: &[Coordinate],
    ) -> StorageResult<Vec<bool>> {
        let mut known: Vec<_> = coordinates
            .iter()
            .map(|(collection, key)| {
                context
                    .overlay_record(Some(pending), collection, key)
                    .is_some()
            })
            .collect();
        let view = self.view.read().map_err(|_| unavailable())?;
        if view.revision != context.revision {
            return Ok(known);
        }
        let mut core = self.core.lock().map_err(|_| unavailable())?;
        for (coordinate, known) in coordinates.iter().zip(&mut known) {
            *known |= facts::get(&mut core, coordinate).is_some();
        }
        Ok(known)
    }

    /// Only successful observations from this call's immutable root qualify.
    /// Present atoms and metadata are never retained again as encoded bytes.
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
        for (coordinate, body) in observations {
            facts::retain(&mut core, coordinate, *body);
        }
        context.note_write_view_offers(observations)
    }

    /// The caller holds the publication write lock after typed publication.
    /// Bind exact tip sources and retain typed counters/absences only.
    pub(in crate::storage::disk_pack) fn publish_write_view(
        &self,
        pending: &PendingBatch,
    ) -> StorageResult<()> {
        let records = pending.final_records();
        let mut core = self.core.lock().map_err(|_| unavailable())?;
        for record in records {
            facts::retain(
                &mut core,
                &(record.collection.clone(), record.key.clone()),
                record.body.as_deref(),
            );
        }
        Ok(())
    }
}

fn unavailable() -> StorageError {
    StorageError::BackendError("common write view memory lock is unavailable".into())
}
