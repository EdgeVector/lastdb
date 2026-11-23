//! Signed-log replay clock, the stamps of local writes, and schema-level replay coordination.

mod records;
mod stream;

pub use records::ReplayAttachmentSlots;

use super::{AuthoredMutation, MutationManager};
use crate::platform::clock;
use crate::schema::types::{KeyValue, Mutation, MutationType};
use crate::schema::SchemaError;
use crate::storage::disk_pack::transaction;
use crate::storage::format::metadata::{ReplayStamp, ReplayTarget, FILE_SLOT, INLINE_SLOT};
use crate::storage::format::records::RecordAddress;
use lastdb_model::record_value::StoredRecord;
use std::sync::Arc;

const NANOS_PER_MILLI: u64 = 1_000_000;

impl MutationManager {
    /// Raise the durable author clock past a verified cloud ordering time.
    pub async fn observe_replay_time(&self, ordering_ms: u64) -> Result<(), SchemaError> {
        self.db_ops.metadata().require_replay_scope()?;
        let floor = ordering_ms
            .checked_add(1)
            .and_then(|milliseconds| milliseconds.checked_mul(1_000_000))
            .ok_or_else(|| SchemaError::InvalidData("replay author clock exhausted".into()))?;
        let metadata = self.db_ops.metadata();
        let mut state = metadata
            .author_clock(&self.writer_id)
            .await?
            .unwrap_or_default();
        if state.physical_nanos < floor {
            state.observe_remote(floor, 0);
            metadata.stage_author_clock(&self.writer_id, &state).await?;
        }
        Ok(())
    }

    /// Give each mutation its author time. A captured write takes a
    /// millisecond of its own, because that millisecond is its order value in
    /// the signed log and the order values of one device rise with every entry.
    pub(super) async fn prepare_mutation_author_clocks(
        &self,
        mutations: &mut [Mutation],
        captured: bool,
    ) -> Result<u64, SchemaError> {
        let exhausted = || SchemaError::InvalidData("mutation author-clock range exhausted".into());
        let mut state = self
            .db_ops
            .metadata()
            .author_clock(&self.writer_id)
            .await?
            .unwrap_or_default();
        let counter = if captured {
            let (first, counter) = state
                .advance_local_batch_spaced(clock::unix_nanos(), mutations.len())
                .ok_or_else(exhausted)?;
            for (m, step) in mutations.iter_mut().zip(0u64..) {
                m.imported_written_at = Some(first + step * NANOS_PER_MILLI);
            }
            counter
        } else {
            let (mut times, counter) = state
                .advance_local_batch(clock::unix_nanos(), mutations.len())
                .ok_or_else(exhausted)?;
            for m in mutations {
                m.imported_written_at = Some(times.next().expect("reserved time"));
            }
            counter
        };
        self.db_ops
            .metadata()
            .stage_author_clock(&self.writer_id, &state)
            .await?;
        Ok(counter)
    }

    /// The stamp each write has in the signed log, in commit order. The spool
    /// takes one entry for each mutation in the same order, so the sequence
    /// numbers here are the ones the sender signs.
    pub(super) async fn local_replay_stamps(
        &self,
        mutations: &[Mutation],
        captured: bool,
    ) -> Result<Vec<Option<ReplayStamp>>, SchemaError> {
        if !captured {
            return Ok(vec![None; mutations.len()]);
        }
        let orders = mutations
            .iter()
            .map(|m| {
                m.imported_written_at
                    .map(|nanos| nanos / NANOS_PER_MILLI)
                    .ok_or_else(|| {
                        SchemaError::InvalidData("a captured write has no author time".into())
                    })
            })
            .collect::<Result<Vec<u64>, _>>()?;
        let stamps = self
            .db_ops
            .metadata()
            .stage_local_replay_stamps(&self.writer_id, &orders)
            .await?;
        Ok(stamps.into_iter().map(Some).collect())
    }

    /// Write the stamp rows of each captured write of a commit, in the
    /// transaction of the commit. The write is the newest of its record from
    /// this moment: an entry of another device is compared with these rows
    /// when it arrives, and nothing about the write changes when it is sent.
    ///
    /// A delete is the newest write of the fields, the file and the inline
    /// file. Any other write is the newest of the fields, and of the file or
    /// the inline file only when it sets or clears that part.
    pub(super) async fn stamp_local_records(
        &self,
        addresses: &[RecordAddress],
        mutations: &[AuthoredMutation],
    ) -> Result<(), SchemaError> {
        let mut rows = Vec::new();
        for (address, item) in addresses.iter().zip(mutations) {
            let Some(stamp) = &item.replay else {
                continue;
            };
            let target = ReplayTarget::Record(address.as_str());
            let slots = slots_of(&item.mutation);
            rows.push((target, None, stamp));
            if slots.file {
                rows.push((target, Some(FILE_SLOT), stamp));
            }
            if slots.inline {
                rows.push((target, Some(INLINE_SLOT), stamp));
            }
        }
        if rows.is_empty() {
            return Ok(());
        }
        self.db_ops.metadata().stage_replay_stamps(rows).await
    }

    /// Stable address of one record in this store.
    pub fn record_storage_address(
        &self,
        schema: &str,
        key: &KeyValue,
    ) -> Result<String, SchemaError> {
        self.replay_record_address(schema, key)
    }

    /// Greatest applied schema put or drop stamp in the current replay scope.
    pub async fn replay_schema_winner(
        &self,
        schema: &str,
    ) -> Result<Option<ReplayStamp>, SchemaError> {
        self.db_ops
            .metadata()
            .replay_winner(ReplayTarget::Schema(schema))
            .await
    }

    /// Read the greatest applied schema put or drop stamp in a fresh read scope.
    pub async fn read_replay_schema_winner(
        &self,
        schema: &str,
    ) -> Result<Option<ReplayStamp>, SchemaError> {
        transaction::scope(
            self.db_ops.native_runtime(),
            self.replay_schema_winner(schema),
        )
        .await
    }

    /// Stage a greater schema put or drop stamp in the current replay scope.
    pub async fn accept_replay_schema(
        &self,
        schema: &str,
        stamp: &ReplayStamp,
    ) -> Result<bool, SchemaError> {
        self.db_ops
            .metadata()
            .accept_replay(ReplayTarget::Schema(schema), stamp)
            .await
    }

    /// True when `schema` is absent because a newer schema drop already removed it.
    pub async fn replay_schema_dropped_after(
        &self,
        schema: &str,
        stamp: &ReplayStamp,
    ) -> Result<bool, SchemaError> {
        let metadata = self.db_ops.metadata();
        metadata.require_replay_scope()?;
        let schemas = self.db_ops.schemas();
        if schemas.get_schema_state(schema).await?.is_some()
            || schemas.get_schema(schema).await?.is_some()
        {
            return Ok(false);
        }
        Ok(metadata
            .replay_winner(ReplayTarget::Schema(schema))
            .await?
            .is_some_and(|winner| &winner > stamp))
    }

    pub(super) fn replay_record_address(
        &self,
        schema: &str,
        key: &KeyValue,
    ) -> Result<String, SchemaError> {
        let schema = self.schema_manager.require_schema_metadata(schema)?;
        let key = Self::canonical_primary_key(&schema, key)?;
        self.db_ops.records().record_key(&schema, &key)
    }

    pub(super) async fn replay_preimage(
        &self,
        address: String,
    ) -> Result<Option<Arc<StoredRecord>>, SchemaError> {
        let [before]: [_; 1] = self
            .db_ops
            .records()
            .read_records(&[address])
            .await?
            .try_into()
            .map_err(|_| SchemaError::InvalidData("incomplete replay preimage read".into()))?;
        Ok(before)
    }
}

/// The parts of a record besides its fields that a write is the newest write of.
pub(super) fn slots_of(mutation: &Mutation) -> ReplayAttachmentSlots {
    let deletes = matches!(
        mutation.mutation_type,
        MutationType::Delete | MutationType::Purge
    );
    ReplayAttachmentSlots {
        file: deletes || mutation.file_write.is_some(),
        inline: deletes || mutation.inline_write.is_some(),
    }
}
