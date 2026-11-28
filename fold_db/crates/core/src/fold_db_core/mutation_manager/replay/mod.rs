//! Signed-log replay clock, the stamps of local writes, and schema-level replay coordination.

mod records;
mod saved_fields;
mod stream;
mod winner;

pub use records::ReplayAttachmentSlots;

use super::MutationManager;
use crate::platform::clock;
use crate::schema::types::author_clock::MAX_ORDERING_MS;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Mutation, MutationType, Schema};
use crate::schema::SchemaError;
use crate::storage::disk_backup::accepted::LocalEntry;
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
        let Some(next_ms) = ordering_ms
            .checked_add(1)
            .filter(|&next| next < MAX_ORDERING_MS)
        else {
            return Ok(());
        };
        let floor = next_ms * NANOS_PER_MILLI;
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
    /// millisecond of its own, floored above `previous.order` and below
    /// `MAX_ORDERING_MS`, before staging the updated clock.
    pub(super) async fn prepare_mutation_author_clocks(
        &self,
        mutations: &mut [Mutation],
        previous: Option<&LocalEntry>,
        captured: bool,
    ) -> Result<(), SchemaError> {
        let exhausted = || SchemaError::InvalidData("mutation author-clock range exhausted".into());
        let mut state = self
            .db_ops
            .metadata()
            .author_clock(&self.writer_id)
            .await?
            .unwrap_or_default();
        if captured {
            if let Some(prev) = previous {
                state.floor_above_order(prev.order).ok_or_else(exhausted)?;
            }
            let (first, _) = state
                .advance_local_batch_spaced(clock::unix_nanos(), mutations.len())
                .ok_or_else(exhausted)?;
            for (m, step) in mutations.iter_mut().zip(0u64..) {
                m.imported_written_at = Some(first + step * NANOS_PER_MILLI);
            }
        } else {
            let (mut times, _) = state
                .advance_local_batch(clock::unix_nanos(), mutations.len())
                .ok_or_else(exhausted)?;
            for m in mutations {
                let reserved = times.next().expect("reserved time");
                if m.imported_written_at.is_none() {
                    m.imported_written_at = Some(reserved);
                }
            }
        }
        self.db_ops
            .metadata()
            .stage_author_clock(&self.writer_id, &state)
            .await
    }

    /// The stamp each write has in the signed log, in commit order.
    pub(super) fn local_replay_stamps(
        &self,
        mutations: &[Mutation],
        previous: Option<&LocalEntry>,
        captured: bool,
    ) -> Result<Vec<Option<ReplayStamp>>, SchemaError> {
        if !captured {
            return Ok(vec![None; mutations.len()]);
        }
        let metadata = self.db_ops.metadata();
        let mut seq = previous.map_or(0, |held| held.seq);
        let mut stamps = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            let order = mutation
                .imported_written_at
                .map(|nanos| nanos / NANOS_PER_MILLI)
                .ok_or_else(|| {
                    SchemaError::InvalidData("a captured write has no author time".into())
                })?;
            seq += 1;
            stamps.push(Some(metadata.local_replay_stamp(
                &self.writer_id,
                seq,
                order,
            )?));
        }
        Ok(stamps)
    }

    /// Write the stamp rows of each captured write of a commit, in the
    /// transaction of the commit.
    pub(in crate::fold_db_core::mutation_manager) async fn stamp_local_records(
        &self,
        schema: &Schema,
        addresses: &[RecordAddress],
        local_replays: &[(Option<ReplayStamp>, ReplayAttachmentSlots)],
    ) -> Result<(), SchemaError> {
        let metadata = self.db_ops.metadata();
        let mut rows = Vec::new();
        for (address, (replay, slots)) in addresses.iter().zip(local_replays) {
            let Some(stamp) = replay else {
                continue;
            };
            let target = ReplayTarget::Record(address.as_str());
            rows.push((target, None, stamp));
            if slots.file {
                rows.push((target, Some(FILE_SLOT), stamp));
            }
            if slots.inline {
                rows.push((target, Some(INLINE_SLOT), stamp));
            }
            if schema.file_required {
                metadata
                    .stage_saved_replay_fields(address.as_str(), None)
                    .await?;
            }
        }
        if rows.is_empty() {
            return Ok(());
        }
        metadata.stage_replay_stamps(rows).await
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

    /// Greatest applied schema drop or replacement floor in the current replay scope.
    pub async fn replay_schema_drop_winner(
        &self,
        schema: &str,
    ) -> Result<Option<ReplayStamp>, SchemaError> {
        self.db_ops.metadata().replay_drop_winner(schema).await
    }

    /// Stage a greater schema drop stamp in the current replay scope.
    pub async fn accept_replay_drop_schema(
        &self,
        schema: &str,
        stamp: &ReplayStamp,
    ) -> Result<bool, SchemaError> {
        self.db_ops
            .metadata()
            .accept_replay_drop_schema(schema, stamp)
            .await
    }

    /// Reconcile stored records of `schema_name` against a raised `drop-winner` floor
    /// in bounded transactions, removing records and attachments at or below `floor`
    /// while preserving writes strictly above `floor`.
    pub async fn reconcile_replay_schema_floor(
        &self,
        schema_name: &str,
        floor: &ReplayStamp,
    ) -> Result<(), SchemaError> {
        let runtime = self.db_ops.native_runtime();
        let Some(schema) = transaction::scope(
            Arc::clone(&runtime),
            self.resolve_replay_schema(schema_name),
        )
        .await?
        else {
            return Ok(());
        };
        let records = self.db_ops.records();
        let cmp = records.key_comparison_context(&schema)?;
        let ranged = schema.schema_type == DeclarativeSchemaType::HashRange;
        let mut cursor: Option<String> = None;
        loop {
            let (pairs, next, has_more) = self
                .db_ops
                .read_scope()
                .await?
                .run_result(records.list_record_keys(&schema, &cmp, 64, cursor.as_deref(), None))
                .await?;
            if !pairs.is_empty() {
                runtime
                    .replay_accepted(transaction::scope(Arc::clone(&runtime), async {
                        let Some(live_schema) = self.resolve_replay_schema(schema_name).await?
                        else {
                            return Ok(());
                        };
                        for (hash, range) in &pairs {
                            let key =
                                KeyValue::new(Some(hash.clone()), ranged.then(|| range.clone()));
                            let Ok(address) = self.replay_record_address(&live_schema, &key) else {
                                continue;
                            };
                            let del = saved_fields::as_mutation(&live_schema.name, key, None);
                            self.merge_validated_record(&live_schema, &address, del, floor)
                                .await?;
                        }
                        Ok::<(), SchemaError>(())
                    }))
                    .await?;
            }
            if !has_more {
                break;
            }
            cursor = next;
        }
        Ok(())
    }

    /// True when `schema` is absent because a schema drop is its current winning state.
    pub async fn is_replay_schema_dropped(&self, schema: &str) -> Result<bool, SchemaError> {
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
            .is_some())
    }

    pub(super) fn replay_record_address(
        &self,
        schema: &Schema,
        key: &KeyValue,
    ) -> Result<String, SchemaError> {
        let key = Self::canonical_primary_key(schema, key)?;
        self.db_ops.records().record_key(schema, &key)
    }

    pub(super) async fn replay_preimage(
        &self,
        address: &str,
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

/// A delete or purge clears both attachment slots. A file write moves only
/// `FILE_SLOT`; an inline write moves only `INLINE_SLOT`.
pub(in crate::fold_db_core::mutation_manager) fn slots_from(
    mutation_type: MutationType,
    has_file_write: bool,
    has_inline_write: bool,
) -> ReplayAttachmentSlots {
    let clears = matches!(mutation_type, MutationType::Delete | MutationType::Purge);
    ReplayAttachmentSlots {
        file: clears || has_file_write,
        inline: clears || has_inline_write,
    }
}

pub(super) fn slots_of(mutation: &Mutation) -> ReplayAttachmentSlots {
    slots_from(
        mutation.mutation_type,
        mutation.file_write.is_some(),
        mutation.inline_write.is_some(),
    )
}
