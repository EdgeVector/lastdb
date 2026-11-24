//! The last-writer rule for one record entry of another device.
//!
//! A record has a winner row for its fields and attachment winner rows for its
//! single live attachment (file or inline file). Each part takes the value of
//! its newest write, independent of arrival order, and pre-drop entries at or
//! below a schema's `drop-winner` are ignored.

use super::saved_fields::{
    apply_attachment_patch, as_mutation, has_file, mutation_from_saved, saved_from_live,
    saved_from_mutation,
};
use super::{slots_of, MutationManager};
use crate::schema::types::{
    cas::CasExpectation, FileWrite, KeyValue, Mutation, MutationType, Schema,
};
use crate::schema::{SchemaError, SchemaState};
use crate::storage::disk_pack::transaction;
use crate::storage::format::metadata::{ReplayStamp, ReplayTarget, FILE_SLOT, INLINE_SLOT};

/// The file and the inline file of a record: which of them a write sets or clears.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReplayAttachmentSlots {
    pub file: bool,
    pub inline: bool,
}

impl MutationManager {
    /// True when a remote record entry cannot change the fields or the parts named in `slots`.
    pub async fn is_replay_record_superseded(
        &self,
        schema: &str,
        key: &KeyValue,
        stamp: &ReplayStamp,
        slots: ReplayAttachmentSlots,
    ) -> Result<bool, SchemaError> {
        transaction::scope(self.db_ops.native_runtime(), async {
            let Some(schema_def) = self.resolve_replay_schema(schema).await? else {
                return self.replay_schema_dropped_after(schema, stamp).await;
            };
            let metadata = self.db_ops.metadata();
            if metadata
                .replay_drop_winner(&schema_def.name)
                .await?
                .is_some_and(|drop_stamp| stamp <= &drop_stamp)
            {
                return Ok(true);
            }
            let address = self.replay_record_address(&schema_def, key)?;
            let target = ReplayTarget::Record(&address);
            if metadata
                .replay_winner(target)
                .await?
                .is_none_or(|winner| &winner < stamp)
            {
                return Ok(false);
            }
            let touches = slots.file || slots.inline;
            if touches && self.beats_slot(target, FILE_SLOT, stamp).await? {
                return Ok(false);
            }
            if touches && self.beats_slot(target, INLINE_SLOT, stamp).await? {
                return Ok(false);
            }
            Ok(true)
        })
        .await
    }

    async fn resolve_replay_schema(
        &self,
        schema_name: &str,
    ) -> Result<Option<Schema>, SchemaError> {
        let (schema, state) = self.schema_manager.get_schema_and_state(schema_name)?;
        if state == SchemaState::Blocked {
            return Ok(None);
        }
        if schema.is_some() {
            return Ok(schema);
        }
        if self.db_ops.schemas().get_schema_state(schema_name).await? == Some(SchemaState::Blocked)
        {
            return Ok(None);
        }
        self.db_ops.schemas().get_schema(schema_name).await
    }

    /// Apply the record write of an entry of another device, in the caller's replay scope.
    pub async fn merge_replay_record(
        &self,
        mut mutation: Mutation,
        stamp: &ReplayStamp,
    ) -> Result<(), SchemaError> {
        let metadata = self.db_ops.metadata();
        metadata.require_replay_scope()?;
        if metadata
            .replay_drop_winner(&mutation.schema_name)
            .await?
            .is_some_and(|drop_stamp| stamp <= &drop_stamp)
        {
            return Ok(());
        }
        let schema = self.load_schema_for_delta(&mutation.schema_name).await?;
        let address = self.replay_record_address(&schema, &mutation.key_value)?;
        mutation.imported_written_at = Some(stamp.ordering_ms.saturating_mul(1_000_000));
        let target = ReplayTarget::Record(&address);
        let slots = slots_of(&mutation);
        let newest = metadata
            .replay_winner(target)
            .await?
            .is_none_or(|held| &held < stamp);
        let parts = if newest {
            self.apply_newest(&schema, &address, mutation, stamp)
                .await?;
            metadata.accept_replay(target, stamp).await?;
            slots
        } else {
            let parts = ReplayAttachmentSlots {
                file: slots.file && self.beats_slot(target, FILE_SLOT, stamp).await?,
                inline: slots.inline && self.beats_slot(target, INLINE_SLOT, stamp).await?,
            };
            if parts.file || parts.inline {
                self.apply_older_parts(&schema, &address, mutation, stamp, parts)
                    .await?;
            }
            parts
        };
        if parts.file {
            metadata
                .accept_replay_slot(target, FILE_SLOT, stamp)
                .await?;
        }
        if parts.inline {
            metadata
                .accept_replay_slot(target, INLINE_SLOT, stamp)
                .await?;
        }
        Ok(())
    }

    async fn beats_slot(
        &self,
        target: ReplayTarget<'_>,
        slot: &str,
        stamp: &ReplayStamp,
    ) -> Result<bool, SchemaError> {
        Ok(self
            .db_ops
            .metadata()
            .replay_slot_winner(target, slot)
            .await?
            .is_none_or(|held| stamp > &held))
    }

    async fn apply_newest(
        &self,
        schema: &Schema,
        address: &str,
        mutation: Mutation,
        stamp: &ReplayStamp,
    ) -> Result<(), SchemaError> {
        let metadata = self.db_ops.metadata();
        let deletes = matches!(
            mutation.mutation_type,
            MutationType::Delete | MutationType::Purge
        );
        if schema.file_required {
            if deletes {
                metadata.stage_saved_replay_fields(address, None).await?;
                return self
                    .apply_replay_record(schema, mutation, &stamp.writer_key)
                    .await;
            }
            let before = self.replay_preimage(address).await?;
            let live = before.as_deref().filter(|record| record.is_live());
            if !has_file(&mutation, live) {
                let saved = saved_from_mutation(schema, &mutation, stamp, live)?;
                metadata
                    .stage_saved_replay_fields(address, Some(&saved))
                    .await?;
                let Some(live) = live else {
                    return Ok(());
                };
                let mut removal = as_mutation(&schema.name, live.key.clone(), None);
                removal.imported_written_at = mutation.imported_written_at;
                return self
                    .apply_replay_record(schema, removal, &stamp.writer_key)
                    .await;
            }
            metadata.stage_saved_replay_fields(address, None).await?;
        }
        self.apply_replay_record(schema, mutation, &stamp.writer_key)
            .await
    }

    async fn apply_older_parts(
        &self,
        schema: &Schema,
        address: &str,
        mutation: Mutation,
        stamp: &ReplayStamp,
        parts: ReplayAttachmentSlots,
    ) -> Result<(), SchemaError> {
        let metadata = self.db_ops.metadata();
        let before = self.replay_preimage(address).await?;
        let deletes = matches!(
            mutation.mutation_type,
            MutationType::Delete | MutationType::Purge
        );
        let Some(live) = before.as_deref().filter(|record| record.is_live()) else {
            if schema.file_required && !deletes && parts.file && has_file(&mutation, None) {
                self.try_restore_saved_file(schema, address, mutation)
                    .await?;
            }
            return Ok(());
        };
        let clears = deletes
            || matches!(mutation.file_write, Some(FileWrite::Clear))
            || matches!(
                mutation.inline_write,
                Some(lastdb_model::record_value::InlineWrite::Clear)
            );
        if clears && live.file.is_none() && live.inline_file.is_none() {
            return Ok(());
        }
        let mut patched = as_mutation(&schema.name, live.key.clone(), Some(live.clone()));
        patched.imported_written_at = live.written_at();
        apply_attachment_patch(&mut patched, mutation, deletes, parts);
        let writer = live.updated_by.as_deref().unwrap_or(&stamp.writer_key);
        if schema.file_required && !has_file(&patched, Some(live)) {
            let saved = saved_from_live(live, writer);
            metadata
                .stage_saved_replay_fields(address, Some(&saved))
                .await?;
            let mut removal = as_mutation(&schema.name, live.key.clone(), None);
            removal.imported_written_at = live.written_at();
            return self.apply_replay_record(schema, removal, writer).await;
        }
        self.apply_replay_record(schema, patched, writer).await
    }

    async fn try_restore_saved_file(
        &self,
        schema: &Schema,
        address: &str,
        mutation: Mutation,
    ) -> Result<(), SchemaError> {
        let metadata = self.db_ops.metadata();
        let Some(saved) = metadata.saved_replay_fields(address).await? else {
            return Ok(());
        };
        let key = Self::canonical_primary_key(schema, &mutation.key_value)?;
        let mut restored = mutation_from_saved(&schema.name, key, &saved);
        restored.file = mutation.file;
        restored.file_write = mutation.file_write;
        let writer = saved.writer.clone();
        self.apply_replay_record(schema, restored, &writer).await?;
        metadata.stage_saved_replay_fields(address, None).await
    }

    /// Write one whole-record change in the caller's replay scope.
    async fn apply_replay_record(
        &self,
        schema: &Schema,
        mut mutation: Mutation,
        writer: &str,
    ) -> Result<(), SchemaError> {
        if mutation.writer.is_none() {
            mutation.writer = Some(writer.to_owned());
        }
        self.db_ops.metadata().require_replay_scope()?;
        let address = self.replay_record_address(schema, &mutation.key_value)?;
        let before = self.replay_preimage(&address).await?;
        let live = before.as_ref().filter(|record| record.is_live());
        match (&mutation.mutation_type, live) {
            (MutationType::Delete | MutationType::Purge, None) => return Ok(()),
            (MutationType::Delete | MutationType::Purge, Some(current)) => {
                mutation.alternates.clone_from(&current.alternates);
            }
            (_, Some(_)) => mutation.mutation_type = MutationType::Replace,
            (_, None) => mutation.mutation_type = MutationType::Create,
        }
        mutation.expected =
            live.map_or(CasExpectation::Absent, |current| CasExpectation::Revision {
                revision: current.stamp.revision(),
            });
        self.write_mutations_receipt(vec![mutation]).await?;
        Ok(())
    }
}
