//! The last-writer rule for one record entry of another device.
//!
//! A record has three parts with a stamp each: the fields, the file and the
//! inline file. An entry is the newest write of the fields when its stamp is
//! above the record's winner row. It is the newest write of the file, or of the
//! inline file, when it sets or clears that part (a delete clears both) and its
//! stamp is above the row of that part. Each part takes the value of its newest
//! write, so the result does not depend on the order in which entries arrive.
//!
//! A local write is not compared here: it gets its stamp rows when it commits.

use super::{slots_of, MutationManager};
use crate::schema::types::{
    cas::CasExpectation, FileWrite, KeyValue, Mutation, MutationType, Schema,
};
use crate::schema::SchemaError;
use crate::storage::disk_pack::transaction;
use crate::storage::format::metadata::{ReplayStamp, ReplayTarget, FILE_SLOT, INLINE_SLOT};
use lastdb_model::record_replacement::RecordTarget;
use lastdb_model::record_value::{InlineWrite, StoredRecord};
use std::collections::{BTreeMap, HashMap};

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
        let installed = self.schema_manager.resolve_schema(schema).await?.is_some();
        transaction::scope(self.db_ops.native_runtime(), async {
            if !installed {
                return self.replay_schema_dropped_after(schema, stamp).await;
            }
            let address = self.replay_record_address(schema, key)?;
            let target = ReplayTarget::Record(&address);
            if self
                .db_ops
                .metadata()
                .replay_winner(target)
                .await?
                .is_none_or(|winner| &winner < stamp)
            {
                return Ok(false);
            }
            if slots.file && self.beats_slot(target, FILE_SLOT, stamp).await? {
                return Ok(false);
            }
            if slots.inline && self.beats_slot(target, INLINE_SLOT, stamp).await? {
                return Ok(false);
            }
            Ok(true)
        })
        .await
    }

    /// Apply the record write of an entry of another device, in the caller's replay scope.
    pub async fn merge_replay_record(
        &self,
        mutation: Mutation,
        stamp: &ReplayStamp,
    ) -> Result<(), SchemaError> {
        let metadata = self.db_ops.metadata();
        metadata.require_replay_scope()?;
        let schema = self.load_schema_for_delta(&mutation.schema_name).await?;
        let address = self.replay_record_address(&schema.name, &mutation.key_value)?;
        let target = ReplayTarget::Record(&address);
        let slots = slots_of(&mutation);
        let newest = metadata
            .replay_winner(target)
            .await?
            .is_none_or(|held| &held < stamp);
        let parts = if newest {
            self.apply_newest(&schema, address.clone(), mutation, stamp)
                .await?;
            metadata.accept_replay(target, stamp).await?;
            slots
        } else {
            let parts = ReplayAttachmentSlots {
                file: slots.file && self.beats_slot(target, FILE_SLOT, stamp).await?,
                inline: slots.inline && self.beats_slot(target, INLINE_SLOT, stamp).await?,
            };
            if parts.file || parts.inline {
                self.apply_older_parts(&schema, address.clone(), mutation, stamp, parts)
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

    /// True when `stamp` is above the stamp of the newest write of one part.
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

    /// The entry is the newest write of the fields: write the record it describes.
    /// A part the entry does not set stays as the record holds it.
    async fn apply_newest(
        &self,
        schema: &Schema,
        address: String,
        mutation: Mutation,
        stamp: &ReplayStamp,
    ) -> Result<(), SchemaError> {
        let deletes = matches!(
            mutation.mutation_type,
            MutationType::Delete | MutationType::Purge
        );
        if schema.file_required && !deletes {
            let before = self.replay_preimage(address).await?;
            let live = before.as_deref().filter(|record| record.is_live());
            if !has_file(mutation.file_write.as_ref(), live) {
                // A record of this schema does not exist without its file.
                let Some(live) = live else {
                    return Ok(());
                };
                let removal = as_mutation(&schema.name, live.key.clone(), None);
                return self.apply_replay_record(removal, &stamp.writer_key).await;
            }
        }
        self.apply_replay_record(mutation, &stamp.writer_key).await
    }

    /// The entry lost the fields to a newer write, and is the newest write of
    /// the file, the inline file, or both. Put those parts into the live record
    /// and leave its fields. A delete clears them.
    async fn apply_older_parts(
        &self,
        schema: &Schema,
        address: String,
        mutation: Mutation,
        stamp: &ReplayStamp,
        parts: ReplayAttachmentSlots,
    ) -> Result<(), SchemaError> {
        let before = self.replay_preimage(address).await?;
        let Some(live) = before.as_deref().filter(|record| record.is_live()) else {
            return Ok(());
        };
        let deletes = matches!(
            mutation.mutation_type,
            MutationType::Delete | MutationType::Purge
        );
        let nothing_to_clear = deletes
            && !(parts.file && live.file.is_some())
            && !(parts.inline && live.inline_file.is_some());
        if nothing_to_clear {
            return Ok(());
        }
        let mut patched = as_mutation(&schema.name, live.key.clone(), Some(live.clone()));
        if parts.file {
            if deletes {
                patched.file_write = Some(FileWrite::Clear);
            } else {
                patched.file = mutation.file;
                patched.file_write = mutation.file_write;
            }
        }
        if parts.inline {
            patched.inline_write = if deletes {
                Some(InlineWrite::Clear)
            } else {
                mutation.inline_write
            };
        }
        let writer = live.updated_by.as_deref().unwrap_or(&stamp.writer_key);
        if schema.file_required && !has_file(patched.file_write.as_ref(), Some(live)) {
            // A record of this schema does not exist without its file.
            let removal = as_mutation(&schema.name, live.key.clone(), None);
            return self.apply_replay_record(removal, writer).await;
        }
        self.apply_replay_record(patched, writer).await
    }

    /// Write one whole-record change in the caller's replay scope.
    /// `writer` is the log entry's writer key. A caller that already set one keeps it.
    async fn apply_replay_record(
        &self,
        mut mutation: Mutation,
        writer: &str,
    ) -> Result<(), SchemaError> {
        if mutation.writer.is_none() {
            mutation.writer = Some(writer.to_owned());
        }
        self.db_ops.metadata().require_replay_scope()?;
        let address = self.replay_record_address(&mutation.schema_name, &mutation.key_value)?;
        let before = self.replay_preimage(address).await?;
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

/// Whether the record holds a file after a write: the write sets one, or it
/// leaves the file of a live record in place.
fn has_file(write: Option<&FileWrite>, live: Option<&StoredRecord>) -> bool {
    match write {
        Some(FileWrite::Clear) => false,
        Some(_) => true,
        None => live.is_some_and(|record| record.file.is_some()),
    }
}

/// The stored record as a write of the same content, or a delete when there is none.
fn as_mutation(schema: &str, key: KeyValue, record: Option<StoredRecord>) -> Mutation {
    let mut target = RecordTarget {
        schema: schema.to_owned(),
        key,
        alternates: BTreeMap::new(),
    };
    let kind = if record.is_some() {
        MutationType::Create
    } else {
        MutationType::Delete
    };
    let mut fields = HashMap::new();
    let mut meta = None;
    let mut copied_authorship = None;
    if let Some(record) = record {
        target.alternates = record.alternates;
        fields = record.fields.into_iter().collect();
        meta = record.meta;
        copied_authorship = Some((record.created_by, record.updated_by));
    }
    let mut mutation = Mutation::with_uuid(
        uuid::Uuid::new_v4().to_string(),
        target,
        fields,
        kind,
        CasExpectation::Absent,
    );
    // Author keys travel in `copied_authorship`, not in the copied map.
    if let Some(meta) = meta.and_then(lastdb_model::record_value::RecordMeta::drop_reserved_authors)
    {
        mutation.source_file_name = meta.source_file_name;
        if !meta.metadata.is_empty() {
            mutation.metadata = Some(meta.metadata.into_iter().collect());
        }
    }
    mutation.copied_authorship = copied_authorship;
    mutation
}
