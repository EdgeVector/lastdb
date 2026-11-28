//! Replay mutation helpers and saved-field conversions for `file_required` schemas.

use super::records::ReplayAttachmentSlots;
use super::MutationManager;
use crate::fold_db_core::mutation_manager::replace::{replacement_metadata, validate_fields};
use crate::schema::types::{
    cas::CasExpectation, FileWrite, KeyValue, Mutation, MutationType, Schema,
};
use crate::schema::SchemaError;
use crate::storage::format::metadata::{ReplayStamp, SavedReplayFields};
use lastdb_model::record_replacement::RecordTarget;
use lastdb_model::record_value::{InlineWrite, RecordMeta, StoredRecord};
use std::collections::{BTreeMap, HashMap};

/// Whether the record holds an external file after `mutation` applies on top of `live`.
pub(super) fn has_file(mutation: &Mutation, live: Option<&StoredRecord>) -> bool {
    match &mutation.file_write {
        Some(FileWrite::Put(_) | FileWrite::LocalPath(_)) => true,
        Some(FileWrite::Clear) => false,
        // An inline-only write leaves the external file slot alone.
        None => live.is_some_and(|record| record.file.is_some()),
    }
}

pub(super) fn saved_from_mutation(
    schema: &Schema,
    mutation: &Mutation,
    stamp: &ReplayStamp,
    live: Option<&StoredRecord>,
) -> Result<SavedReplayFields, SchemaError> {
    let key = MutationManager::canonical_primary_key(schema, &mutation.key_value)?;
    let mut alternates = mutation.alternates.clone();
    MutationManager::canonicalize_alternates(schema, &mut alternates)?;
    let target = RecordTarget {
        schema: schema.name.clone(),
        key,
        alternates: alternates.clone(),
    };
    target
        .validate(schema.require_record_keys()?)
        .map_err(SchemaError::InvalidData)?;
    let fields = validate_fields(schema, &mutation.record_fields)?;
    let writer = mutation
        .writer
        .clone()
        .unwrap_or_else(|| stamp.writer_key.clone());
    let created_by = live
        .and_then(|r| r.created_by.clone())
        .or_else(|| Some(writer.clone()));
    Ok(SavedReplayFields {
        fields,
        alternates,
        writer,
        created_by,
        meta: replacement_metadata(mutation),
        ordering_ms: Some(stamp.ordering_ms),
    })
}

pub(super) fn saved_from_live(live: &StoredRecord, fallback_writer: &str) -> SavedReplayFields {
    let writer = live
        .updated_by
        .clone()
        .unwrap_or_else(|| fallback_writer.to_owned());
    SavedReplayFields {
        fields: live.fields.clone(),
        alternates: live.alternates.clone(),
        writer,
        created_by: live.created_by.clone(),
        meta: live
            .meta
            .clone()
            .and_then(RecordMeta::drop_reserved_authors),
        ordering_ms: Some(live.stamp.authored_at_ms),
    }
}

pub(super) fn mutation_from_saved(
    schema: &str,
    key: KeyValue,
    saved: &SavedReplayFields,
) -> Mutation {
    let target = RecordTarget {
        schema: schema.to_owned(),
        key,
        alternates: saved.alternates.clone(),
    };
    let fields: HashMap<_, _> = saved
        .fields
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let mut mutation = Mutation::with_uuid(
        uuid::Uuid::new_v4().to_string(),
        target,
        fields,
        MutationType::Create,
        CasExpectation::Absent,
    );
    if let Some(meta) = saved
        .meta
        .clone()
        .and_then(RecordMeta::drop_reserved_authors)
    {
        mutation.source_file_name = meta.source_file_name;
        if !meta.metadata.is_empty() {
            mutation.metadata = Some(meta.metadata.into_iter().collect());
        }
    }
    mutation.writer = Some(saved.writer.clone());
    mutation.copied_authorship = Some((saved.created_by.clone(), Some(saved.writer.clone())));
    mutation.imported_written_at = saved.ordering_ms.map(|ms| ms.saturating_mul(1_000_000));
    mutation
}

/// The stored record as a write of the same content, or a delete when there is none.
pub(super) fn as_mutation(schema: &str, key: KeyValue, record: Option<StoredRecord>) -> Mutation {
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
    if let Some(meta) = meta.and_then(RecordMeta::drop_reserved_authors) {
        mutation.source_file_name = meta.source_file_name;
        if !meta.metadata.is_empty() {
            mutation.metadata = Some(meta.metadata.into_iter().collect());
        }
    }
    mutation.copied_authorship = copied_authorship;
    mutation
}

pub(super) fn apply_attachment_patch(
    patched: &mut Mutation,
    mutation: Mutation,
    deletes: bool,
    parts: ReplayAttachmentSlots,
) {
    if deletes {
        if parts.file {
            patched.file_write = Some(FileWrite::Clear);
        }
        if parts.inline {
            patched.inline_write = Some(InlineWrite::Clear);
        }
        return;
    }
    if parts.file {
        match &mutation.file_write {
            Some(fw @ (FileWrite::Put(_) | FileWrite::LocalPath(_))) => {
                patched.file = mutation.file.clone();
                patched.file_write = Some(fw.clone());
            }
            Some(FileWrite::Clear) => {
                patched.file = None;
                patched.file_write = Some(FileWrite::Clear);
            }
            None => {}
        }
    }
    if parts.inline {
        match &mutation.inline_write {
            Some(iw @ InlineWrite::Put(_)) => {
                patched.inline_write = Some(iw.clone());
            }
            Some(InlineWrite::Clear) => {
                patched.inline_write = Some(InlineWrite::Clear);
            }
            None => {}
        }
    }
}
