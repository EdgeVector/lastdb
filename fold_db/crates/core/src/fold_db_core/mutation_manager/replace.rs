//! Preimage loading, CAS expectation checks, and whole-record replacement construction.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use lastdb_model::portable_mutation::Stamp;
use lastdb_model::record_replacement::{RecordMetadata, RecordReplacement};
use lastdb_model::record_value::{FileMarker, InlineWrite, StoredRecord};
use serde_json::Value;

use crate::db_operations::record_store::RecordStore;
use crate::schema::types::{cas::CasExpectation, FileWrite, Mutation, MutationType, Schema};
use crate::schema::SchemaError;
use crate::storage::format::records::RecordAddress;

pub(super) async fn read_write_preimages(
    records: &RecordStore,
    addresses: &[RecordAddress],
    purges: &[bool],
) -> Result<Vec<Option<Arc<StoredRecord>>>, SchemaError> {
    let mut stored = records.read_records(addresses).await?;
    let misses: Vec<_> = purges
        .iter()
        .zip(&stored)
        .enumerate()
        .filter_map(|(i, (&is_purge, r))| (is_purge && r.is_none()).then_some(i))
        .collect();
    if !misses.is_empty() {
        let addrs: Vec<_> = misses.iter().map(|&i| &addresses[i]).collect();
        for (idx, stamp) in misses
            .into_iter()
            .zip(records.format.applied_deletes(&addrs).await?)
        {
            stored[idx] = stamp.map(|s| Arc::new(StoredRecord::erased(s)));
        }
    }
    Ok(stored)
}

pub(super) fn check_expectation(
    mutation: &Mutation,
    record: Option<&StoredRecord>,
) -> Result<(), SchemaError> {
    let current = record.filter(|r| r.is_live() || mutation.mutation_type == MutationType::Purge);
    let actual = current.map(|r| r.stamp.revision());
    let matches = match &mutation.expected {
        CasExpectation::Absent => !record.is_some_and(StoredRecord::is_live),
        CasExpectation::Revision { revision } => actual.as_ref() == Some(revision),
    };
    if !matches {
        let expected = match &mutation.expected {
            CasExpectation::Absent => "<absent>".into(),
            CasExpectation::Revision { revision } => revision.clone(),
        };
        return Err(SchemaError::CasConflict {
            schema: mutation.schema_name.clone(),
            key: mutation.key_value.to_storage_key(),
            expected,
            actual,
        });
    }
    if matches!(
        mutation.mutation_type,
        MutationType::Delete | MutationType::Purge
    ) && current.is_some_and(|r| r.is_live() && r.alternates != mutation.alternates)
    {
        return Err(SchemaError::InvalidData(
            "deletion must supply the record's complete current alternate keys".into(),
        ));
    }
    if mutation.erase_written_before_nanos.is_some_and(|cutoff| {
        record
            .and_then(StoredRecord::written_at)
            .is_some_and(|w| w >= cutoff)
    }) {
        return Err(SchemaError::InvalidData(
            "record is newer than the erasure cutoff".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_fields(
    schema: &Schema,
    record_fields: &HashMap<String, Value>,
) -> Result<BTreeMap<String, Value>, SchemaError> {
    let mut fields = BTreeMap::new();
    for (field, value) in record_fields {
        if !schema.fields.iter().flatten().any(|f| f == field) {
            return Err(SchemaError::InvalidData(format!(
                "field '{field}' is not declared in schema '{}'",
                schema.name
            )));
        }
        let field_type = schema.get_field_type(field);
        if let Err(type_err) = field_type.validate(value) {
            return Err(SchemaError::InvalidData(format!(
                "Type error in field '{field}' of schema '{}': {type_err}. Expected {field_type}, got {}",
                schema.name,
                serde_json::to_string(value).unwrap_or_else(|_| "?".into())
            )));
        }
        fields.insert(field.clone(), value.clone());
    }
    Ok(fields)
}

pub(super) fn build_replacement(
    schema: &Schema,
    mutation: &Mutation,
    before: Option<&StoredRecord>,
) -> Result<Option<(RecordReplacement, bool)>, SchemaError> {
    if matches!(
        mutation.mutation_type,
        MutationType::Delete | MutationType::Purge
    ) {
        return Ok(None);
    }
    let live_before = before.filter(|r| r.is_live());
    let fields = validate_fields(schema, &mutation.record_fields)?;
    let (file, inline_file, keep_file) = resolve_attachments(mutation, live_before)?;
    if schema.file_required && file.is_none() {
        return Err(SchemaError::InvalidData(
            "this record requires a file".into(),
        ));
    }
    let replacement = RecordReplacement {
        target: mutation.target(),
        expected: mutation.expected.clone(),
        fields,
        metadata: replacement_metadata(mutation),
        file,
        inline_file,
    };
    replacement
        .validate(schema.require_record_keys()?)
        .map_err(SchemaError::InvalidData)?;
    Ok(Some((replacement, keep_file)))
}

fn resolve_attachments(
    mutation: &Mutation,
    before: Option<&StoredRecord>,
) -> Result<(Option<FileMarker>, Option<FileMarker>, bool), SchemaError> {
    // External file and inline file are independent slots. A write that names
    // only one leaves the other as it was. A photo may keep a full file and a
    // thumbnail together. `keep_file` preserves the on-disk file version when
    // this write does not touch the external file slot.
    let keep_file = mutation.file_write.is_none();
    let file = match &mutation.file_write {
        Some(FileWrite::Put(f)) => {
            Some(mutation.file.clone().unwrap_or_else(|| f.marker().clone()))
        }
        Some(FileWrite::LocalPath(f)) => {
            Some(mutation.file.clone().unwrap_or_else(|| f.marker().clone()))
        }
        Some(FileWrite::Clear) => {
            if mutation.file.is_some() {
                return Err(SchemaError::InvalidData(
                    "a file clear cannot carry a file marker".into(),
                ));
            }
            None
        }
        None => mutation
            .file
            .clone()
            .or_else(|| before.and_then(|r| r.file.clone())),
    };
    let inline_file = match &mutation.inline_write {
        Some(InlineWrite::Put(f)) => Some(f.marker.clone()),
        Some(InlineWrite::Clear) => None,
        None => before.and_then(|r| r.inline_file.clone()),
    };
    Ok((file, inline_file, keep_file))
}

pub(super) fn finish_record(
    replacement: Option<(RecordReplacement, bool)>,
    mutation: &Mutation,
    stamp: Stamp,
    before: Option<&StoredRecord>,
) -> StoredRecord {
    let Some((replacement, keep_file)) = replacement else {
        return StoredRecord::erased(stamp);
    };
    let live_before = before.filter(|r| r.is_live());
    let mut stored = StoredRecord::replacement(replacement, stamp, live_before, keep_file);
    stamp_authors(&mut stored, mutation, live_before);
    stored
}

/// `created_by` is set only when no live record is at the key.
/// A replace keeps the previous live record's `created_by`, including when it is absent.
/// `updated_by` is the writer. A saved record copies both values, including when absent.
pub(super) fn stamp_authors(
    record: &mut StoredRecord,
    mutation: &Mutation,
    before: Option<&StoredRecord>,
) {
    if let Some((created_by, updated_by)) = &mutation.copied_authorship {
        record.created_by.clone_from(created_by);
        record.updated_by.clone_from(updated_by);
        return;
    }
    let Some(writer) = mutation.writer.clone() else {
        return;
    };
    record.created_by = match before.filter(|existing| existing.is_live()) {
        Some(existing) => existing.created_by.clone(),
        None => Some(writer.clone()),
    };
    record.updated_by = Some(writer);
}

pub(super) fn replacement_metadata(mutation: &Mutation) -> Option<RecordMetadata> {
    let meta = if mutation.source_file_name.is_some() || mutation.metadata.is_some() {
        Some(RecordMetadata {
            source_file_name: mutation.source_file_name.clone(),
            metadata: mutation
                .metadata
                .clone()
                .unwrap_or_default()
                .into_iter()
                .collect(),
        })
    } else {
        None
    };
    meta.and_then(RecordMetadata::drop_reserved_authors)
}
