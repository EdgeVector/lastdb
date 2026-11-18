//! Preimage loading, CAS expectation checks, and whole-record replacement construction.

use std::collections::BTreeMap;
use std::sync::Arc;

use lastdb_model::portable_mutation::Stamp;
use lastdb_model::record_replacement::{RecordMetadata, RecordReplacement};
use lastdb_model::record_value::{InlineWrite, StoredRecord};

use super::AuthoredMutation;
use crate::db_operations::record_store::RecordStore;
use crate::schema::types::{cas::CasExpectation, FileWrite, Mutation, MutationType, Schema};
use crate::schema::SchemaError;
use crate::storage::format::records::RecordAddress;

pub(super) async fn read_write_preimages(
    records: &RecordStore,
    addresses: &[RecordAddress],
    mutations: &[AuthoredMutation],
) -> Result<Vec<Option<Arc<StoredRecord>>>, SchemaError> {
    let mut stored = records.read_records(addresses).await?;
    let misses: Vec<_> = mutations
        .iter()
        .zip(&stored)
        .enumerate()
        .filter_map(|(i, (m, r))| {
            (m.mutation.mutation_type == MutationType::Purge && r.is_none()).then_some(i)
        })
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

pub(super) fn require_contribution_definition(
    schema: &Schema,
    expected: &str,
) -> Result<(), SchemaError> {
    let wire = schema.to_wire();
    wire.identity_hash
        .clone()
        .ok_or_else(|| "schema has no catalog identity".to_owned())
        .and_then(|id| schema_types::PortableSchemaId::from_catalog(&id, wire))
        .ok()
        .filter(|id| id.as_str() == expected)
        .map(|_| ())
        .ok_or_else(|| {
            SchemaError::InvalidField(
                "share contribution requires the exact installed schema definition".into(),
            )
        })
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
        let msg = "deletion must supply the record's complete current alternate keys".into();
        return Err(if mutation.contribution.is_some() {
            SchemaError::InvalidField(msg)
        } else {
            SchemaError::InvalidData(msg)
        });
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

pub(super) fn replace_record(
    schema: &Schema,
    mutation: &Mutation,
    stamp: Stamp,
    before: Option<&StoredRecord>,
) -> Result<StoredRecord, SchemaError> {
    if matches!(
        mutation.mutation_type,
        MutationType::Delete | MutationType::Purge
    ) {
        return Ok(StoredRecord::erased(stamp));
    }
    let mut fields = BTreeMap::new();
    for (field, value) in &mutation.record_fields {
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
    let file = mutation.file.clone().or_else(|| {
        (!matches!(&mutation.file_write, Some(FileWrite::Clear)))
            .then(|| before.and_then(|r| r.file.clone()))
            .flatten()
    });
    let inline_file = match &mutation.inline_write {
        Some(InlineWrite::Put(f)) => Some(f.marker.clone()),
        Some(InlineWrite::Clear) => None,
        None => before.and_then(|r| r.inline_file.clone()),
    };
    let replacement = RecordReplacement {
        target: mutation.target(),
        expected: mutation.expected.clone(),
        fields,
        metadata: replacement_metadata(mutation, before),
        file,
        inline_file,
    };
    replacement
        .validate(schema.require_record_keys()?)
        .map_err(SchemaError::InvalidData)?;
    let mut stored = StoredRecord::replacement(replacement, stamp, before);
    stamp_authors(&mut stored, mutation, before);
    Ok(stored)
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

fn replacement_metadata(
    mutation: &Mutation,
    before: Option<&StoredRecord>,
) -> Option<RecordMetadata> {
    if mutation.mutation_type == MutationType::Replace
        && mutation
            .contribution
            .as_ref()
            .is_some_and(|admission| admission.schema_scope)
    {
        // The same preimage supplies the revision check and the publisher's metadata.
        return before.and_then(|record| record.meta.clone());
    }
    (mutation.source_file_name.is_some() || mutation.metadata.is_some()).then(|| RecordMetadata {
        source_file_name: mutation.source_file_name.clone(),
        metadata: mutation
            .metadata
            .clone()
            .unwrap_or_default()
            .into_iter()
            .collect(),
    })
}
