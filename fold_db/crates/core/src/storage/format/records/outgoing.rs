//! Encode outgoing mutations from the same final record state the writer stages.

use super::{PortableEffect, RecordChange};
use crate::schema::{Schema, SchemaError};
use lastdb_model::portable_mutation::{
    FileMarker, MutationEntry, MutationOperation, PortableMutationError, RecordData, RecordKey,
    SchemaIdentity,
};
use lastdb_model::record_value::{self, InlineFile, InlineWrite, StoredRecord};
use serde_json::Value;
use std::collections::BTreeMap;

/// A shared entry carries the bytes of its record's inline file. A shared
/// write that keeps the inline file has no bytes in hand, so the writer reads
/// these rows, in one batch, before it encodes the entries.
pub(super) fn kept_inline_files(
    changes: &[RecordChange],
) -> Vec<(String, record_value::FileMarker)> {
    changes
        .iter()
        .filter(|change| matches!(change.portable, PortableEffect::SharedAuthor(_)))
        .filter(|change| !matches!(change.inline_write, Some(InlineWrite::Put(_))))
        .filter_map(|change| {
            let marker = change.after.inline_file.as_ref()?;
            change
                .after
                .is_live()
                .then(|| (change.key.as_str().to_owned(), marker.clone()))
        })
        .collect()
}

pub(super) fn entries(
    schema: &Schema,
    changes: &[RecordChange],
    kept: &BTreeMap<String, InlineFile>,
) -> Result<Vec<MutationEntry>, SchemaError> {
    let mut entries = Vec::new();
    for change in changes {
        let PortableEffect::SharedAuthor(schema_id) = &change.portable else {
            continue;
        };
        let primary_key =
            RecordKey::try_from(change.key.api_key().clone()).map_err(|error| invalid(&error))?;
        let operation = if change.after.is_live() {
            let inline = match &change.inline_write {
                Some(InlineWrite::Put(file)) => Some(file),
                _ => kept.get(change.key.as_str()),
            };
            MutationOperation::ReplaceRecord {
                primary_key,
                data: record_data(&change.after, inline)?,
            }
        } else {
            MutationOperation::DeleteRecord { primary_key }
        };
        let entry = MutationEntry {
            stamp: change.after.stamp.clone(),
            schema: SchemaIdentity::Shared(schema_id.clone()),
            operation,
        };
        entry
            .validate_keys(schema.require_record_keys()?)
            .map_err(|error| invalid(&error))?;
        entries.push(entry);
    }
    Ok(entries)
}

pub(crate) fn record_data(
    record: &StoredRecord,
    inline: Option<&InlineFile>,
) -> Result<RecordData, SchemaError> {
    if record.inline_file.as_ref() != inline.map(|file| &file.marker) {
        return Err(SchemaError::InvalidData(
            "a shared record's inline file bytes differ from its marker".into(),
        ));
    }
    let metadata = record
        .meta
        .as_ref()
        .map(|meta| match serde_json::to_value(meta) {
            Ok(Value::Object(object)) => Ok(object.into_iter().collect()),
            _ => Err(SchemaError::InvalidData(
                "record metadata is not a portable object".into(),
            )),
        })
        .transpose()?;
    let alternates = record
        .alternates
        .iter()
        .map(|(name, key)| {
            Ok((
                name.clone(),
                key.clone()
                    .map(RecordKey::try_from)
                    .transpose()
                    .map_err(|error| invalid(&error))?,
            ))
        })
        .collect::<Result<_, SchemaError>>()?;
    let file = record
        .file
        .as_ref()
        .zip(record.file_version.as_ref())
        .map(|(marker, version)| FileMarker {
            version: version.clone(),
            name: marker.name.clone(),
            media_type: marker.media_type.clone(),
            size_bytes: marker.size_bytes,
            sha256: marker.sha256.clone(),
        });
    Ok(RecordData {
        alternates,
        metadata,
        fields: record.fields.clone(),
        file,
        inline_file: inline.cloned().map(Box::new),
    })
}

fn invalid(error: &PortableMutationError) -> SchemaError {
    SchemaError::InvalidData(error.to_string())
}
