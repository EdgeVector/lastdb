use super::invalid;
use crate::schema::SchemaError;
use lastdb_model::portable_mutation::{MutationEntry, MutationOperation, SchemaIdentity};
use lastdb_model::record_value::{FileMarker, InlineWrite, StoredRecord, RECORD_VALUE_VERSION};
use schema_types::PortableSchemaId;
use serde_json::{Map, Value};

/// A replacement entry carries the bytes of its record's inline file, so the
/// replayed record writes its inline file row. An unchanged marker keeps the
/// local row; a record without one clears it.
pub(super) fn inline_write(
    entry: &MutationEntry,
    before: Option<&StoredRecord>,
) -> Option<InlineWrite> {
    let MutationOperation::ReplaceRecord { data, .. } = &entry.operation else {
        return None;
    };
    let before = before.and_then(|record| record.inline_file.as_ref());
    match data.inline_file.as_deref() {
        Some(file) if before == Some(&file.marker) => None,
        Some(file) => Some(InlineWrite::Put(file.clone())),
        None if before.is_some() => Some(InlineWrite::Clear),
        None => None,
    }
}

pub(super) fn record(entry: &MutationEntry) -> Result<StoredRecord, SchemaError> {
    let MutationOperation::ReplaceRecord { primary_key, data } = &entry.operation else {
        return Ok(StoredRecord::erased(entry.stamp.clone()));
    };
    let meta = data
        .metadata
        .as_ref()
        .map(|metadata| {
            let object: Map<String, Value> = metadata.clone().into_iter().collect();
            serde_json::from_value(Value::Object(object))
                .map_err(|_| invalid("portable record metadata differs from the record API"))
        })
        .transpose()?;
    // A shared record keeps the definition its writer used.
    let definition_id = match &entry.schema {
        SchemaIdentity::Shared(id) => Some(
            id.parse::<PortableSchemaId>()
                .map_err(|_| invalid("a shared record names an invalid schema definition"))?,
        ),
        SchemaIdentity::Local(_) => None,
    };
    Ok(StoredRecord {
        v: RECORD_VALUE_VERSION,
        key: primary_key.clone().into(),
        alternates: data
            .alternates
            .iter()
            .map(|(name, key)| (name.clone(), key.clone().map(Into::into)))
            .collect(),
        fields: data.fields.clone(),
        stamp: entry.stamp.clone(),
        meta,
        file: data.file.as_ref().map(|file| FileMarker {
            name: file.name.clone(),
            media_type: file.media_type.clone(),
            size_bytes: file.size_bytes,
            sha256: file.sha256.clone(),
        }),
        file_version: data.file.as_ref().map(|file| file.version.clone()),
        inline_file: data.inline_file.as_ref().map(|file| file.marker.clone()),
        definition_id,
        deleted: false,
    })
}
