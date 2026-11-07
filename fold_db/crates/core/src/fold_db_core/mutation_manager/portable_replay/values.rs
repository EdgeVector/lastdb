use super::invalid;
use crate::schema::SchemaError;
use lastdb_model::portable_mutation::{MutationEntry, MutationOperation};
use lastdb_model::record_value::{FileMarker, StoredRecord, RECORD_VALUE_VERSION};
use serde_json::{Map, Value};

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
        inline_file: data.inline_file.clone(),
        deleted: false,
    })
}
