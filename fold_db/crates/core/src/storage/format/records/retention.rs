//! Outgoing portable mutation encoding and file-version retention steps.

use super::{PortableEffect, RecordAddress, RecordChange, RecordFormat};
use crate::schema::{Schema, SchemaError};
use crate::storage::current_keys::families::record_file_path;
use crate::storage::disk_pack::PackKvStore;
use crate::storage::format::metadata::{FileReferenceChange, FileRetention};
use crate::storage::format::RECORD_FILE_PREFIX;
use crate::storage::KvMutation;
use lastdb_model::portable_mutation::{
    FileMarker, MutationEntry, MutationOperation, PortableMutationError, Position, RecordData,
    RecordKey, SchemaIdentity,
};
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::{self, InlineFile, InlineWrite, StoredRecord};
use serde_json::Value;
use std::collections::BTreeMap;

pub(in crate::storage::format::records) fn kept_inline_files(
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

pub(in crate::storage::format::records) fn outgoing_entries(
    schema: &Schema,
    changes: &[RecordChange],
    kept: &BTreeMap<String, InlineFile>,
) -> Result<Vec<MutationEntry>, SchemaError> {
    let mut entries = Vec::new();
    for change in changes {
        let PortableEffect::SharedAuthor(schema_id) = &change.portable else {
            continue;
        };
        let primary_key = RecordKey::try_from(change.key.api_key().clone())
            .map_err(|error| invalid_portable(&error))?;
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
            .map_err(|error| invalid_portable(&error))?;
        entries.push(entry);
    }
    Ok(entries)
}

fn record_data(
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
                    .map_err(|error| invalid_portable(&error))?,
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

fn invalid_portable(error: &PortableMutationError) -> SchemaError {
    SchemaError::InvalidData(error.to_string())
}

impl RecordFormat {
    pub(crate) async fn stage_completed_file_references(
        &self,
        files: Vec<(RecordAddress, Position)>,
        through: u64,
    ) -> Result<(), SchemaError> {
        let changes = files
            .into_iter()
            .map(|(key, version)| {
                Ok((
                    record_file_path(key.as_str(), &version)?,
                    FileReferenceChange {
                        completed_through: through,
                        ..FileReferenceChange::default()
                    },
                ))
            })
            .collect::<Result<_, SchemaError>>()?;
        let retired = self.metadata.stage_file_references(changes).await?;
        self.stage_files(retired.into_iter().map(KvMutation::delete).collect())
            .await
    }

    pub(crate) async fn file_retention_complete(&self) -> Result<bool, SchemaError> {
        Ok(matches!(
            self.metadata.file_retention().await?,
            FileRetention::Complete
        ))
    }

    pub(crate) async fn stage_file_retention_step(&self) -> Result<bool, SchemaError> {
        self.require_scope()?;
        let next = match self.metadata.file_retention().await? {
            FileRetention::Uninitialized => {
                if !self.metadata.portable_writer_caught_up().await? {
                    return Ok(false);
                }
                FileRetention::Log(Vec::new())
            }
            FileRetention::Log(after) => match self.metadata.stage_legacy_log_cleanup(after).await?
            {
                Some(last) => FileRetention::Log(last),
                None => FileRetention::Records(Vec::new()),
            },
            FileRetention::Records(after) => self.mark_live_file_page(after).await?,
            FileRetention::Files(after) => self.retire_file_page(after).await?,
            FileRetention::Complete => return Ok(false),
        };
        self.metadata.stage_file_retention(&next).await?;
        Ok(!matches!(next, FileRetention::Complete))
    }

    async fn mark_live_file_page(&self, after: Vec<u8>) -> Result<FileRetention, SchemaError> {
        let rows = page(&self.main_store, RECORD_PREFIX, after, 64).await?;
        let Some((last, _)) = rows.last() else {
            return Ok(FileRetention::Files(Vec::new()));
        };
        let mut references = BTreeMap::new();
        for (key, value) in &rows {
            let record = StoredRecord::decode(value).map_err(invalid_retention)?;
            if let Some(version) = record.file_version {
                let key = std::str::from_utf8(key).map_err(invalid_retention)?;
                references.insert(
                    record_file_path(key, &version)?,
                    FileReferenceChange {
                        live: Some(true),
                        ..FileReferenceChange::default()
                    },
                );
            }
        }
        self.metadata.stage_file_references(references).await?;
        Ok(FileRetention::Records(last.clone()))
    }

    async fn retire_file_page(&self, after: Vec<u8>) -> Result<FileRetention, SchemaError> {
        let files = self.file_store().await?;
        let mut rows = page(&files, RECORD_FILE_PREFIX, after, 1).await?;
        let Some((key, _)) = rows.pop() else {
            return Ok(FileRetention::Complete);
        };
        let path = std::str::from_utf8(&key).map_err(invalid_retention)?;
        if !self.metadata.has_file_reference(path).await? {
            self.stage_files(vec![KvMutation::delete(key.clone())])
                .await?;
        }
        Ok(FileRetention::Files(key))
    }
}

async fn page(
    store: &PackKvStore,
    prefix: &str,
    after: Vec<u8>,
    limit: usize,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, SchemaError> {
    let mut end = prefix.as_bytes().to_vec();
    *end.last_mut().expect("file retention family separator") = b';';
    let start = if after.is_empty() {
        prefix.as_bytes().to_vec()
    } else if after.starts_with(prefix.as_bytes()) {
        [after, vec![0]].concat()
    } else {
        return Err(invalid_retention(
            "file retention cursor is outside its family",
        ));
    };
    Ok(store.scan_range_paged(&start, &end, limit).await?)
}

fn invalid_retention(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(format!("shared file retention: {error}"))
}
