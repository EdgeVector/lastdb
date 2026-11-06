//! Retire exact record-owned versions once both local references are gone.
//! Existing homes rebuild the index in durable, bounded maintenance steps.

use super::{RecordAddress, RecordFormat};
use crate::schema::SchemaError;
use crate::storage::current_keys::families::record_file_path;
use crate::storage::format::metadata::{FileReferenceChange, FileRetention};
use crate::storage::format::RECORD_FILE_PREFIX;
use crate::storage::{KvMutation, KvStore};
use lastdb_model::portable_mutation::Position;
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::StoredRecord;
use std::collections::BTreeMap;

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
                // Once caught up, every future pending reference is indexed
                // by the record writer. Until then, keep legacy file versions.
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
        let rows = page(self.main_store.inner().as_ref(), RECORD_PREFIX, after, 64).await?;
        let Some((last, _)) = rows.last() else {
            return Ok(FileRetention::Files(Vec::new()));
        };
        let mut references = BTreeMap::new();
        for (key, value) in &rows {
            let record = StoredRecord::decode(value).map_err(invalid)?;
            if let Some(version) = record.file_version {
                let key = std::str::from_utf8(key).map_err(invalid)?;
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
        // The old index carries no owner address. Walk the exact file family
        // after marking every live file and establishing pending-log coverage.
        let mut rows = page(files.as_ref(), RECORD_FILE_PREFIX, after, 1).await?;
        let Some((key, _)) = rows.pop() else {
            return Ok(FileRetention::Complete);
        };
        let path = std::str::from_utf8(&key).map_err(invalid)?;
        if !self.metadata.has_file_reference(path).await? {
            self.stage_files(vec![KvMutation::delete(key.clone())])
                .await?;
        }
        Ok(FileRetention::Files(key))
    }
}

async fn page(
    store: &dyn KvStore,
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
        return Err(invalid("file retention cursor is outside its family"));
    };
    Ok(store.scan_range_paged(&start, &end, limit).await?)
}

fn invalid(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(format!("shared file retention: {error}"))
}
