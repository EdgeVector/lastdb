//! A schema release selects format-owned families, never caller-built prefixes.

use super::{
    applied_deletes::APPLIED_DELETE_FAMILY, file_crypto::dropped_file_paths,
    inline_files::INLINE_FILE_FAMILY, RecordFormat,
};
use crate::schema::{Schema, SchemaError};
use crate::storage::format::metadata::FileReferenceChange;
use crate::storage::format::MAIN_NAMESPACE;
use crate::storage::KvMutation;
use lastdb_model::field_key_id::{alternate_field_key_id, ALTERNATE_ENTRY_PREFIX};
use lastdb_model::record_space::{record_space_uuid, RECORD_PREFIX};

#[derive(Clone, Copy)]
pub(crate) enum ReleaseKind {
    Records,
    Alternates,
    AppliedDeletes,
    InlineFiles,
}

pub(crate) struct ReleaseFamily {
    prefix: String,
    kind: ReleaseKind,
}

pub(crate) fn release_families(schema: &Schema) -> Result<Vec<ReleaseFamily>, SchemaError> {
    let space = record_space_uuid(&schema.name);
    let mut families = vec![
        ReleaseFamily {
            prefix: format!("{RECORD_PREFIX}{space}:"),
            kind: ReleaseKind::Records,
        },
        ReleaseFamily {
            prefix: format!("{APPLIED_DELETE_FAMILY}:{RECORD_PREFIX}{space}:"),
            kind: ReleaseKind::AppliedDeletes,
        },
        ReleaseFamily {
            prefix: format!("{INLINE_FILE_FAMILY}:{RECORD_PREFIX}{space}:"),
            kind: ReleaseKind::InlineFiles,
        },
    ];
    families.extend(
        schema
            .require_record_keys()?
            .alternates
            .keys()
            .map(|name| ReleaseFamily {
                prefix: format!(
                    "{ALTERNATE_ENTRY_PREFIX}{}:",
                    alternate_field_key_id(schema, name)
                ),
                kind: ReleaseKind::Alternates,
            }),
    );
    Ok(families)
}

impl ReleaseFamily {
    pub(crate) fn prefix(&self) -> &str {
        &self.prefix
    }
    pub(crate) fn kind(&self) -> ReleaseKind {
        self.kind
    }

    fn require_key(&self, key: &[u8]) -> Result<(), SchemaError> {
        if !key.starts_with(self.prefix.as_bytes()) {
            return Err(SchemaError::InvalidData(
                "schema release key has a different owner or family".into(),
            ));
        }
        Ok(crate::storage::current_keys::validate(MAIN_NAMESPACE, key)?)
    }
}

impl RecordFormat {
    /// Re-read selected keys under the same gate, then stage the exact child deletes.
    pub(crate) async fn release_schema_batch(
        &self,
        keys: Vec<Vec<u8>>,
        family: &ReleaseFamily,
    ) -> Result<u64, SchemaError> {
        self.require_scope()?;
        if keys.len() > super::super::SCHEMA_RELEASE_ROWS {
            return Err(SchemaError::InvalidData(
                "schema release exceeds its row bound".into(),
            ));
        }
        for key in &keys {
            family.require_key(key)?;
        }
        let stored = self.main_store.inner().get_many(keys.clone()).await?;
        if stored.len() != keys.len() {
            return Err(SchemaError::InvalidData(
                "schema release read batch is incomplete".into(),
            ));
        }
        let rows: Vec<_> = keys
            .into_iter()
            .zip(stored)
            .filter_map(|(key, value)| value.map(|value| (key, value)))
            .collect();
        if rows.is_empty() {
            return Ok(0);
        }
        let count = rows.len() as u64;
        let files = if matches!(family.kind, ReleaseKind::Records) {
            dropped_file_paths(rows.iter().map(|(k, v)| (k.as_slice(), v.as_slice())))?
        } else {
            Vec::new()
        };
        let deletes = rows
            .into_iter()
            .map(|(key, _)| KvMutation::delete(key))
            .collect();
        self.main_store.inner().batch_mutate(deletes).await?;
        let references = files
            .into_iter()
            .map(|key| {
                let path = String::from_utf8(key).map_err(|error| {
                    SchemaError::InvalidData(format!("released file key: {error}"))
                })?;
                Ok((
                    path,
                    FileReferenceChange {
                        live: Some(false),
                        local_detach: true,
                        ..FileReferenceChange::default()
                    },
                ))
            })
            .collect::<Result<_, SchemaError>>()?;
        let files = self.metadata.stage_file_references(references).await?;
        self.stage_files(files.into_iter().map(KvMutation::delete).collect())
            .await?;
        Ok(count)
    }
}
