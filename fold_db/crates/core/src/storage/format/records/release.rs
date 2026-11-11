//! Format-minted record/alternate addresses, schema release, and erase audit rows.

use super::{
    applied_deletes::APPLIED_DELETE_FAMILY, file_crypto::dropped_file_paths,
    inline_files::INLINE_FILE_FAMILY, RecordFormat,
};
use crate::schema::types::{field::HashRangeFilter, KeyValue, Schema};
use crate::schema::SchemaError;
use crate::storage::format::{
    control, CURRENT_KEY_FORMAT_KEY, DELETE_AUDIT_FAMILY, DELETE_REPLAY_CUTOFF_KEY, MAIN_NAMESPACE,
};
use crate::storage::{KvMutation, StorageError};
use lastdb_model::field_key_codec::FieldKeyCodec;
use lastdb_model::field_key_id::{alternate_field_key_id, ALTERNATE_ENTRY_PREFIX};
use lastdb_model::record_space::{record_space_uuid, RECORD_PREFIX};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One audit row per erase batch. The row joins the native erase atomically,
/// so a stored row always names a committed erase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordDeleteLedgerEntry {
    pub verb: String,
    pub at: String,
    pub caller: String,
    pub schema: String,
    pub key_fingerprint: String,
    pub records_erased: u64,
    pub file_records_erased: u64,
}

/// A primary record address. No caller can supply an arbitrary storage string.
#[derive(Clone, Debug)]
pub(crate) struct RecordAddress {
    storage: String,
    api: KeyValue,
}

impl PartialEq for RecordAddress {
    fn eq(&self, other: &Self) -> bool {
        self.storage == other.storage
    }
}
impl Eq for RecordAddress {}

impl RecordAddress {
    pub(crate) fn new(
        codec: &FieldKeyCodec,
        space: &str,
        key: &KeyValue,
    ) -> Result<Self, SchemaError> {
        let hash = key
            .hash
            .as_deref()
            .ok_or_else(|| SchemaError::InvalidData("a record key requires a hash".into()))?;
        codec
            .api_record_key(space, hash, key.range.as_deref().unwrap_or_default())
            .map(|storage| Self {
                storage,
                api: key.clone(),
            })
            .map_err(|error| SchemaError::InvalidData(format!("invalid record key: {error}")))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.storage
    }

    pub(crate) fn into_string(self) -> String {
        self.storage
    }

    pub(crate) fn api_key(&self) -> &KeyValue {
        &self.api
    }
}

impl AsRef<str> for RecordAddress {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// An alternate entry address. The family prefix and coordinate encoding are
/// selected here rather than supplied by the change producer.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct AlternateAddress(String);

impl AlternateAddress {
    pub(crate) fn new(
        codec: &FieldKeyCodec,
        field_key: &str,
        key: &KeyValue,
        primary: &RecordAddress,
    ) -> Result<Self, SchemaError> {
        use sha2::{Digest, Sha256};
        let base = alternate_range_key(codec, field_key, key)?;
        Ok(Self(format!(
            "{base}\0{:x}",
            Sha256::digest(primary.as_str().as_bytes())
        )))
    }

    pub(crate) fn into_string(self) -> String {
        self.0
    }
}

fn alternate_range_key(
    codec: &FieldKeyCodec,
    field_key: &str,
    key: &KeyValue,
) -> Result<String, SchemaError> {
    let encoded = codec
        .api_hash_range_member_prefix(
            field_key,
            key.hash.as_deref().unwrap_or_default(),
            key.range.as_deref().unwrap_or_default(),
        )
        .map_err(|_| {
            SchemaError::InvalidData("alternate key coordinate has an invalid type".into())
        })?;
    Ok(format!("{ALTERNATE_ENTRY_PREFIX}{encoded}"))
}

/// Storage bounds for a declared alternate key under exactly one hash.
pub(crate) fn alternate_bounds(
    codec: &FieldKeyCodec,
    field_key: &str,
    filter: &HashRangeFilter,
) -> Result<(String, String), SchemaError> {
    use lastdb_model::kind_partition::exact_prefix_bounds;
    let invalid = |message: &str| SchemaError::InvalidData(message.into());
    let encode = |hash: &str, range: &str| {
        alternate_range_key(
            codec,
            field_key,
            &KeyValue::new(Some(hash.to_owned()), Some(range.to_owned())),
        )
    };
    match filter {
        HashRangeFilter::HashRangeKey { hash, range } => {
            Ok(exact_prefix_bounds(&format!("{}\0", encode(hash, range)?)))
        }
        HashRangeFilter::HashKey(hash) => {
            let prefix = codec
                .api_hash_range_scan_prefix_for_hash(field_key, hash)
                .map_err(|_| invalid("invalid alternate hash"))?;
            Ok(exact_prefix_bounds(&format!(
                "{ALTERNATE_ENTRY_PREFIX}{prefix}"
            )))
        }
        HashRangeFilter::HashRangePrefix { hash, prefix } => {
            codec
                .require_text_range_prefix()
                .map_err(|_| invalid("alternate range prefix requires a text field"))?;
            Ok(exact_prefix_bounds(&encode(hash, prefix)?))
        }
        HashRangeFilter::HashRangeRange { hash, start, end } => {
            Ok((encode(hash, start)?, encode(hash, end)?))
        }
        HashRangeFilter::HashRangeKeys(_) => Err(invalid(
            "alternate reads require an exact key or a range under one hash",
        )),
    }
}

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
    pub(in crate::storage::format) async fn require_current_key_format(
        &self,
    ) -> Result<(), StorageError> {
        let controls = self
            .main_store
            .get_many(vec![
                CURRENT_KEY_FORMAT_KEY.as_bytes().to_vec(),
                DELETE_REPLAY_CUTOFF_KEY.as_bytes().to_vec(),
            ])
            .await?;
        if controls.len() != 2 {
            return Err(control::invalid("incomplete fence read"));
        }
        if !controls[0]
            .as_deref()
            .is_some_and(lastdb_model::field_key_codec::current_key_format)
        {
            return Err(StorageError::InvalidOperation(
                "the logical key format does not match this node home".into(),
            ));
        }
        control::validate_frozen_replay_floor(controls[0].as_deref(), controls[1].as_deref())
            .map(|_| ())
    }

    pub(crate) async fn record_delete_audit(
        &self,
        entry: RecordDeleteLedgerEntry,
    ) -> Result<(), SchemaError> {
        self.require_scope()?;
        let ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0).max(0);
        let key = lastdb_model::kind_partition::anchored(
            DELETE_AUDIT_FAMILY,
            &format!("{ts:020}:{}", uuid::Uuid::new_v4()),
        );
        let bytes = serde_json::to_vec(&entry)
            .map_err(|error| StorageError::SerializationError(error.to_string()))?;
        self.main_store
            .put(key.as_bytes(), bytes)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("stage erase audit: {error}")))
    }

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
        let stored = self.main_store.get_many(keys.clone()).await?;
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
        self.main_store
            .batch_mutate(
                rows.into_iter()
                    .map(|(key, _)| KvMutation::delete(key))
                    .collect(),
            )
            .await?;
        let references: BTreeMap<String, bool> = files
            .into_iter()
            .map(|key| {
                String::from_utf8(key)
                    .map(|path| (path, false))
                    .map_err(|error| {
                        SchemaError::InvalidData(format!("released file key: {error}"))
                    })
            })
            .collect::<Result<_, SchemaError>>()?;
        let files = self.metadata.stage_file_references(references).await?;
        self.stage_files(files.into_iter().map(KvMutation::delete).collect())
            .await?;
        Ok(count)
    }
}
