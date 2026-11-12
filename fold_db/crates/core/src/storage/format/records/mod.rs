//! Format-driven record encoding, using the request's existing commit owner.

mod file_crypto;
mod release;
mod store;

pub(crate) use file_crypto::{decrypt_file_row, require_row, seal_file_row};
pub use release::RecordDeleteLedgerEntry;
pub(crate) use release::{
    alternate_bounds, release_families, AlternateAddress, RecordAddress, ReleaseFamily, ReleaseKind,
};
pub(crate) use store::RecordFormat;

use crate::error::FoldDbError;
use crate::schema::types::{FileWrite, KeyValue, Schema};
use crate::schema::SchemaError;
use crate::storage::disk_pack::{
    record_warm::WrittenRecord, transaction, PackKvStore, PackRuntime,
};
use crate::storage::error::StorageResult;
use crate::storage::{KvMutation, StorageError};
use lastdb_model::field_key_codec::FieldKeyCodec;
use lastdb_model::field_key_id::alternate_field_key_id;
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::{
    FileMarker, InlineWrite, RecordValueError, StoredRecord, MAX_RECORD_BYTES,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;

pub const MAX_ALTERNATE_ENTRIES: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlternateEntry {
    pub primary: KeyValue,
    pub matched: KeyValue,
}

pub(in crate::storage) mod applied_deletes {
    use super::{RecordAddress, RecordFormat};
    use crate::storage::error::StorageResult;
    use crate::storage::{KvMutation, StorageError};
    use lastdb_model::portable_mutation::{AppliedDelete, Stamp};

    pub(in crate::storage) const APPLIED_DELETE_FAMILY: &str = "ad";
    const MAX_DELETE_BYTES: usize = 512;

    pub(super) fn encode(key: &RecordAddress, stamp: &Stamp) -> StorageResult<KvMutation> {
        let value = serde_json::to_vec(&AppliedDelete::from(stamp.clone())).map_err(invalid)?;
        Ok(KvMutation::put(format!("ad:{}", key.as_str()), value))
    }

    fn decode(bytes: &[u8]) -> StorageResult<Stamp> {
        if bytes.len() > MAX_DELETE_BYTES {
            return Err(invalid("encoded value exceeds its bound"));
        }
        let entry: AppliedDelete = serde_json::from_slice(bytes).map_err(invalid)?;
        entry.delete_position.validate().map_err(invalid)?;
        Ok(entry.into())
    }

    pub(crate) fn record_key(key: &str) -> Option<&str> {
        key.strip_prefix("ad:")
    }

    impl RecordFormat {
        pub(crate) async fn applied_deletes(
            &self,
            keys: &[&RecordAddress],
        ) -> StorageResult<Vec<Option<Stamp>>> {
            let req = keys
                .iter()
                .map(|k| format!("ad:{}", k.as_str()).into_bytes())
                .collect();
            let rows = self.main_store.get_many(req).await?;
            if rows.len() != keys.len() {
                return Err(invalid("incomplete applied-delete read"));
            }
            rows.iter()
                .map(|r| r.as_deref().map(decode).transpose())
                .collect()
        }
    }

    fn invalid(error: impl std::fmt::Display) -> StorageError {
        StorageError::InvalidOperation(format!("applied delete: {error}"))
    }
}

pub(in crate::storage) mod inline_files {
    use crate::storage::error::StorageResult;
    use crate::storage::format::records::RecordAddress;
    use crate::storage::{KvMutation, StorageError};
    use lastdb_model::record_value::{FileMarker, InlineFile};

    pub(in crate::storage) const INLINE_FILE_FAMILY: &str = "fi";

    pub(in crate::storage::format::records) fn put(
        key: &RecordAddress,
        file: &InlineFile,
    ) -> StorageResult<KvMutation> {
        let value = file.encode().map_err(StorageError::InvalidOperation)?;
        Ok(KvMutation::put(address(key.as_str()), value))
    }

    pub(in crate::storage::format::records) fn delete(key: &RecordAddress) -> KvMutation {
        KvMutation::delete(address(key.as_str()))
    }

    pub(crate) fn record_key(key: &str) -> Option<&str> {
        key.strip_prefix("fi:")
    }

    pub(in crate::storage) fn address(record: &str) -> String {
        format!("fi:{record}")
    }

    pub(in crate::storage) fn decode_for(
        bytes: &[u8],
        marker: &FileMarker,
    ) -> StorageResult<InlineFile> {
        let file = InlineFile::decode(bytes).map_err(StorageError::InvalidOperation)?;
        (&file.marker == marker).then_some(file).ok_or_else(|| {
            StorageError::InvalidOperation(
                "the inline file row differs from its record marker".into(),
            )
        })
    }
}

impl RecordFormat {
    pub(crate) async fn open_inline_file(
        &self,
        record_key: &str,
        marker: &FileMarker,
    ) -> Result<Option<Vec<u8>>, FoldDbError> {
        let Some(row) = self
            .main_store
            .get(inline_files::address(record_key).as_bytes())
            .await?
        else {
            return Ok(None);
        };
        inline_files::decode_for(&row, marker)?
            .bytes()
            .map(Some)
            .map_err(FoldDbError::Database)
    }
}

pub(in crate::storage::format::records) fn encode_alternates(
    schema: &Schema,
    changes: &[RecordChange],
    node_key: &[u8; 32],
) -> Result<Vec<KvMutation>, SchemaError> {
    let defs = schema.require_record_keys()?;
    if defs.alternates.is_empty() {
        return Ok(Vec::new());
    }
    let mut pointers = Vec::new();
    for change in changes {
        let before = change.before.as_ref().filter(|r| r.is_live());
        let after = change.after.is_live().then_some(&change.after);
        for (name, def) in &defs.alternates {
            let old = before.and_then(|r| r.alternates.get(name)?.as_ref());
            let new = after.and_then(|r| r.alternates.get(name)?.as_ref());
            if old == new {
                continue;
            }
            let fk = alternate_field_key_id(schema, name);
            let profile = def.range_profile().map_err(SchemaError::InvalidData)?;
            let codec = FieldKeyCodec::from_node_key(node_key, &fk, profile);
            if let Some(old) = old {
                let key = AlternateAddress::new(&codec, &fk, old, &change.key)?;
                pointers.push(KvMutation::delete(key.into_string()));
            }
            if let Some(new) = new {
                let key = AlternateAddress::new(&codec, &fk, new, &change.key)?;
                let entry = AlternateEntry {
                    primary: change.key.api_key().clone(),
                    matched: new.clone(),
                };
                let bytes = serde_json::to_vec(&entry)
                    .map_err(|_| SchemaError::InvalidData("encode alternate entry".into()))?;
                pointers.push(KvMutation::put(key.into_string(), bytes));
            }
            if pointers.len() > MAX_ALTERNATE_ENTRIES {
                return Err(SchemaError::InvalidData(
                    "alternate change batch exceeds 4096 entries".into(),
                ));
            }
        }
    }
    Ok(pointers)
}

pub(crate) struct RecordChange {
    pub key: RecordAddress,
    pub before: Option<Arc<StoredRecord>>,
    pub after: StoredRecord,
    pub file_write: Option<FileWrite>,
    pub inline_write: Option<InlineWrite>,
}

pub(in crate::storage::format::records) struct EncodedRecords {
    main: Vec<KvMutation>,
    pub(in crate::storage::format::records) files: Vec<store::FileChange>,
    written: Vec<WrittenRecord>,
}

pub(in crate::storage::format::records) fn encode(
    changes: Vec<RecordChange>,
    alternates: Vec<KvMutation>,
) -> Result<EncodedRecords, SchemaError> {
    let n = changes.len();
    let mut main = Vec::with_capacity(n + alternates.len());
    let (mut files, mut written, mut keys) = (
        Vec::with_capacity(n),
        Vec::with_capacity(n),
        HashSet::with_capacity(n),
    );
    for change in &changes {
        if !keys.insert(change.key.as_str()) {
            return Err(SchemaError::InvalidData(
                "a record batch repeats a record address".into(),
            ));
        }
    }
    for change in changes {
        if change.after.is_live() && &change.after.key != change.key.api_key() {
            return Err(SchemaError::InvalidData(
                "record payload differs from its bound primary address".into(),
            ));
        }
        let after = Arc::new(change.after);
        files.push(store::FileChange {
            key: change.key.clone(),
            after: Arc::clone(&after),
            write: change.file_write,
        });
        if after.is_live() {
            match &change.inline_write {
                Some(InlineWrite::Put(f)) if after.inline_file.as_ref() == Some(&f.marker) => {
                    main.push(inline_files::put(&change.key, f)?);
                }
                Some(InlineWrite::Clear) if after.inline_file.is_none() => {
                    main.push(inline_files::delete(&change.key));
                }
                None => {}
                Some(_) => {
                    return Err(SchemaError::InvalidData(
                        "the inline file write differs from the record marker".into(),
                    ))
                }
            }
            let value = after.encode_uncapped().map_err(record_value_error)?;
            written.push(WrittenRecord::new(change.key.as_str(), after, &value));
            main.push(KvMutation::put(change.key.into_string(), value));
        } else {
            main.push(inline_files::delete(&change.key));
            main.push(applied_deletes::encode(&change.key, &after.stamp)?);
            main.push(KvMutation::delete(change.key.into_string()));
        }
    }
    main.extend(alternates);
    Ok(EncodedRecords {
        main,
        files,
        written,
    })
}

impl EncodedRecords {
    pub(in crate::storage::format::records) async fn stage(
        self,
        main_store: &PackKvStore,
        runtime: &PackRuntime,
    ) -> Result<Vec<WrittenRecord>, SchemaError> {
        if !transaction::publication::active(runtime) {
            return Err(SchemaError::InvalidData(
                "row commits require a complete native write scope".into(),
            ));
        }
        main_store.batch_mutate(self.main).await?;
        Ok(self.written)
    }
}

pub(in crate::storage) fn require_final_record_sizes<'a>(
    max_body_bytes: usize,
    records: impl Iterator<Item = (&'a str, &'a str, usize)>,
) -> StorageResult<()> {
    if max_body_bytes <= MAX_RECORD_BYTES {
        return Ok(());
    }
    for (col, key, bytes) in records {
        if col == super::RECORDS_COLLECTION
            && key.starts_with(RECORD_PREFIX)
            && bytes > MAX_RECORD_BYTES
        {
            return Err(StorageError::RecordTooLarge {
                size: bytes,
                limit: MAX_RECORD_BYTES,
            });
        }
    }
    Ok(())
}

pub(in crate::storage::format::records) fn record_value_error(
    error: RecordValueError,
) -> SchemaError {
    match error {
        RecordValueError::TooLarge { bytes } => SchemaError::RecordTooLarge {
            size: bytes,
            limit: MAX_RECORD_BYTES,
        },
        other => SchemaError::InvalidData(format!("the record write is refused: {other}")),
    }
}
