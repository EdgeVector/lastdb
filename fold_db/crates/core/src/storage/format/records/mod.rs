//! Format-driven record encoding, addresses, and staging.

mod file_crypto;
mod store;

pub(in crate::storage) use file_crypto::{applied_deletes, inline_files};
pub(crate) use file_crypto::{decrypt_file_row, require_row, seal_file_row};
pub(crate) use store::RecordFormat;

use crate::schema::types::{field::HashRangeFilter, FileWrite, KeyValue, Schema};
use crate::schema::SchemaError;
use crate::storage::disk_pack::record_warm::WrittenRecord;
use crate::storage::error::StorageResult;
use crate::storage::{KvMutation, StorageError};
use lastdb_model::field_key_codec::FieldKeyCodec;
use lastdb_model::field_key_id::{alternate_field_key_id, ALTERNATE_ENTRY_PREFIX};
use lastdb_model::kind_partition::exact_prefix_bounds;
use lastdb_model::record_space::{record_space_uuid, RECORD_PREFIX};
use lastdb_model::record_value::{InlineWrite, RecordValueError, StoredRecord, MAX_RECORD_BYTES};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::sync::Arc;

pub const MAX_ALTERNATE_ENTRIES: usize = 4096;

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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlternateEntry {
    pub primary: KeyValue,
    pub matched: KeyValue,
}

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
            .map_err(|e| SchemaError::InvalidData(format!("invalid record key: {e}")))
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
        &self.storage
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct AlternateAddress(String);

impl AlternateAddress {
    pub(crate) fn new(
        codec: &FieldKeyCodec,
        field_key: &str,
        key: &KeyValue,
        primary: &RecordAddress,
    ) -> Result<Self, SchemaError> {
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
    let (h, r) = (
        key.hash.as_deref().unwrap_or_default(),
        key.range.as_deref().unwrap_or_default(),
    );
    codec
        .api_hash_range_member_prefix(field_key, h, r)
        .map(|s| format!("{ALTERNATE_ENTRY_PREFIX}{s}"))
        .map_err(|_| {
            SchemaError::InvalidData("alternate key coordinate has an invalid type".into())
        })
}

pub(crate) fn alternate_bounds(
    codec: &FieldKeyCodec,
    field_key: &str,
    filter: &HashRangeFilter,
) -> Result<(String, String), SchemaError> {
    let invalid = |m: &str| SchemaError::InvalidData(m.into());
    let encode = |h: &str, r: &str| {
        alternate_range_key(
            codec,
            field_key,
            &KeyValue::new(Some(h.into()), Some(r.into())),
        )
    };
    match filter {
        HashRangeFilter::HashRangeKey { hash, range } => {
            Ok(exact_prefix_bounds(&format!("{}\0", encode(hash, range)?)))
        }
        HashRangeFilter::HashKey(hash) => codec
            .api_hash_range_scan_prefix_for_hash(field_key, hash)
            .map(|p| exact_prefix_bounds(&format!("{ALTERNATE_ENTRY_PREFIX}{p}")))
            .map_err(|_| invalid("invalid alternate hash")),
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
    pub(super) prefix: String,
    pub(super) kind: ReleaseKind,
}

impl ReleaseFamily {
    pub(crate) fn prefix(&self) -> &str {
        &self.prefix
    }
    pub(crate) fn kind(&self) -> ReleaseKind {
        self.kind
    }
}

pub(crate) fn release_families(schema: &Schema) -> Result<Vec<ReleaseFamily>, SchemaError> {
    let base = format!("{RECORD_PREFIX}{}:", record_space_uuid(&schema.name));
    let (ad, inf) = (
        applied_deletes::APPLIED_DELETE_FAMILY,
        inline_files::INLINE_FILE_FAMILY,
    );
    let fam = |prefix, kind| ReleaseFamily { prefix, kind };
    let mut out = vec![
        fam(base.clone(), ReleaseKind::Records),
        fam(format!("{ad}:{base}"), ReleaseKind::AppliedDeletes),
        fam(format!("{inf}:{base}"), ReleaseKind::InlineFiles),
    ];
    for name in schema.require_record_keys()?.alternates.keys() {
        let p = format!(
            "{ALTERNATE_ENTRY_PREFIX}{}:",
            alternate_field_key_id(schema, name)
        );
        out.push(fam(p, ReleaseKind::Alternates));
    }
    Ok(out)
}

pub(super) fn encode_alternates(
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
            let codec = FieldKeyCodec::from_node_key(
                node_key,
                &fk,
                def.range_profile().map_err(SchemaError::InvalidData)?,
            );
            if let Some(old) = old {
                pointers.push(KvMutation::delete(
                    AlternateAddress::new(&codec, &fk, old, &change.key)?.into_string(),
                ));
            }
            if let Some(new) = new {
                let key = AlternateAddress::new(&codec, &fk, new, &change.key)?.into_string();
                let entry = AlternateEntry {
                    primary: change.key.api_key().clone(),
                    matched: new.clone(),
                };
                let bytes = serde_json::to_vec(&entry)
                    .map_err(|_| SchemaError::InvalidData("encode alternate entry".into()))?;
                pointers.push(KvMutation::put(key, bytes));
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

pub(super) type EncodedFile = (RecordAddress, Arc<StoredRecord>, Option<FileWrite>);

pub(super) fn encode(
    changes: Vec<RecordChange>,
    alternates: Vec<KvMutation>,
) -> Result<(Vec<KvMutation>, Vec<EncodedFile>, Vec<WrittenRecord>), SchemaError> {
    let invalid = |m: &str| SchemaError::InvalidData(m.into());
    let (n, mut seen) = (changes.len(), HashSet::with_capacity(changes.len()));
    if !changes.iter().all(|c| seen.insert(c.key.as_str())) {
        return Err(invalid("a record batch repeats a record address"));
    }
    let mut main = Vec::with_capacity(n + alternates.len());
    let (mut files, mut written) = (Vec::with_capacity(n), Vec::with_capacity(n));
    for c in changes {
        if c.after.is_live() && &c.after.key != c.key.api_key() {
            return Err(invalid(
                "record payload differs from its bound primary address",
            ));
        }
        let after = Arc::new(c.after);
        files.push((c.key.clone(), Arc::clone(&after), c.file_write));
        if after.is_live() {
            match &c.inline_write {
                Some(InlineWrite::Put(f)) if after.inline_file.as_ref() == Some(&f.marker) => {
                    main.push(inline_files::put(&c.key, f)?);
                }
                Some(InlineWrite::Clear) if after.inline_file.is_none() => {
                    main.push(inline_files::delete(&c.key));
                }
                None => {}
                Some(_) => {
                    return Err(invalid(
                        "the inline file write differs from the record marker",
                    ))
                }
            }
            let value = after.encode_uncapped().map_err(record_value_error)?;
            written.push(WrittenRecord::new(c.key.as_str(), after, &value));
            main.push(KvMutation::put(c.key.into_string(), value));
        } else {
            main.push(inline_files::delete(&c.key));
            main.push(applied_deletes::encode(&c.key, &after.stamp)?);
            main.push(KvMutation::delete(c.key.into_string()));
        }
    }
    main.extend(alternates);
    Ok((main, files, written))
}

pub(in crate::storage) fn require_final_record_sizes<'a>(
    max_body_bytes: usize,
    records: impl Iterator<Item = (&'a str, &'a str, usize)>,
) -> StorageResult<()> {
    if max_body_bytes <= MAX_RECORD_BYTES {
        return Ok(());
    }
    for (col, key, size) in records {
        if col == super::RECORDS_COLLECTION
            && key.starts_with(RECORD_PREFIX)
            && size > MAX_RECORD_BYTES
        {
            return Err(StorageError::RecordTooLarge {
                size,
                limit: MAX_RECORD_BYTES,
            });
        }
    }
    Ok(())
}

pub(super) fn record_value_error(error: RecordValueError) -> SchemaError {
    match error {
        RecordValueError::TooLarge { bytes } => SchemaError::RecordTooLarge {
            size: bytes,
            limit: MAX_RECORD_BYTES,
        },
        other => SchemaError::InvalidData(format!("the record write is refused: {other}")),
    }
}
