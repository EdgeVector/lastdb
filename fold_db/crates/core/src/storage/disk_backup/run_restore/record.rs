//! Exact record and alternate lookups from the same inactive native root as the restore proof.

use super::database::{bad_request, checked_address, resource_limit, scan_records_page};
use super::{invalid, map, RunNodeGuard, StorageResult};
use crate::schema::types::field::HashRangeFilter;
use crate::schema::types::{KeyValue, Schema};
use crate::storage::disk_pack::native_routing;
use crate::storage::format::{
    records::{alternate_bounds, inline_files, AlternateAddress, AlternateEntry, RecordAddress},
    RECORDS_COLLECTION, SCHEMAS_NAMESPACE,
};
use cloud_backup_protocol::{Digest, NativeRoot};
use lastdb_model::field_key_id::alternate_field_key_id;
use lastdb_model::record_value::{
    InlineFile, StoredRecord, MAX_INLINE_STORED_BYTES, MAX_RECORD_BYTES,
};
use lastdb_model::storage_policy::backup_decode::RECORD_BODY_MAX_BYTES;
use lastdb_model::{field_key_codec::FieldKeyCodec, record_space::record_space_uuid};
use laststore::disk_run::{home::RunRead, MAX_RUN_KEY_BYTES};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;

pub struct SnapshotAlternatePage {
    pub rows: Vec<(AlternateEntry, StoredRecord)>,
    pub has_more: bool,
}

pub struct StagedRecordQuery<'a> {
    schema: &'a str,
    key: KeyValue,
    include_inline: bool,
}

impl<'a> StagedRecordQuery<'a> {
    pub fn new(schema: &'a str, key: KeyValue, include_inline: bool) -> StorageResult<Self> {
        let valid = |s: &str| (1..=MAX_RUN_KEY_BYTES).contains(&s.len());
        (valid(schema)
            && key.hash.as_deref().is_some_and(valid)
            && key
                .range
                .as_ref()
                .is_none_or(|r| r.len() <= MAX_RUN_KEY_BYTES))
        .then_some(Self {
            schema,
            key,
            include_inline,
        })
        .ok_or_else(|| invalid("staged record coordinates"))
    }
}

#[derive(Serialize)]
pub struct StagedRecord {
    key: KeyValue,
    fields: BTreeMap<String, Value>,
    inline_file: Option<InlineFile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    share_author: Option<Digest>,
}

impl StagedRecord {
    pub fn fields(&self) -> &BTreeMap<String, Value> {
        &self.fields
    }
}

pub struct RunSnapshotReader(pub(super) RunNodeGuard);

impl RunSnapshotReader {
    pub fn open(
        path: &Path,
        account_key: &[u8; 32],
        verified_root: &NativeRoot,
    ) -> StorageResult<Self> {
        let native = RunNodeGuard::open(path, account_key, true)?;
        native.profile.validate_root(verified_root)?;
        native.require_empty_local_cas().map(|()| Self(native))
    }

    pub fn read_record(
        &self,
        query: &StagedRecordQuery<'_>,
        account_key: &[u8; 32],
    ) -> StorageResult<Option<StagedRecord>> {
        let (mut read, key) = (
            self.lookup(3)?,
            native_routing::encode(query.schema.as_bytes()),
        );
        let bytes = read
            .get_bounded(SCHEMAS_NAMESPACE, &key, RECORD_BODY_MAX_BYTES)
            .map_err(map)?
            .ok_or_else(|| invalid("staged schema is absent"))?;
        let schema = decode_schema(&bytes, &key)?;
        let codec = record_codec(&schema, account_key)?;
        let address = record_address(&schema, &codec, &query.key)?;
        let Some(record) = read_verified(&mut read, &schema, &codec, &address)? else {
            return Ok(None);
        };
        if !record.is_live() {
            return Err(invalid("staged record payload differs from its key"));
        }
        let inline_file = query
            .include_inline
            .then(|| read_inline(&mut read, &address, &record))
            .transpose()?
            .flatten();
        let share_author = record
            .meta
            .and_then(|mut m| m.metadata.remove("share_author"))
            .and_then(|a| Digest::try_from(a).ok());
        Ok(Some(StagedRecord {
            key: record.key,
            fields: record.fields,
            inline_file,
            share_author,
        }))
    }

    pub fn alternate_page(
        &self,
        schema: &Schema,
        name: &str,
        filter: &HashRangeFilter,
        after: Option<&AlternateEntry>,
        limit: usize,
        account_key: &[u8; 32],
    ) -> StorageResult<SnapshotAlternatePage> {
        if limit > 1000 {
            return Err(resource_limit(
                "verified alternate page exceeds its read allowance",
            ));
        }
        let def = schema
            .require_record_keys()
            .ok()
            .and_then(|keys| keys.alternates.get(name))
            .ok_or_else(|| bad_request("alternate key is not declared"))?;
        let profile = def
            .range_profile()
            .map_err(|_| invalid("staged alternate key range profile"))?;
        let (field, mut read) = (alternate_field_key_id(schema, name), self.lookup(10_000)?);
        let alternate = FieldKeyCodec::from_node_key(account_key, &field, profile);
        let primary = record_codec(schema, account_key)?;
        let (start, end) = alternate_bounds(&alternate, &field, filter).map_err(|_| {
            bad_request("alternate reads require a valid key or range under one hash")
        })?;
        let encode = |entry: &AlternateEntry| {
            def.validate_coordinates(
                entry.matched.hash.as_deref(),
                entry.matched.range.as_deref(),
            )
            .map_err(|_| bad_request("alternate key differs from its schema"))?;
            let address = checked_address(schema, &primary, &entry.primary)?;
            AlternateAddress::new(&alternate, &field, &entry.matched, &address)
                .map(AlternateAddress::into_string)
                .map_err(|_| bad_request("alternate cursor coordinates are invalid"))
        };
        let (entries, has_more) = read_entries(&mut read, &start, &end, after, limit, encode)?;
        let keys: Vec<_> = entries.iter().map(|e| e.primary.clone()).collect();
        let mut records: HashMap<_, _> = self
            .exact_records(schema, &keys, account_key)?
            .into_iter()
            .map(|r| (r.key.clone(), r))
            .collect();
        let rows = entries
            .into_iter()
            .map(|e| {
                records
                    .remove(&e.primary)
                    .filter(|r| r.alternates.get(name).and_then(Option::as_ref) == Some(&e.matched))
                    .map(|r| (e, r))
                    .ok_or_else(|| {
                        invalid("staged alternate entry differs from its primary record")
                    })
            })
            .collect::<StorageResult<_>>()?;
        Ok(SnapshotAlternatePage { rows, has_more })
    }
}

pub(super) fn decode_schema(bytes: &[u8], expected_encoded_key: &str) -> StorageResult<Schema> {
    let schema: Schema =
        serde_json::from_slice(bytes).map_err(|_| invalid("staged schema record"))?;
    (native_routing::encode(schema.name.as_bytes()) == expected_encoded_key)
        .then_some(schema)
        .ok_or_else(|| invalid("staged schema name differs from its key"))
}

fn read_entries(
    read: &mut RunRead,
    start: &str,
    end: &str,
    after: Option<&AlternateEntry>,
    limit: usize,
    encode: impl Fn(&AlternateEntry) -> StorageResult<String>,
) -> StorageResult<(Vec<AlternateEntry>, bool)> {
    let after = after.map(&encode).transpose()?;
    let Some((rows, _, resumed)) = scan_records_page(
        read,
        start,
        end,
        after.as_deref(),
        limit,
        "alternate cursor is outside the requested key range",
    )?
    else {
        return Ok((Vec::new(), false));
    };
    if resumed {
        return Err(resource_limit(
            "verified alternate page exceeds its read allowance",
        ));
    }
    let has_more = rows.len() > limit;
    let entries = rows
        .into_iter()
        .take(limit)
        .map(|(key, bytes)| {
            let e: AlternateEntry = serde_json::from_slice(&bytes)
                .map_err(|_| invalid("staged alternate entry format"))?;
            (encode(&e).ok().as_deref() == Some(key.as_str()))
                .then_some(e)
                .ok_or_else(|| invalid("staged alternate entry differs from its storage key"))
        })
        .collect::<StorageResult<_>>()?;
    Ok((entries, has_more))
}

pub(super) fn record_codec(
    schema: &Schema,
    account_key: &[u8; 32],
) -> StorageResult<FieldKeyCodec> {
    let profile = schema
        .require_record_keys()
        .map_err(|_| invalid("staged schema has no record keys"))?
        .primary
        .range_profile()
        .map_err(|_| invalid("staged schema range profile"))?;
    Ok(FieldKeyCodec::from_node_key(
        account_key,
        &record_space_uuid(&schema.name),
        profile,
    ))
}

pub(super) fn record_address(
    schema: &Schema,
    codec: &FieldKeyCodec,
    key: &KeyValue,
) -> StorageResult<RecordAddress> {
    schema
        .require_record_keys()
        .map_err(|_| invalid("staged schema has no record keys"))?
        .primary
        .validate_coordinates(key.hash.as_deref(), key.range.as_deref())
        .map_err(|_| invalid("staged record key differs from its schema"))?;
    RecordAddress::new(codec, &record_space_uuid(&schema.name), key)
        .map_err(|_| invalid("staged record address"))
}

pub(super) fn decode_verified(
    schema: &Schema,
    codec: &FieldKeyCodec,
    stored: &str,
    bytes: &[u8],
    live_only: bool,
) -> StorageResult<Option<StoredRecord>> {
    let record = StoredRecord::decode(bytes).map_err(|_| invalid("staged record value"))?;
    if live_only && !record.is_live() {
        return Ok(None);
    }
    (record_address(schema, codec, &record.key)?.as_str() == stored)
        .then_some(Some(record))
        .ok_or_else(|| invalid("staged record payload differs from its key"))
}

pub(super) fn read_verified(
    read: &mut RunRead,
    schema: &Schema,
    codec: &FieldKeyCodec,
    address: &RecordAddress,
) -> StorageResult<Option<StoredRecord>> {
    let key = native_routing::encode(address.as_str().as_bytes());
    read.get_bounded(RECORDS_COLLECTION, &key, MAX_RECORD_BYTES as u64)
        .map_err(map)?
        .map(|b| decode_verified(schema, codec, address.as_str(), &b, false))
        .transpose()
        .map(Option::flatten)
}

pub(super) fn read_inline(
    read: &mut RunRead,
    address: &RecordAddress,
    record: &StoredRecord,
) -> StorageResult<Option<InlineFile>> {
    let Some(marker) = &record.inline_file else {
        return Ok(None);
    };
    let key = native_routing::encode(inline_files::address(address.as_str()).as_bytes());
    let bytes = read
        .get_bounded(RECORDS_COLLECTION, &key, MAX_INLINE_STORED_BYTES as u64)
        .map_err(map)?
        .ok_or_else(|| invalid("staged inline file is absent"))?;
    inline_files::decode_for(&bytes, marker)
        .map(Some)
        .map_err(|_| invalid("staged inline file differs from its record"))
}
