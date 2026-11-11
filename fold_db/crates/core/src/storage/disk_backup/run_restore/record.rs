//! Exact record and alternate lookups from the same inactive native root as the restore proof.

use super::database::{bad_request, checked_address, resource_limit};
use super::{invalid, map, read_budget, RunNodeGuard, StorageResult};
use crate::schema::types::field::HashRangeFilter;
use crate::schema::types::{KeyValue, Schema};
use crate::storage::disk_pack::native_routing;
use crate::storage::format::{
    records::{alternate_bounds, inline_files, AlternateAddress, AlternateEntry, RecordAddress},
    RECORDS_COLLECTION, SCHEMAS_NAMESPACE,
};
use cloud_backup_protocol::Digest;
use lastdb_model::field_key_id::alternate_field_key_id;
use lastdb_model::record_value::{
    InlineFile, StoredRecord, MAX_INLINE_STORED_BYTES, MAX_RECORD_BYTES,
};
use lastdb_model::storage_policy::backup_decode::{
    NATIVE_STREAM_LOOKUP_READ_BYTES, RECORD_BODY_MAX_BYTES,
};
use lastdb_model::{field_key_codec::FieldKeyCodec, record_space::record_space_uuid};
use laststore::disk_run::{home::RunRead, MAX_RUN_KEY_BYTES};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;

/// Verified alternate pointer and primary record pairs, in alternate order.
pub struct SnapshotAlternatePage {
    pub rows: Vec<(AlternateEntry, StoredRecord)>,
    pub has_more: bool,
}

/// One schema and primary key to read after the staged snapshot verifies.
pub struct StagedRecordQuery<'a> {
    schema: &'a str,
    key: KeyValue,
    include_inline: bool,
}

impl<'a> StagedRecordQuery<'a> {
    /// Bound the coordinates before starting any restore work.
    pub fn new(schema: &'a str, key: KeyValue, include_inline: bool) -> StorageResult<Self> {
        if schema.is_empty()
            || schema.len() > MAX_RUN_KEY_BYTES
            || key
                .hash
                .as_ref()
                .is_none_or(|hash| hash.is_empty() || hash.len() > MAX_RUN_KEY_BYTES)
            || key
                .range
                .as_ref()
                .is_some_and(|range| range.len() > MAX_RUN_KEY_BYTES)
        {
            return Err(invalid("staged record coordinates"));
        }
        Ok(Self {
            schema,
            key,
            include_inline,
        })
    }
}

/// Only the requested record and, when requested, its authenticated inline file.
#[derive(Serialize)]
pub struct StagedRecord {
    key: KeyValue,
    fields: BTreeMap<String, Value>,
    inline_file: Option<InlineFile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    share_author: Option<Digest>,
}

impl StagedRecord {
    /// The authenticated fields of this exact primary record.
    pub fn fields(&self) -> &BTreeMap<String, Value> {
        &self.fields
    }
}

/// A read-only native root that matches the caller's authenticated stage proof.
pub struct RunSnapshotReader(pub(in crate::storage::disk_backup::run_restore) RunNodeGuard);

impl RunSnapshotReader {
    pub fn open(
        path: &Path,
        account_key: &[u8; 32],
        verified_root: &cloud_backup_protocol::NativeRoot,
    ) -> StorageResult<Self> {
        let native = RunNodeGuard::open_snapshot(path, account_key)?;
        native.profile.validate_root(verified_root)?;
        native.require_empty_local_cas()?;
        Ok(Self(native))
    }

    pub fn read_record(
        &self,
        query: &StagedRecordQuery<'_>,
        account_key: &[u8; 32],
    ) -> StorageResult<Option<StagedRecord>> {
        read_at_root(&self.0, query, account_key)
    }

    /// Verify each alternate pointer, then batch-read its primary record.
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
        let definition = schema
            .require_record_keys()
            .ok()
            .and_then(|keys| keys.alternates.get(name))
            .ok_or_else(|| bad_request("alternate key is not declared"))?;
        let profile = definition
            .range_profile()
            .map_err(|_| invalid("staged alternate key range profile"))?;
        let field = alternate_field_key_id(schema, name);
        let mut read = self.lookup(10_000)?;
        let alternate = key_codec(&field, profile, account_key);
        let primary = record_codec(schema, account_key)?;
        let (start, end) = alternate_bounds(&alternate, &field, filter).map_err(|_| {
            bad_request("alternate reads require a valid key or range under one hash")
        })?;
        let encode = |entry: &AlternateEntry| {
            definition
                .validate_coordinates(
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
        let keys = entries
            .iter()
            .map(|entry| entry.primary.clone())
            .collect::<Vec<_>>();
        let mut records = self
            .exact_records(schema, &keys, account_key)?
            .into_iter()
            .map(|record| (record.key.clone(), record))
            .collect::<HashMap<_, _>>();
        let rows = entries
            .into_iter()
            .map(|entry| {
                let record = records
                    .remove(&entry.primary)
                    .filter(|record| {
                        record.alternates.get(name).and_then(Option::as_ref) == Some(&entry.matched)
                    })
                    .ok_or_else(|| {
                        invalid("staged alternate entry differs from its primary record")
                    })?;
                Ok((entry, record))
            })
            .collect::<StorageResult<_>>()?;
        Ok(SnapshotAlternatePage { rows, has_more })
    }
}

fn read_at_root(
    native: &RunNodeGuard,
    query: &StagedRecordQuery<'_>,
    account_key: &[u8; 32],
) -> StorageResult<Option<StagedRecord>> {
    let mut budget = read_budget();
    budget.bytes = NATIVE_STREAM_LOOKUP_READ_BYTES;
    budget.keys = 3;
    let mut read = native.main.fork_bounded(budget).map_err(map)?;
    let schema = schema(&mut read, query.schema)?;
    let codec = record_codec(&schema, account_key)?;
    let address = record_address(&schema, &codec, &query.key)?;
    let Some(record) = read_record(&mut read, &address)? else {
        return Ok(None);
    };
    check_record_address(&schema, &codec, &record, address.as_str())?;
    if !record.is_live() {
        return Err(invalid("staged record payload differs from its key"));
    }
    let inline_file = if query.include_inline {
        read_inline(&mut read, &address, &record)?
    } else {
        None
    };
    Ok(Some(StagedRecord {
        key: record.key,
        fields: record.fields,
        inline_file,
        share_author: record
            .meta
            .and_then(|mut metadata| metadata.metadata.remove("share_author"))
            .and_then(|author| Digest::try_from(author).ok()),
    }))
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
    if after
        .as_ref()
        .is_some_and(|cursor| cursor.as_str() < start || cursor.as_str() >= end)
    {
        return Err(bad_request(
            "alternate cursor is outside the requested key range",
        ));
    }
    if start >= end {
        return Ok((Vec::new(), false));
    }
    let mut range = match after {
        Some(cursor) => {
            read.administrative_range_after(RECORDS_COLLECTION, cursor.as_bytes(), end.as_bytes())
        }
        None => read.administrative_range(RECORDS_COLLECTION, start.as_bytes(), end.as_bytes()),
    }
    .map_err(map)?;
    let page = range
        .next_page_yielding_bounded(limit + 1, 0, 0, MAX_RECORD_BYTES as u64)
        .map_err(map)?;
    if page.resume.is_some() {
        return Err(resource_limit(
            "verified alternate page exceeds its read allowance",
        ));
    }
    let has_more = page.records.len() > limit;
    let entries = page
        .records
        .into_iter()
        .take(limit)
        .map(|(key, bytes)| {
            let entry: AlternateEntry = serde_json::from_slice(&bytes)
                .map_err(|_| invalid("staged alternate entry format"))?;
            if encode(&entry).ok().as_deref() != Some(key.as_str()) {
                return Err(invalid(
                    "staged alternate entry differs from its storage key",
                ));
            }
            Ok(entry)
        })
        .collect::<StorageResult<Vec<_>>>()?;
    Ok((entries, has_more))
}

pub(in crate::storage::disk_backup::run_restore) fn record_codec(
    schema: &Schema,
    account_key: &[u8; 32],
) -> StorageResult<FieldKeyCodec> {
    let primary = &schema
        .require_record_keys()
        .map_err(|_| invalid("staged schema has no record keys"))?
        .primary;
    let profile = primary
        .range_profile()
        .map_err(|_| invalid("staged schema range profile"))?;
    Ok(key_codec(
        &record_space_uuid(&schema.name),
        profile,
        account_key,
    ))
}

fn key_codec(
    field: &str,
    profile: schema_types::field_range_profile::FieldRangeProfile,
    account_key: &[u8; 32],
) -> FieldKeyCodec {
    FieldKeyCodec::from_node_key(account_key, field, profile)
}

pub(in crate::storage::disk_backup::run_restore) fn record_address(
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

pub(in crate::storage::disk_backup::run_restore) fn check_record_address(
    schema: &Schema,
    codec: &FieldKeyCodec,
    record: &StoredRecord,
    stored: &str,
) -> StorageResult<()> {
    if record_address(schema, codec, &record.key)?.as_str() != stored {
        return Err(invalid("staged record payload differs from its key"));
    }
    Ok(())
}

fn schema(read: &mut RunRead, name: &str) -> StorageResult<Schema> {
    let key = native_routing::encode(name.as_bytes());
    let bytes = read
        .get_bounded(SCHEMAS_NAMESPACE, &key, RECORD_BODY_MAX_BYTES)
        .map_err(map)?
        .ok_or_else(|| invalid("staged schema is absent"))?;
    let schema: Schema =
        serde_json::from_slice(&bytes).map_err(|_| invalid("staged schema record"))?;
    if schema.name != name {
        return Err(invalid("staged schema name differs from its key"));
    }
    Ok(schema)
}

pub(in crate::storage::disk_backup::run_restore) fn read_record(
    read: &mut RunRead,
    address: &RecordAddress,
) -> StorageResult<Option<StoredRecord>> {
    let key = native_routing::encode(address.as_str().as_bytes());
    read.get_bounded(RECORDS_COLLECTION, &key, MAX_RECORD_BYTES as u64)
        .map_err(map)?
        .map(|bytes| StoredRecord::decode(&bytes).map_err(|_| invalid("staged record value")))
        .transpose()
}

pub(in crate::storage::disk_backup::run_restore) fn read_inline(
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
    let file = inline_files::decode_for(&bytes, marker)
        .map_err(|_| invalid("staged inline file differs from its record"))?;
    Ok(Some(file))
}
