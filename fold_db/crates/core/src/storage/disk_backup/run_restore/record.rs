//! An exact record read from the same inactive native root as the restore proof.
use super::{invalid, map, RunRestoreGuard, StorageResult};
use crate::schema::types::{KeyValue, Schema};
use crate::storage::disk_pack::native_routing;
use crate::storage::format::{
    field_keys::{node_bundle_storage_keys, open_frozen_typed_bundle, NODE_KEY_ROW_MAX_BYTES},
    records::{inline_files, RecordAddress},
    FIELD_KEYS_NAMESPACE, RECORDS_COLLECTION, SCHEMAS_NAMESPACE,
};
use lastdb_model::record_space::record_space_uuid;
use lastdb_model::record_value::{
    InlineFile, StoredRecord, MAX_INLINE_STORED_BYTES, MAX_RECORD_BYTES,
};
use lastdb_storage_policy::backup_decode::{
    NATIVE_STREAM_LOOKUP_READ_BYTES, RECORD_BODY_MAX_BYTES,
};
use laststore::disk_run::{home::RunRead, MAX_RUN_KEY_BYTES};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

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
}

impl RunRestoreGuard {
    /// Read one primary key while the verified native root remains locked.
    pub fn read_staged_record(
        &self,
        query: &StagedRecordQuery<'_>,
        account_key: &[u8; 32],
    ) -> StorageResult<Option<StagedRecord>> {
        // One catalog row, two key bundle rows, one record and one inline row.
        let mut budget = super::read_budget();
        budget.bytes = NATIVE_STREAM_LOOKUP_READ_BYTES;
        budget.keys = 5;
        let mut read = self.native.main.fork_bounded(budget).map_err(map)?;
        let schema = schema(&mut read, query.schema)?;
        let primary = &schema
            .require_record_keys()
            .map_err(|_| invalid("staged schema has no record keys"))?
            .primary;
        primary
            .validate_coordinates(query.key.hash.as_deref(), query.key.range.as_deref())
            .map_err(|_| invalid("staged record key differs from its schema"))?;
        let profile = primary
            .range_profile()
            .map_err(|_| invalid("staged schema range profile"))?;
        let space = record_space_uuid(query.schema);
        let rows = node_bundle_storage_keys(&space)?.map(|key| (FIELD_KEYS_NAMESPACE.into(), key));
        let rows = read
            .get_many_bounded(&rows, NODE_KEY_ROW_MAX_BYTES)
            .map_err(map)?
            .into_iter()
            .collect::<laststore::Result<Vec<_>>>()
            .map_err(map)?;
        let [metadata, wrap] = rows.as_slice() else {
            return Err(invalid("staged record key bundle batch"));
        };
        let bundle = open_frozen_typed_bundle(
            &space,
            profile,
            account_key,
            metadata.as_deref(),
            wrap.as_deref(),
        )?;
        let codec = bundle.key_codec(profile);
        let address = RecordAddress::new(&codec, &space, &query.key)
            .map_err(|_| invalid("staged record address"))?;
        let Some(record) = read_record(&mut read, &address)? else {
            return Ok(None);
        };
        primary
            .validate_coordinates(record.key.hash.as_deref(), record.key.range.as_deref())
            .map_err(|_| invalid("staged record payload coordinates"))?;
        let stored_address = RecordAddress::new(&codec, &space, &record.key)
            .map_err(|_| invalid("staged record payload address"))?;
        if !record.is_live() || stored_address != address {
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
        }))
    }
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

fn read_record(read: &mut RunRead, address: &RecordAddress) -> StorageResult<Option<StoredRecord>> {
    let key = native_routing::encode(address.as_str().as_bytes());
    read.get_bounded(RECORDS_COLLECTION, &key, MAX_RECORD_BYTES as u64)
        .map_err(map)?
        .map(|bytes| StoredRecord::decode(&bytes).map_err(|_| invalid("staged record value")))
        .transpose()
}

fn read_inline(
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
