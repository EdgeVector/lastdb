//! Database reads through the same held root as the recipient proof.

use super::record::{
    decode_schema, decode_verified, read_inline, read_verified, record_address, record_codec,
    RunSnapshotReader,
};
use super::{fork_bounded, invalid, map, StorageResult};
use crate::schema::types::field::{FilterUtils, HashRangeFilter};
use crate::schema::types::{KeyValue, Schema};
use crate::schema::{SchemaState, SchemaWithState};
use crate::storage::disk_pack::native_routing;
use crate::storage::format::{
    records::RecordAddress, RECORDS_COLLECTION, SCHEMAS_NAMESPACE, SCHEMA_STATES_NAMESPACE,
};
use crate::storage::StorageError;
use lastdb_model::field_key_codec::FieldKeyCodec;
use lastdb_model::record_space::{record_hash_prefix, record_space_uuid, RECORD_PREFIX};
use lastdb_model::record_value::{FileMarker, StoredRecord, MAX_RECORD_BYTES};
use lastdb_model::storage_policy::backup_decode::{
    NATIVE_STREAM_LOOKUP_READ_BYTES, RECORD_BODY_MAX_BYTES,
};
use laststore::disk_run::home::RunRead;
use std::collections::BTreeMap;

pub struct SnapshotSchema {
    pub entry: SchemaWithState,
}

pub struct SnapshotRecordPage {
    pub record_space: String,
    pub records: Vec<StoredRecord>,
    pub next_cursor: Option<String>,
}

pub(super) type RawPage = (Vec<(String, Vec<u8>)>, bool, bool);

pub(super) fn scan_records_page(
    read: &mut RunRead,
    start: &str,
    end: &str,
    after: Option<&str>,
    limit: usize,
    cursor_err: &str,
) -> StorageResult<Option<RawPage>> {
    if after.is_some_and(|c| c < start || c >= end) {
        return Err(bad_request(cursor_err));
    }
    if start >= end {
        return Ok(None);
    }
    let mut range = match after {
        Some(c) => {
            read.administrative_range_after(RECORDS_COLLECTION, c.as_bytes(), end.as_bytes())
        }
        None => read.administrative_range(RECORDS_COLLECTION, start.as_bytes(), end.as_bytes()),
    }
    .map_err(map)?;
    let p = range
        .next_page_yielding_bounded(limit.saturating_add(1), 0, 0, MAX_RECORD_BYTES as u64)
        .map_err(map)?;
    Ok(Some((p.records, p.exhausted, p.resume.is_some())))
}

impl RunSnapshotReader {
    pub fn schemas(&self) -> StorageResult<Vec<SnapshotSchema>> {
        const LIMIT: usize = 1024;
        let mut read = self.lookup(LIMIT * 2 + 8)?;
        let p = read
            .administrative_range(SCHEMAS_NAMESPACE, b"", &[0xff])
            .map_err(map)?
            .next_page_yielding_bounded(LIMIT + 1, 0, 0, RECORD_BODY_MAX_BYTES)
            .map_err(map)?;
        if !p.exhausted || p.resume.is_some() {
            return Err(resource_limit(
                "verified schema catalog exceeds its read allowance",
            ));
        }
        p.records
            .into_iter()
            .map(|(name, bytes)| {
                let schema = decode_schema(&bytes, &name)?;
                let state: SchemaState = read
                    .get_bounded(SCHEMA_STATES_NAMESPACE, &name, 256)
                    .map_err(map)?
                    .map(|b| serde_json::from_slice(&b).map_err(|_| invalid("staged schema state")))
                    .transpose()?
                    .unwrap_or_default();
                Ok(SnapshotSchema {
                    entry: SchemaWithState::new(schema, state),
                })
            })
            .collect()
    }

    pub fn exact_records(
        &self,
        schema: &Schema,
        keys: &[KeyValue],
        account_key: &[u8; 32],
    ) -> StorageResult<Vec<StoredRecord>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        if keys.len() > 1000 {
            return Err(bad_request("a Share exact read accepts at most 1000 keys"));
        }
        let (mut read, codec) = (
            self.lookup(keys.len() + 2)?,
            record_codec(schema, account_key)?,
        );
        let mut addresses = BTreeMap::new();
        for key in keys {
            let addr = checked_address(schema, &codec, key)?;
            addresses.insert(native_routing::encode(addr.as_str().as_bytes()), addr);
        }
        let coords: Vec<_> = addresses
            .keys()
            .map(|k| (RECORDS_COLLECTION.into(), k.clone()))
            .collect();
        let raw = read
            .get_many_bounded(&coords, MAX_RECORD_BYTES as u64)
            .map_err(map)?;
        addresses
            .values()
            .zip(raw)
            .filter_map(|(addr, val)| {
                val.map_err(map)
                    .and_then(|opt| {
                        opt.map(|b| decode_verified(schema, &codec, addr.as_str(), &b, true))
                            .transpose()
                    })
                    .map(Option::flatten)
                    .transpose()
            })
            .collect()
    }

    pub fn record_page(
        &self,
        schema: &Schema,
        filter: Option<&HashRangeFilter>,
        after: Option<&str>,
        limit: usize,
        account_key: &[u8; 32],
    ) -> StorageResult<SnapshotRecordPage> {
        let (mut read, codec) = (self.lookup(10_000)?, record_codec(schema, account_key)?);
        let space = record_space_uuid(&schema.name);
        let (start, end) = page_bounds(&space, &codec, filter)?;
        let Some((rows, exhausted, _)) = scan_records_page(
            &mut read,
            &start,
            &end,
            after,
            limit,
            "cursor is outside the Share key selection",
        )?
        else {
            return Ok(SnapshotRecordPage {
                record_space: space,
                records: Vec::new(),
                next_cursor: None,
            });
        };
        let has_more = rows.len() > limit || !exhausted;
        let (mut records, mut last) = (Vec::new(), None);
        for (key, bytes) in rows.into_iter().take(limit) {
            if let Some(rec) = decode_verified(schema, &codec, &key, &bytes, true)? {
                records.push(rec);
            }
            last = Some(key);
        }
        if last.is_none() && has_more {
            return Err(resource_limit("verified record page cannot continue"));
        }
        Ok(SnapshotRecordPage {
            record_space: space,
            records,
            next_cursor: last.filter(|_| has_more),
        })
    }

    pub fn inline_file(
        &self,
        schema: &Schema,
        key: &KeyValue,
        expected_revision: Option<&str>,
        account_key: &[u8; 32],
    ) -> Result<Option<(FileMarker, Vec<u8>)>, crate::error::FoldDbError> {
        let (mut read, codec) = (self.lookup(4)?, record_codec(schema, account_key)?);
        let address = checked_address(schema, &codec, key)?;
        let record = read_verified(&mut read, schema, &codec, &address)?;
        let live = record.as_ref().filter(|r| r.is_live());
        if let Some(expected) = expected_revision {
            let actual = live.map(|r| r.stamp.revision());
            if actual.as_deref() != Some(expected) {
                return Err(crate::schema::SchemaError::CasConflict {
                    schema: schema.name.clone(),
                    key: key.to_storage_key(),
                    expected: expected.to_owned(),
                    actual,
                }
                .into());
            }
        }
        let Some(record) = live else { return Ok(None) };
        read_inline(&mut read, &address, record)?
            .map(|f| {
                f.bytes()
                    .map(|b| (f.marker, b))
                    .map_err(|_| invalid("staged inline file bytes").into())
            })
            .transpose()
    }

    pub(super) fn lookup(&self, keys: usize) -> StorageResult<RunRead> {
        fork_bounded(&self.0.main, NATIVE_STREAM_LOOKUP_READ_BYTES, keys as u64)
    }
}

pub(super) fn checked_address(
    schema: &Schema,
    codec: &FieldKeyCodec,
    key: &KeyValue,
) -> StorageResult<RecordAddress> {
    record_address(schema, codec, key)
        .map_err(|_| bad_request("record key differs from its schema"))
}

fn page_bounds(
    space: &str,
    codec: &FieldKeyCodec,
    filter: Option<&HashRangeFilter>,
) -> StorageResult<(String, String)> {
    use HashRangeFilter as F;
    let base = match filter {
        None => format!("{RECORD_PREFIX}{space}:"),
        Some(
            F::HashKey(h) | F::HashRangePrefix { hash: h, .. } | F::HashRangeRange { hash: h, .. },
        ) if !h.is_empty() => record_hash_prefix(
            space,
            &codec
                .storage_hash(space, h)
                .map_err(|_| bad_request("invalid hash"))?,
        ),
        _ => return Err(bad_request("a Share page requires one nonempty hash")),
    };
    let end_of =
        |p| FilterUtils::create_prefix_end(p).map_err(|_| bad_request("invalid Share key prefix"));
    let stored = |r| {
        codec
            .storage_range(space, r)
            .map(|s| format!("{base}{s}"))
            .map_err(|_| bad_request("range differs from the declared key type"))
    };
    match filter {
        Some(F::HashRangePrefix { prefix, .. }) => {
            codec
                .require_text_range_prefix()
                .map_err(|_| bad_request("range prefixes require a text key"))?;
            let start = stored(prefix)?;
            Ok((start.clone(), end_of(&start)?))
        }
        Some(F::HashRangeRange { start, end, .. }) => Ok((stored(start)?, stored(end)?)),
        _ => Ok((base.clone(), end_of(&base)?)),
    }
}

pub(super) fn bad_request(message: &str) -> StorageError {
    StorageError::InvalidOperation(message.to_string())
}

pub(super) fn resource_limit(message: &str) -> StorageError {
    StorageError::ReadResourceLimit {
        detail: message.to_string(),
    }
}
