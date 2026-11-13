//! Record, page, and alternate lookups from the same inactive native root as the restore proof.
use super::{invalid, map, RunSnapshotReader, StorageError, StorageResult};
use crate::schema::schema_types::SchemaWithState;
use crate::schema::types::field::{FilterUtils, HashRangeFilter};
use crate::schema::types::{KeyValue, Schema};
use crate::storage::disk_pack::native_routing;
use crate::storage::format::{
    records::{alternate_bounds, inline_files, AlternateAddress, AlternateEntry, RecordAddress},
    RECORDS_COLLECTION,
};
use lastdb_model::field_key_codec::FieldKeyCodec;
use lastdb_model::field_key_id::alternate_field_key_id;
use lastdb_model::record_space::{record_hash_prefix, record_space_uuid, RECORD_PREFIX};
use lastdb_model::record_value::{
    InlineFile, StoredRecord, MAX_INLINE_STORED_BYTES, MAX_RECORD_BYTES,
};
use laststore::disk_run::home::RunRead;
use std::collections::{BTreeMap, HashMap};

pub struct SnapshotSchema {
    pub entry: SchemaWithState,
}

pub struct SnapshotRecordPage {
    pub record_space: String,
    pub records: Vec<StoredRecord>,
    pub next_cursor: Option<String>,
}

pub struct SnapshotAlternatePage {
    pub rows: Vec<(AlternateEntry, StoredRecord)>,
    pub has_more: bool,
}

impl RunSnapshotReader {
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
        let mut addrs = BTreeMap::new();
        for key in keys {
            let addr = checked_address(schema, &codec, key)?;
            addrs.insert(native_routing::encode(addr.as_str().as_bytes()), addr);
        }
        let coords: Vec<_> = addrs
            .keys()
            .map(|k| (RECORDS_COLLECTION.into(), k.clone()))
            .collect();
        let mut out = Vec::new();
        for (addr, val) in addrs.values().zip(
            read.get_many_bounded(&coords, MAX_RECORD_BYTES as u64)
                .map_err(map)?,
        ) {
            if let Some(b) = val.map_err(map)? {
                if let Some(rec) = decode_verified(schema, &codec, addr.as_str(), &b, true)? {
                    out.push(rec);
                }
            }
        }
        Ok(out)
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
        let (space, err) = (
            record_space_uuid(&schema.name),
            "cursor is outside the Share key selection",
        );
        let (start, end) = page_bounds(&space, &codec, filter)?;
        let Some((rows, exhausted, _)) = scan_page(&mut read, &start, &end, after, limit, err)?
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
        let encode = |e: &AlternateEntry| {
            def.validate_coordinates(e.matched.hash.as_deref(), e.matched.range.as_deref())
                .map_err(|_| bad_request("alternate key differs from its schema"))?;
            let addr = checked_address(schema, &primary, &e.primary)?;
            AlternateAddress::new(&alternate, &field, &e.matched, &addr)
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
        let mut rows = Vec::with_capacity(entries.len());
        for e in entries {
            let rec = records
                .remove(&e.primary)
                .filter(|r| r.alternates.get(name).and_then(Option::as_ref) == Some(&e.matched))
                .ok_or_else(|| invalid("staged alternate entry differs from its primary record"))?;
            rows.push((e, rec));
        }
        Ok(SnapshotAlternatePage { rows, has_more })
    }
}

type RawPage = (Vec<(String, Vec<u8>)>, bool, bool);

fn scan_page(
    read: &mut RunRead,
    start: &str,
    end: &str,
    after: Option<&str>,
    limit: usize,
    err: &str,
) -> StorageResult<Option<RawPage>> {
    if after.is_some_and(|c| c < start || c >= end) {
        return Err(bad_request(err));
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
            end_of(&start).map(|end| (start, end))
        }
        Some(F::HashRangeRange { start, end, .. }) => Ok((stored(start)?, stored(end)?)),
        _ => end_of(&base).map(|end| (base, end)),
    }
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
    let err = "alternate cursor is outside the requested key range";
    let Some((rows, _, resumed)) = scan_page(read, start, end, after.as_deref(), limit, err)?
    else {
        return Ok((Vec::new(), false));
    };
    if resumed {
        return Err(resource_limit(
            "verified alternate page exceeds its read allowance",
        ));
    }
    let has_more = rows.len() > limit;
    let mut entries = Vec::new();
    for (key, bytes) in rows.into_iter().take(limit) {
        let e: AlternateEntry =
            serde_json::from_slice(&bytes).map_err(|_| invalid("staged alternate entry format"))?;
        if encode(&e).ok().as_deref() != Some(key.as_str()) {
            return Err(invalid(
                "staged alternate entry differs from its storage key",
            ));
        }
        entries.push(e);
    }
    Ok((entries, has_more))
}

pub(super) fn decode_schema(bytes: &[u8], expected_encoded_key: &str) -> StorageResult<Schema> {
    let schema: Schema =
        serde_json::from_slice(bytes).map_err(|_| invalid("staged schema record"))?;
    (native_routing::encode(schema.name.as_bytes()) == expected_encoded_key)
        .then_some(schema)
        .ok_or_else(|| invalid("staged schema name differs from its key"))
}

pub(super) fn record_codec(
    schema: &Schema,
    account_key: &[u8; 32],
) -> StorageResult<FieldKeyCodec> {
    let keys = schema
        .require_record_keys()
        .map_err(|_| invalid("staged schema has no record keys"))?;
    let profile = keys
        .primary
        .range_profile()
        .map_err(|_| invalid("staged schema range profile"))?;
    Ok(FieldKeyCodec::from_node_key(
        account_key,
        &record_space_uuid(&schema.name),
        profile,
    ))
}

pub(super) fn checked_address(
    schema: &Schema,
    codec: &FieldKeyCodec,
    key: &KeyValue,
) -> StorageResult<RecordAddress> {
    record_address(schema, codec, key)
        .map_err(|_| bad_request("record key differs from its schema"))
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

fn decode_verified(
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
    match read
        .get_bounded(RECORDS_COLLECTION, &key, MAX_RECORD_BYTES as u64)
        .map_err(map)?
    {
        Some(b) => decode_verified(schema, codec, address.as_str(), &b, false),
        None => Ok(None),
    }
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

fn bad_request(message: &str) -> StorageError {
    StorageError::InvalidOperation(message.to_string())
}

pub(super) fn resource_limit(message: &str) -> StorageError {
    StorageError::ReadResourceLimit {
        detail: message.to_string(),
    }
}
