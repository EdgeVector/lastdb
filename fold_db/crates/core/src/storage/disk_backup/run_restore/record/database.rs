//! Database reads through the same held root as the recipient proof.

use super::*;
use crate::schema::types::field::{FilterUtils, HashRangeFilter};
use crate::schema::{SchemaNameClaim, SchemaState, SchemaWithState};
use crate::storage::format::schemas::keys::name_claim_key;
use crate::storage::format::SCHEMA_STATES_NAMESPACE;
use crate::storage::StorageError;
use lastdb_model::record_space::{record_hash_prefix, RECORD_PREFIX};
use lastdb_model::record_value::FileMarker;
use serde::de::DeserializeOwned;

/// Catalog entry and any retired descriptive-name claim in the verified copy.
pub struct SnapshotSchema {
    pub entry: SchemaWithState,
    pub name_claim_retired: bool,
}

/// One page in native record-key order.
pub struct SnapshotRecordPage {
    pub record_space: String,
    pub records: Vec<StoredRecord>,
    pub next_cursor: Option<String>,
}

impl RunSnapshotReader {
    /// Read the catalog at the held version without starting a database writer.
    pub fn schemas(&self) -> StorageResult<Vec<SnapshotSchema>> {
        const CATALOG_LIMIT: usize = 1024;
        let mut read = self.lookup(CATALOG_LIMIT * 3 + 8)?;
        let page = read
            .administrative_range(SCHEMAS_NAMESPACE, b"", &[0xff])
            .map_err(map)?
            .next_page_yielding_bounded(CATALOG_LIMIT + 1, 0, 0, RECORD_BODY_MAX_BYTES)
            .map_err(map)?;
        if !page.exhausted || page.resume.is_some() {
            return Err(resource_limit(
                "verified schema catalog exceeds its read allowance",
            ));
        }
        page.records
            .into_iter()
            .map(|(name, bytes)| {
                let schema: Schema =
                    serde_json::from_slice(&bytes).map_err(|_| invalid("staged schema record"))?;
                if native_routing::encode(schema.name.as_bytes()) != name {
                    return Err(invalid("staged schema name differs from its key"));
                }
                let state: SchemaState =
                    catalog_state(&mut read, &schema.name)?.unwrap_or_default();
                let claim: Option<SchemaNameClaim> =
                    catalog_state(&mut read, &name_claim_key(&schema.name))?;
                Ok(SnapshotSchema {
                    entry: SchemaWithState::new(schema, state),
                    name_claim_retired: claim.is_some_and(|claim| claim.retired),
                })
            })
            .collect()
    }

    /// Read a bounded group of exact primary addresses at one version.
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
        let mut read = self.lookup(keys.len() + 4)?;
        let codec = record_codec(&mut read, schema, account_key)?;
        let mut addresses = BTreeMap::new();
        for key in keys {
            let address = checked_address(schema, &codec, key)?;
            let encoded = native_routing::encode(address.as_str().as_bytes());
            addresses.insert(encoded, address);
        }
        let coordinates = addresses
            .keys()
            .map(|key| (RECORDS_COLLECTION.to_string(), key.clone()))
            .collect::<Vec<_>>();
        let values = read
            .get_many_bounded(&coordinates, MAX_RECORD_BYTES as u64)
            .map_err(map)?;
        let mut records = Vec::new();
        for (address, value) in addresses.values().zip(values) {
            if let Some(bytes) = value.map_err(map)? {
                if let Some(record) = page_record(schema, &codec, address.as_str(), &bytes)? {
                    records.push(record);
                }
            }
        }
        Ok(records)
    }

    /// Read one ordered primary key page. An unfiltered page is available only
    /// to the explicit key-list caller, which never returns record values.
    pub fn record_page(
        &self,
        schema: &Schema,
        filter: Option<&HashRangeFilter>,
        after: Option<&str>,
        limit: usize,
        account_key: &[u8; 32],
    ) -> StorageResult<SnapshotRecordPage> {
        let mut read = self.lookup(10_000)?;
        let codec = record_codec(&mut read, schema, account_key)?;
        let space = record_space_uuid(&schema.name);
        let (start, end) = page_bounds(&space, &codec, filter)?;
        if after.is_some_and(|cursor| cursor < start.as_str() || cursor >= end.as_str()) {
            return Err(bad_request("cursor is outside the Share key selection"));
        }
        if start >= end {
            return Ok(SnapshotRecordPage {
                record_space: space,
                records: Vec::new(),
                next_cursor: None,
            });
        }
        let mut range = match after {
            Some(cursor) => read.administrative_range_after(
                RECORDS_COLLECTION,
                cursor.as_bytes(),
                end.as_bytes(),
            ),
            None => read.administrative_range(RECORDS_COLLECTION, start.as_bytes(), end.as_bytes()),
        }
        .map_err(map)?;
        // One extra physical record distinguishes a full final page from a
        // full page with more work. Deleted records still advance the cursor.
        let page = range
            .next_page_yielding_bounded(limit.saturating_add(1), 0, 0, MAX_RECORD_BYTES as u64)
            .map_err(map)?;
        let has_more = page.records.len() > limit || !page.exhausted;
        let mut records = Vec::new();
        let mut last = None;
        for (key, bytes) in page.records.into_iter().take(limit) {
            if let Some(record) = page_record(schema, &codec, &key, &bytes)? {
                records.push(record);
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

    /// Read only the authenticated inline file belonging to the given record.
    pub fn inline_file(
        &self,
        schema: &Schema,
        key: &KeyValue,
        expected_revision: Option<&str>,
        account_key: &[u8; 32],
    ) -> Result<Option<(FileMarker, Vec<u8>)>, crate::error::FoldDbError> {
        let mut read = self.lookup(6)?;
        let codec = record_codec(&mut read, schema, account_key)?;
        let address = checked_address(schema, &codec, key)?;
        let record = read_record(&mut read, &address)?;
        if let Some(record) = &record {
            check_record_address(schema, &codec, record, address.as_str())?;
        }
        let live = record.as_ref().filter(|record| record.is_live());
        if let Some(expected) = expected_revision {
            let actual = live.map(|record| record.stamp.revision());
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
        let Some(record) = live else {
            return Ok(None);
        };
        read_inline(&mut read, &address, record)?
            .map(|file| {
                let bytes = file
                    .bytes()
                    .map_err(|_| invalid("staged inline file bytes"))?;
                Ok((file.marker, bytes))
            })
            .transpose()
    }

    pub(super) fn lookup(&self, keys: usize) -> StorageResult<RunRead> {
        let mut budget = super::super::read_budget();
        budget.bytes = NATIVE_STREAM_LOOKUP_READ_BYTES;
        budget.keys = keys as u64;
        self.0.main.fork_bounded(budget).map_err(map)
    }
}

fn catalog_state<T: DeserializeOwned>(read: &mut RunRead, name: &str) -> StorageResult<Option<T>> {
    let key = native_routing::encode(name.as_bytes());
    read.get_bounded(SCHEMA_STATES_NAMESPACE, &key, 256)
        .map_err(map)?
        .map(|body| serde_json::from_slice(&body).map_err(|_| invalid("staged schema state")))
        .transpose()
}

fn page_record(
    schema: &Schema,
    codec: &FieldKeyCodec,
    address: &str,
    bytes: &[u8],
) -> StorageResult<Option<StoredRecord>> {
    let record = StoredRecord::decode(bytes).map_err(|_| invalid("staged record value"))?;
    if !record.is_live() {
        return Ok(None);
    }
    check_record_address(schema, codec, &record, address)?;
    Ok(Some(record))
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
    use HashRangeFilter as Filter;
    let base = match filter {
        None => format!("{RECORD_PREFIX}{space}:"),
        Some(
            Filter::HashKey(hash)
            | Filter::HashRangePrefix { hash, .. }
            | Filter::HashRangeRange { hash, .. },
        ) if !hash.is_empty() => {
            let storage_hash = codec
                .storage_hash(space, hash)
                .map_err(|_| bad_request("invalid hash"))?;
            record_hash_prefix(space, &storage_hash)
        }
        _ => return Err(bad_request("a Share page requires one nonempty hash")),
    };
    let end_of = |prefix| {
        FilterUtils::create_prefix_end(prefix).map_err(|_| bad_request("invalid Share key prefix"))
    };
    let stored = |range| {
        codec
            .storage_range(space, range)
            .map(|range| format!("{base}{range}"))
            .map_err(|_| bad_request("range differs from the declared key type"))
    };
    match filter {
        Some(Filter::HashRangePrefix { prefix, .. }) => {
            codec
                .require_text_range_prefix()
                .map_err(|_| bad_request("range prefixes require a text key"))?;
            let start = stored(prefix)?;
            let end = end_of(&start)?;
            Ok((start, end))
        }
        Some(Filter::HashRangeRange { start, end, .. }) => Ok((stored(start)?, stored(end)?)),
        _ => {
            let end = end_of(&base)?;
            Ok((base, end))
        }
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
