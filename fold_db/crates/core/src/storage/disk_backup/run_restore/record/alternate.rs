//! A named alternate lookup through one held recipient root.

use super::database::{bad_request, checked_address, resource_limit};
use super::*;
use crate::schema::types::field::HashRangeFilter;
use crate::storage::format::records::{alternate_bounds, AlternateAddress, AlternateEntry};
use lastdb_model::field_key_id::alternate_field_key_id;
use std::collections::HashMap;

/// Verified alternate pointer and primary record pairs, in alternate order.
pub struct SnapshotAlternatePage {
    pub rows: Vec<(AlternateEntry, StoredRecord)>,
    pub has_more: bool,
}

impl RunSnapshotReader {
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
        let alternate = key_codec(&mut read, &field, profile, account_key)?;
        let primary = record_codec(&mut read, schema, account_key)?;
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
