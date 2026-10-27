//! A target profile cannot silently change the existing TTL expiry clock.

use super::{invalid, InventoryLimits, MetadataRecord, StorageResult};
use crate::db_operations::schema_store::{
    frozen_retention_keys, require_typed_range_retention, SchemaRetentionAnchor,
};
use crate::schema::{Schema, SchemaRetentionPolicy};
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;

pub(super) struct RetentionCatalog<'a>(BTreeMap<&'a str, &'a MetadataRecord>);

impl<'a> RetentionCatalog<'a> {
    pub(super) fn new(records: &'a [MetadataRecord]) -> Self {
        Self(
            records
                .iter()
                .filter(|row| row.collection == "schema_states")
                .map(|row| (row.key.as_str(), row))
                .collect(),
        )
    }
}

pub(super) fn blocker(
    schema: &Schema,
    records: &RetentionCatalog<'_>,
    key: &[u8; 32],
    limits: InventoryLimits,
) -> StorageResult<Option<String>> {
    let [anchor_key, _] = frozen_retention_keys(&schema.name);
    let anchor = value::<SchemaRetentionAnchor>(records, &anchor_key, key, limits)?
        .map_or_else(|| schema.name.clone(), |row| row.anchor);
    let [_, policy_key] = frozen_retention_keys(&anchor);
    if value::<SchemaRetentionPolicy>(records, &policy_key, key, limits)?.is_none() {
        return Ok(None);
    }
    Ok(require_typed_range_retention(schema).err().map(|_| {
        "the target scalar profile cannot preserve the existing timestamp-prefix TTL policy"
            .to_owned()
    }))
}

fn value<T: DeserializeOwned>(
    records: &RetentionCatalog<'_>,
    name: &str,
    key: &[u8; 32],
    limits: InventoryLimits,
) -> StorageResult<Option<T>> {
    let Some(row) = records.0.get(name) else {
        return Ok(None);
    };
    let opened = crate::crypto::is_sealed_at_rest(&row.value)
        .then(|| {
            crate::crypto::at_rest::open_at_rest_bounded(
                key,
                &row.value,
                limits.max_metadata_bytes as u64,
            )
            .map(zeroize::Zeroizing::new)
        })
        .transpose()
        .map_err(|_| invalid("a retention policy envelope cannot be opened"))?;
    let bytes = opened
        .as_deref()
        .map_or(row.value.as_slice(), Vec::as_slice);
    serde_json::from_slice(bytes)
        .map(Some)
        .map_err(|_| invalid("retention policy metadata is unreadable"))
}
