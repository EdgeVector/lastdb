//! Bounded operator traversal for an account source, never a product listing.

use crate::sync::{SyncError, SyncResult};
use std::collections::BTreeMap;

pub(crate) fn physical_keys(
    store: &laststore::LastStore,
    collection: &str,
    admission: &super::memory::Admission,
) -> SyncResult<BTreeMap<String, Vec<u8>>> {
    let mut output = BTreeMap::new();
    let mut cursor = None;
    let mut bytes = 0;
    loop {
        let page = store
            .walk_all_group_keys(collection, cursor.as_ref(), 128, |peak| {
                let total = super::memory::checked_sum(&[bytes, peak])
                    .map_err(|error| laststore::Error::Config(error.to_string()))?;
                admission
                    .check(total)
                    .map_err(|error| laststore::Error::Config(error.to_string()))
            })
            .map_err(error)?;
        for (key, value) in page.rows {
            bytes = super::memory::checked_sum(&[
                bytes,
                super::memory::checked_scale(key.len() as u64 + 128, 8)?,
            ])?;
            admission.check(bytes)?;
            output.insert(key, value);
        }
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    Ok(output)
}

pub(crate) fn physical_rows(
    store: &laststore::LastStore,
    collection: &str,
    admission: &super::memory::Admission,
) -> SyncResult<BTreeMap<String, Vec<u8>>> {
    let keys: Vec<_> = physical_keys(store, collection, admission)?
        .into_keys()
        .collect();
    let sizes = store
        .point_value_lengths(collection, &keys, |peak| {
            admission
                .check(peak)
                .map_err(|error| laststore::Error::Config(error.to_string()))
        })
        .map_err(error)?;
    let bytes = sizes.iter().flatten().try_fold(0, |sum, size| {
        super::memory::checked_sum(&[sum, size.bytes])
    })?;
    admission.check(super::memory::checked_scale(bytes, 8)?)?;
    let mut output = BTreeMap::new();
    for (key, size) in keys.into_iter().zip(sizes) {
        let size = size.ok_or_else(|| error("source metadata row disappeared"))?;
        admission.check(super::memory::checked_sum(&[
            super::memory::checked_scale(bytes, 8)?,
            size.open_peak,
        ])?)?;
        let value = store
            .load_existing_point(collection, &key)
            .map_err(error)?
            .and_then(|point| point.body)
            .ok_or_else(|| error("source metadata body disappeared"))?;
        output.insert(key, value);
    }
    Ok(output)
}

pub(crate) fn main_rows(
    store: &laststore::LastStore,
    admission: &super::memory::Admission,
) -> SyncResult<BTreeMap<String, Vec<u8>>> {
    let mut output = BTreeMap::new();
    // The exporter needs physical identities, not unrelated atom bodies.
    for collection in [
        "main",
        "legacy_blob_refs",
        "mutation_history",
        "field_hashrange_page_index",
        "field_hashrange_hash_index",
        "field_hashrange_complete",
        "indexes",
        "tips",
        "proteins",
        "atom_locators",
        "atoms",
    ] {
        let keys = physical_keys(store, collection, admission)?;
        let bytes = output
            .keys()
            .chain(keys.keys())
            .try_fold(0u64, |sum, key| {
                super::memory::checked_sum(&[sum, key.len() as u64, 128])
            })?;
        admission.check(super::memory::checked_scale(bytes, 8)?)?;
        output.extend(keys);
    }
    Ok(output)
}

pub(crate) fn bare(key: &str) -> (&str, &str) {
    crate::kind_partition::split_org_storage_prefix(key)
        .unwrap_or((crate::db_operations::UNPREFIXED_INSTANCE_ID, key))
}

pub(super) fn molecule(key: &str) -> Option<&str> {
    let key = bare(key).1;
    for prefix in ["mgp:v1:", "mgr:v1:", "mgd:v1:", "history\0", "molprot:"] {
        if let Some(rest) = key.strip_prefix(prefix) {
            return rest.split([':', '\0']).next();
        }
    }
    crate::atom::molecule_key_codec::molecule_uuid_from_storage_key(key)
}

pub(super) fn references(
    value: &serde_json::Value,
    atoms: &mut std::collections::BTreeSet<String>,
    versions: &mut std::collections::BTreeSet<String>,
) {
    match value {
        serde_json::Value::Object(fields) => {
            for (name, value) in fields {
                if name == "atom_uuid" {
                    if let Some(id) = value.as_str() {
                        atoms.insert(id.into());
                    }
                }
                if name == "prev_tip_id" {
                    if let Some(id) = value.as_str() {
                        versions.insert(id.into());
                    }
                }
                references(value, atoms, versions);
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                references(value, atoms, versions);
            }
        }
        _ => {}
    }
}

pub(super) fn error(error: impl std::fmt::Display) -> SyncError {
    SyncError::Storage(error.to_string())
}
