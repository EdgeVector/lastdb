//! Bounded operator traversal for an account source, never a product listing.

use crate::sync::{SyncError, SyncResult};
use std::collections::BTreeMap;

pub(crate) fn physical_rows(
    store: &laststore::LastStore,
    collection: &str,
) -> SyncResult<BTreeMap<String, Vec<u8>>> {
    let mut output = BTreeMap::new();
    let mut cursor = None;
    loop {
        // Every source key accepted by the exporter starts with an ASCII structural kind.
        let page = store
            .walk_all_groups(
                collection,
                "".."\u{10ffff}",
                cursor.as_ref(),
                128,
                64,
                laststore::AllGroupsPurpose::Admin,
            )
            .map_err(error)?;
        for (key, value) in page.rows {
            output.insert(key, value);
        }
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    Ok(output)
}

pub(crate) fn main_rows(store: &laststore::LastStore) -> SyncResult<BTreeMap<String, Vec<u8>>> {
    let mut output = BTreeMap::new();
    // Canonical collections win over migration residue. Exact logical reads below confirm each chosen row.
    for collection in [
        "main",
        "legacy_blob_refs",
        "mutation_history",
        "field_update_order_log",
        "field_update_order_count",
        "field_hashrange_page_index",
        "field_hashrange_hash_index",
        "field_hashrange_complete",
        "indexes",
        "tips",
        "proteins",
        "atom_locators",
        "atoms",
    ] {
        output.extend(physical_rows(store, collection)?);
    }
    Ok(output)
}

pub(crate) fn bare(key: &str) -> (&str, &str) {
    crate::kind_partition::split_org_storage_prefix(key)
        .unwrap_or((crate::db_operations::UNPREFIXED_INSTANCE_ID, key))
}

pub(super) fn molecule(key: &str) -> Option<&str> {
    let key = bare(key).1;
    for prefix in [
        "mgp:v1:",
        "mgr:v1:",
        "mgd:v1:",
        "history:",
        "history\0",
        "mord\0",
        "moc\0",
        "molprot:",
    ] {
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
