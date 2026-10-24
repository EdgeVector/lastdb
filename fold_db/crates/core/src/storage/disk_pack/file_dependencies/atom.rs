//! Shared inactive-root decoder for account and node-wrapped molecule keys.
use super::{budget_json, invalid, StorageResult};
use crate::atom::Atom;
use crate::db_operations::molecule_key_store::{
    node_bundle_storage_keys, open_frozen_node_bundle, NODE_KEY_ROW_MAX_BYTES,
};
use laststore::disk_pack::home::HomeRead;
use laststore::disk_pack::{Current, ReadBudget};
use std::borrow::Cow;
use zeroize::Zeroizing;

const VALUE_MAX: u64 = 4 * 1024 * 1024;

/// Check the key reader against the same immutable root as the supplied atom.
pub fn open_atom(
    key: &[u8; 32],
    stored: &[u8],
    current: &Current,
    read: impl FnOnce(ReadBudget) -> StorageResult<HomeRead>,
) -> StorageResult<Atom> {
    if stored.len() as u64 > VALUE_MAX {
        return Err(invalid("native atom record exceeds its bound"));
    }
    // Old records retain the account wrapper. New records expose only their
    // header and already-sealed content; neither form may skip the inner tag.
    let row = if crate::crypto::at_rest::is_sealed_at_rest(stored) {
        Cow::Owned(
            crate::crypto::at_rest::open_at_rest_bounded(key, stored, VALUE_MAX)
                .map_err(|_| invalid("native atom envelope"))?,
        )
    } else {
        Cow::Borrowed(stored)
    };
    let parts =
        crate::atom::atom_binary_row_parts(&row).map_err(|_| invalid("native atom frame"))?;
    let (mut header, sealed) = if let Some((header, sealed)) = parts {
        let header = budget_json::decode(header)?;
        if !header.is_object() || header.get("content").is_some() {
            return Err(invalid("atom binary header"));
        }
        (header, sealed.to_vec())
    } else {
        let mut header = budget_json::decode(&row)?;
        let sealed = header
            .as_object_mut()
            .and_then(|object| object.remove("content"))
            .and_then(|value| value.as_str().map(|text| text.as_bytes().to_vec()))
            .ok_or(invalid("atom content envelope"))?;
        (header, sealed)
    };
    let bundle_key = header
        .as_object_mut()
        .ok_or(invalid("atom header"))?
        .remove("molecule_key_bundle")
        .map(|value| {
            let molecule = value
                .as_str()
                .ok_or(invalid("native molecule key selector"))?;
            content_key(key, molecule, current, read)
        })
        .transpose()?;
    let content_key = bundle_key.as_deref().unwrap_or(key);
    let plain = crate::crypto::at_rest::open_at_rest_bounded(content_key, &sealed, VALUE_MAX)
        .map_err(|_| invalid("atom content key"))?;
    let content = budget_json::decode(&plain)?;
    header
        .as_object_mut()
        .ok_or(invalid("atom header"))?
        .insert("content".into(), content);
    serde_json::from_value::<Atom>(header).map_err(|_| invalid("native atom fields"))
}

fn content_key(
    key: &[u8; 32],
    molecule: &str,
    current: &Current,
    read: impl FnOnce(ReadBudget) -> StorageResult<HomeRead>,
) -> StorageResult<Zeroizing<[u8; 32]>> {
    let keys = node_bundle_storage_keys(molecule)?.map(|key| {
        (
            "molecule_keys".into(),
            crate::storage::laststore::native_routing::encode(key.as_bytes()),
        )
    });
    let rows = {
        let mut reader = read(ReadBudget {
            pages: lastdb_storage_policy::backup_decode::NATIVE_STREAM_PAGES,
            blocks: lastdb_storage_policy::backup_decode::NATIVE_STREAM_BLOCKS,
            bytes: lastdb_storage_policy::backup_decode::DIRECTORY_READ_BYTES,
            keys: 2,
        })?;
        if reader.current() != current {
            return Err(invalid("native molecule key root differs"));
        }
        reader
            .get_many_bounded(&keys, NODE_KEY_ROW_MAX_BYTES)
            .map_err(super::super::map)?
    }; // Release native blocks before any key or atom plaintext decode.
    let mut rows = rows.into_iter();
    let mut required = || {
        rows.next()
            .ok_or(invalid("native molecule key batch shape"))?
            .map_err(super::super::map)?
            .ok_or(invalid("native molecule key record is absent"))
    };
    let metadata = required()?;
    let wrap = required()?;
    if rows.next().is_some() {
        return Err(invalid("native molecule key batch shape"));
    }
    let bundle = open_frozen_node_bundle(molecule, key, &metadata, &wrap)?;
    Ok(Zeroizing::new(bundle.content_dek()))
}
