//! Shared inactive-root decoder. It refuses nonportable molecule key bundles.
use super::{budget_json, invalid, StorageResult};
use crate::atom::Atom;
use std::borrow::Cow;

const VALUE_MAX: u64 = 4 * 1024 * 1024;

pub fn open_atom(key: &[u8; 32], stored: &[u8]) -> StorageResult<Atom> {
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
    if header.get("molecule_key_bundle").is_some() {
        return Err(invalid("nonpersonal atom key mode"));
    }
    let plain = crate::crypto::at_rest::open_at_rest_bounded(key, &sealed, VALUE_MAX)
        .map_err(|_| invalid("atom content key"))?;
    let content = budget_json::decode(&plain)?;
    header
        .as_object_mut()
        .ok_or(invalid("atom header"))?
        .insert("content".into(), content);
    serde_json::from_value::<Atom>(header).map_err(|_| invalid("native atom fields"))
}
