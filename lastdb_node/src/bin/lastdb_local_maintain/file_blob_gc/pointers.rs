//! Strict identity parsing around the production file-pointer decoder.

use fold_db::atom::atom_key_codec;
use fold_db::atom::file_pointer::{blob_refs_of_atom, LASTDB_FILE_KEY};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};

pub(super) fn valid_blob_ref(reference: &str) -> Result<(), String> {
    let hash = reference
        .strip_prefix("sha256:")
        .ok_or("file pointer has no sha256 identity")?;
    if hash.len() != 64
        || !hash
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err("file pointer has an invalid sha256 identity".into());
    }
    Ok(())
}

/// The existing storage contract prepends an opaque scope plus ':' to a
/// normal atom key. Preserve every scope; never restrict discovery to orgs.
pub(super) fn atom_identity(key: &[u8]) -> Result<Option<(String, String)>, String> {
    let Ok(text) = std::str::from_utf8(key) else {
        return Ok(None);
    };
    let mut hits = Vec::new();
    for marker in ["atom:", "atom\0"] {
        for (at, _) in text.match_indices(marker) {
            if at == 0 || text.as_bytes().get(at - 1) == Some(&b':') {
                hits.push(at);
            }
        }
    }
    if hits.is_empty() {
        return Ok(None);
    }
    if hits.len() != 1 {
        return Err("ambiguous atom storage scope".into());
    }
    let at = hits[0];
    let scope = if at == 0 { "" } else { &text[..at - 1] };
    if scope.contains('\0') {
        return Err("invalid atom storage scope".into());
    }
    let uuid = atom_key_codec::uuid_of(&text[at..])
        .filter(|uuid| !uuid.is_empty())
        .ok_or("invalid atom storage key")?;
    Ok(Some((scope.to_string(), uuid.to_string())))
}

pub(super) fn resident_blob_ref(key: &[u8]) -> Result<Option<String>, String> {
    let Ok(text) = std::str::from_utf8(key) else {
        return Ok(None);
    };
    let starts = text.strip_prefix("cas_blob:").or_else(|| {
        text.split_once(":cas_blob:")
            .map(|(_, reference)| reference)
    });
    match starts {
        Some(reference) => {
            valid_blob_ref(reference)?;
            Ok(Some(reference.into()))
        }
        None => Ok(None),
    }
}

pub(super) fn retain(
    content: &Value,
    metadata: Option<&HashMap<String, String>>,
    keep: &mut BTreeSet<String>,
) -> Result<(), String> {
    if let Some(pointer) = content.get(LASTDB_FILE_KEY) {
        let pointer = pointer.as_object().ok_or("invalid file pointer object")?;
        checked_ref(pointer.get("blob_ref"), false)?;
        if let Some(thumbnail) = pointer.get("thumbnail") {
            let thumbnail = thumbnail.as_object().ok_or("invalid thumbnail pointer")?;
            checked_ref(thumbnail.get("blob_ref"), false)?;
        }
    }
    checked_ref(content.get("$blob_ref"), true)?;
    for reference in blob_refs_of_atom(content, metadata) {
        valid_blob_ref(&reference)?;
        keep.insert(reference);
    }
    Ok(())
}

fn checked_ref(value: Option<&Value>, optional: bool) -> Result<(), String> {
    if optional && value.is_none() {
        return Ok(());
    }
    let reference = value
        .and_then(Value::as_str)
        .ok_or("file pointer has no valid blob_ref")?;
    valid_blob_ref(reference)
}
