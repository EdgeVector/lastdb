//! Convert only authoritative source keys into independent org key bundles.

use super::org_rows::{self, error};
use crate::atom::{molecule_key_codec as codec, Atom, MoleculeKeyCodec};
use crate::db_operations::{DbOperations, MoleculeKeyBundle};
use crate::sync::SyncResult;
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn plaintext_hashes(atoms: &[(Atom, String)]) -> BTreeSet<String> {
    let mut values = BTreeSet::new();
    for (atom, _) in atoms {
        scalar_key_values(atom.content(), &mut values);
    }
    values
}

fn scalar_key_values(value: &serde_json::Value, values: &mut BTreeSet<String>) {
    match value {
        serde_json::Value::String(text) => {
            values.insert(text.clone());
        }
        serde_json::Value::Number(_) | serde_json::Value::Bool(_) => {
            values.insert(value.to_string());
        }
        serde_json::Value::Object(fields) => {
            for value in fields.values() {
                scalar_key_values(value, values);
            }
        }
        serde_json::Value::Array(items) => {
            for value in items {
                scalar_key_values(value, values);
            }
        }
        serde_json::Value::Null => {}
    }
}

pub(super) fn recode_row_key(
    ops: &DbOperations,
    key: &str,
    molecules: &BTreeSet<String>,
    bundles: &BTreeMap<String, MoleculeKeyBundle>,
    hashes: &BTreeSet<String>,
) -> SyncResult<String> {
    let Some(molecule) = org_rows::molecule(key) else {
        return Ok(key.into());
    };
    if !molecules.contains(molecule) {
        return Err(error("org row names an unselected molecule"));
    }
    let prefix = if key.starts_with("mk:") {
        codec::molecule_record_prefix(molecule)
    } else if key.starts_with("mgd:v1:") {
        codec::molecule_generation_delete_prefix(molecule)
    } else if let Some(rest) = key.strip_prefix(&format!("mgr:v1:{molecule}:")) {
        codec::molecule_generation_record_prefix(
            molecule,
            rest.split(':')
                .next()
                .ok_or_else(|| error("invalid generation key"))?,
        )
    } else {
        return Ok(key.into());
    };
    let suffix = key
        .strip_prefix(&prefix)
        .ok_or_else(|| error("org slot key differs"))?;
    let (hash, range) =
        codec::decode_hash_range_suffix(suffix).ok_or_else(|| error("invalid org slot suffix"))?;
    let old = ops
        .atoms()
        .key_codec_for_molecule(molecule)
        .map_err(error)?;
    let new = bundles[molecule].key_codec(&old);
    let (hash, range) = recode_slot(molecule, &hash, &range, &old, &new, hashes)?;
    let suffix = codec::hash_range_record_key(molecule, &hash, &range);
    Ok(format!(
        "{prefix}{}",
        suffix
            .strip_prefix(&codec::molecule_record_prefix(molecule))
            .ok_or_else(|| error("invalid recoded slot"))?
    ))
}

fn recode_slot(
    molecule: &str,
    hash: &str,
    range: &str,
    old: &MoleculeKeyCodec,
    new: &MoleculeKeyCodec,
    hashes: &BTreeSet<String>,
) -> SyncResult<(String, String)> {
    let hash = if hash.is_empty() {
        String::new()
    } else {
        let plain = if old.encoding() == crate::atom::HashKeyEncoding::Plain {
            hash.to_string()
        } else {
            hashes.iter().find(|candidate| old.storage_hash_read_candidates(molecule,candidate).is_ok_and(|values| values.iter().any(|value|value==hash))).cloned().ok_or_else(||error("legacy org hash has no exact key-field plaintext; source cannot be exported"))?
        };
        new.storage_hash(molecule, &plain).map_err(error)?
    };
    let range = if range.is_empty() {
        String::new()
    } else {
        let plain = crate::crypto::E2eKeys::ope_decode_range_plaintext(range)
            .unwrap_or_else(|| range.into());
        new.storage_range(molecule, &plain).map_err(error)?
    };
    Ok((hash, range))
}

pub(super) fn recode_value(
    ops: &DbOperations,
    value: &mut serde_json::Value,
    molecules: &BTreeSet<String>,
    bundles: &BTreeMap<String, MoleculeKeyBundle>,
    hashes: &BTreeSet<String>,
) -> SyncResult<()> {
    // Order-log rows encode a storage hash/range pair in their value.
    if let Some(object) = value.as_object_mut() {
        if let Some(molecule) = object
            .get("molecule_uuid")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .filter(|id| molecules.contains(id))
        {
            if let (Some(hash), Some(range)) = (
                object.get("hash").and_then(serde_json::Value::as_str),
                object.get("range").and_then(serde_json::Value::as_str),
            ) {
                let old = ops
                    .atoms()
                    .key_codec_for_molecule(&molecule)
                    .map_err(error)?;
                let new = bundles[&molecule].key_codec(&old);
                let (hash, range) = recode_slot(&molecule, hash, range, &old, &new, hashes)?;
                object.insert("hash".into(), hash.into());
                object.insert("range".into(), range.into());
            }
        }
    }
    Ok(())
}
