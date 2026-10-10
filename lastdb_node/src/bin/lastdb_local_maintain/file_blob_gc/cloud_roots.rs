//! Strict durable cloud roots, including legacy replayable operation formats.

use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use fold_db::db_operations::AtomStore;
use fold_db::sync::engine::{
    decode_offline_pin_log_row, offline_pin_log_restore_frontier_key, OfflinePinLogRow,
    PIN_LOG_NAMESPACE,
};
use fold_db::sync::log::{LogOp, MutationEnvelope};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
pub(super) struct Roots {
    pub blobs: BTreeSet<String>,
    pub required_atoms: BTreeSet<(String, String)>,
    pub records: u64,
    pub omitted: u64,
}

pub(super) async fn read(
    opened: &HomeStore,
    decoder: &AtomStore,
    snapshot_map: &BTreeMap<String, u64>,
) -> Result<Roots, String> {
    let raw = opened
        .base
        .open_namespace(PIN_LOG_NAMESPACE)
        .await
        .map_err(err)?;
    let seam = opened
        .store
        .open_namespace(PIN_LOG_NAMESPACE)
        .await
        .map_err(err)?;
    let mut walker = crate::reap::walk::Walker::new(Arc::clone(&raw));
    let mut roots = Roots::default();
    while let Some(page) = walker.next_page().await.map_err(err)? {
        let keys = page
            .rows
            .iter()
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        let decrypt = keys
            .iter()
            .filter(|key| key.as_slice() != offline_pin_log_restore_frontier_key())
            .cloned()
            .collect::<Vec<_>>();
        let values = seam.get_many(decrypt.clone()).await.map_err(err)?;
        if values.len() != decrypt.len() {
            return Err("cloud root batch count differs".into());
        }
        let mut values = values.into_iter();
        let mut atoms = Vec::new();
        for (key, stored) in page.rows {
            let value = if key == offline_pin_log_restore_frontier_key() {
                stored
            } else {
                values
                    .next()
                    .flatten()
                    .ok_or("cloud root is hidden from the reader")?
            };
            let records = match decode_offline_pin_log_row(&key, &value)? {
                OfflinePinLogRow::Entry(record) => vec![record],
                OfflinePinLogRow::CaptureReceipt(records) => records,
                _ => Vec::new(),
            };
            for record in records {
                if matches!(record.entry.op, LogOp::Unknown { .. }) {
                    return Err("unknown durable cloud operation prevents reclaim".into());
                }
                if record.target_id == "personal"
                    && snapshot_map
                        .get(&record.writer_id)
                        .is_some_and(|frontier| *frontier >= record.frontier_after)
                {
                    roots.omitted += 1;
                    continue;
                }
                roots.records += 1;
                observe(&record.entry.op, &mut roots, &mut atoms)?;
            }
        }
        retain_stored_atoms(decoder, &atoms, &mut roots).await?;
    }
    Ok(roots)
}

fn observe(op: &LogOp, roots: &mut Roots, atoms: &mut Vec<Vec<u8>>) -> Result<(), String> {
    match op {
        LogOp::MutationIntent { mutations } => {
            for mutation in mutations {
                intent(mutation, roots)?;
            }
        }
        LogOp::Put {
            namespace,
            key,
            value,
        } => put(namespace, key, value, roots, atoms)?,
        LogOp::BatchPut { namespace, items } => {
            for (key, value) in items {
                put(namespace, key, value, roots, atoms)?;
            }
        }
        LogOp::LogicalCommit { changes } => {
            for change in changes {
                if let Some(value) = &change.value {
                    put(&change.namespace, &change.key, value, roots, atoms)?;
                } else {
                    decode_key(&change.key)?;
                }
            }
        }
        LogOp::PhysicalDigest { items, .. } => {
            // Production replay skips this operation: the snapshot owns its
            // values. Current atom sources below protect any remaining file.
            for (key, hash) in items {
                decode_key(key)?;
                if STANDARD.decode(hash).map_err(err)?.len() != 32 {
                    return Err("invalid physical digest in cloud root".into());
                }
            }
        }
        LogOp::Delete { key, .. } => {
            decode_key(key)?;
        }
        LogOp::BatchDelete { keys, .. } => {
            for key in keys {
                decode_key(key)?;
            }
        }
        LogOp::Unknown { .. } => {
            return Err("unknown cloud operation prevents file blob reclaim".into())
        }
    }
    Ok(())
}

fn intent(mutation: &MutationEnvelope, roots: &mut Roots) -> Result<(), String> {
    match mutation.mutation_type.as_str() {
        "create" | "update" | "delete" | "purge" => {}
        _ => return Err("unknown cloud mutation type".into()),
    }
    let scope = mutation.storage_prefix.clone().unwrap_or_default();
    for (field, uuid) in &mutation.field_atom_uuids {
        if uuid.is_empty() {
            return Err("empty cloud atom identity".into());
        }
        if !mutation.fields_and_values.contains_key(field) {
            roots.required_atoms.insert((scope.clone(), uuid.clone()));
        }
    }
    if matches!(mutation.mutation_type.as_str(), "create" | "update")
        && mutation.fields_and_values.is_empty()
        && mutation.field_atom_uuids.is_empty()
    {
        return Err("cloud mutation has no complete field sources".into());
    }
    for value in mutation.fields_and_values.values() {
        pointers::retain(value, mutation.metadata.as_ref(), &mut roots.blobs)?;
    }
    pointers::retain(
        &serde_json::Value::Null,
        mutation.metadata.as_ref(),
        &mut roots.blobs,
    )
}

fn put(
    namespace: &str,
    key: &str,
    value: &str,
    roots: &mut Roots,
    atoms: &mut Vec<Vec<u8>>,
) -> Result<(), String> {
    if namespace.is_empty() {
        return Err("cloud operation has an empty namespace".into());
    }
    let key = decode_key(key)?;
    let value = STANDARD.decode(value).map_err(err)?;
    if pointers::atom_identity(&key)?.is_some() {
        atoms.push(value);
    } else if namespace == "atoms" {
        return Err("cloud atoms operation has an unsupported atom key".into());
    } else if namespace == "cas_blobs" {
        let reference = std::str::from_utf8(&key).map_err(err)?;
        pointers::valid_blob_ref(reference)?;
        roots.blobs.insert(reference.into());
    } else if let Some(reference) = pointers::resident_blob_ref(&key)? {
        roots.blobs.insert(reference);
    } else {
        // Old physical operation bags can contain atom identities in tips,
        // history, conflicts and proteins. Require their referenced bodies.
        let body: serde_json::Value = serde_json::from_slice(&value)
            .map_err(|_| "unsupported non-JSON replayable cloud root".to_string())?;
        referenced_atom_fields(&body, roots)?;
    }
    Ok(())
}

fn referenced_atom_fields(value: &serde_json::Value, roots: &mut Roots) -> Result<(), String> {
    match value {
        serde_json::Value::Object(object) => {
            if let Some(uuid) = object.get("atom_uuid") {
                let uuid = uuid
                    .as_str()
                    .filter(|id| !id.is_empty())
                    .ok_or("invalid legacy cloud atom identity")?;
                let _ = uuid;
                return Err("legacy physical cloud source has an unresolved atom scope".into());
            }
            for child in object.values() {
                referenced_atom_fields(child, roots)?;
            }
        }
        serde_json::Value::Array(array) => {
            for child in array {
                referenced_atom_fields(child, roots)?;
            }
        }
        _ => {}
    }
    pointers::retain(value, None, &mut roots.blobs)
}

async fn retain_stored_atoms(
    decoder: &AtomStore,
    atoms: &[Vec<u8>],
    roots: &mut Roots,
) -> Result<(), String> {
    for atom in decoder.decode_stored_atom_batch(atoms).await.map_err(err)? {
        pointers::retain(atom.content(), atom.metadata(), &mut roots.blobs)?;
    }
    Ok(())
}

fn decode_key(key: &str) -> Result<Vec<u8>, String> {
    STANDARD.decode(key).map_err(err)
}
