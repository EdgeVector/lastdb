//! Preserve the stopped peer's inbound subscription data outside personal replacement.

use crate::db_operations::DbOperations;
use crate::sync::{SyncError, SyncResult};
use sha2::Digest;
use std::collections::{BTreeMap, BTreeSet};

const COLLECTIONS: &[&str] = &[
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
];

/// Both stores are stopped. The caller commits only after the complete data,
/// metadata, file, cursor, and device-consent closure passes its switch proof.
pub async fn preserve_inbound_shares(
    previous: &DbOperations,
    candidate: &DbOperations,
) -> SyncResult<(String, Vec<String>)> {
    let subscriptions = crate::sharing::store::list_share_subscriptions_in_ops(previous)
        .await
        .map_err(error)?;
    let mut scopes = BTreeSet::new();
    for subscription in &subscriptions {
        let parts: Vec<_> = subscription.share_prefix.split(':').collect();
        if parts.len() != 3
            || parts[0] != "share"
            || parts[1].len() != 32
            || !parts[1].bytes().all(|byte| byte.is_ascii_hexdigit())
            || parts[2].is_empty()
        {
            return Err(error("the prior inbound share scope is invalid"));
        }
        scopes.insert(format!("from:{}:", parts[1]));
    }
    let source_raw = previous
        .namespaced_store()
        .raw_last_store()
        .ok_or_else(|| error("inbound preservation requires a LastStore source"))?;
    let source = previous.open_namespace("main").await.map_err(error)?;
    let destination = candidate.open_namespace("main").await.map_err(error)?;
    let mut keys = BTreeSet::new();
    for scope in &scopes {
        keys.extend(physical_keys(&source_raw, scope)?);
    }
    let mut rows = Vec::new();
    let mut molecules = BTreeSet::new();
    let mut atom_keys = Vec::new();
    for page in keys.into_iter().collect::<Vec<_>>().chunks(128) {
        let values = source
            .get_many(page.iter().map(|key| key.as_bytes().to_vec()).collect())
            .await
            .map_err(error)?;
        if values.len() != page.len() {
            return Err(error("the prior inbound data batch is incomplete"));
        }
        for (key, value) in page.iter().zip(values) {
            let value = value.ok_or_else(|| error("an exact inbound source key disappeared"))?;
            let bare = scopes
                .iter()
                .find_map(|prefix| key.strip_prefix(prefix))
                .ok_or_else(|| error("an inbound key escaped its scope"))?;
            if let Some(molecule) = molecule(bare) {
                molecules.insert(molecule.to_string());
            }
            if crate::kind_partition::starts_with_kind(bare, "atom") {
                atom_keys.push(key.clone());
                let header = crate::atom::parse_atom_binary_row(&value)
                    .map_err(error)?
                    .map(|(header, _)| header)
                    .or_else(|| serde_json::from_slice::<serde_json::Value>(&value).ok());
                if let Some(molecule) = header
                    .as_ref()
                    .and_then(|header| header.get("molecule_key_bundle"))
                    .and_then(serde_json::Value::as_str)
                {
                    molecules.insert(molecule.into());
                }
            }
            rows.push((key.as_bytes().to_vec(), value));
        }
    }
    preserve_metadata(previous, candidate, &molecules).await?;
    let mut blobs = BTreeSet::new();
    for keys in atom_keys.chunks(128) {
        let atoms = previous
            .atoms()
            .get_atoms_by_storage_keys(keys)
            .await
            .map_err(error)?;
        if atoms.len() != keys.len() {
            return Err(error("the inbound atom batch is incomplete"));
        }
        for atom in atoms {
            let atom = atom.ok_or_else(|| error("an inbound atom is absent"))?;
            blobs.extend(crate::atom::file_pointer::blob_refs_of_atom(
                atom.content(),
                atom.metadata(),
            ));
        }
    }
    let mut digest = sha2::Sha256::new();
    copy_blobs(previous, candidate, &blobs, &mut digest).await?;
    // Remove another device's receiver namespace before this peer's exact copy.
    let candidate_raw = candidate
        .namespaced_store()
        .raw_last_store()
        .ok_or_else(|| error("inbound preservation requires a LastStore candidate"))?;
    let inherited = physical_keys(&candidate_raw, "from:")?
        .into_iter()
        .map(String::into_bytes)
        .collect::<Vec<_>>();
    for keys in inherited.chunks(128) {
        destination
            .batch_delete(keys.to_vec())
            .await
            .map_err(error)?;
    }
    for rows in rows.chunks(128) {
        hash(&mut digest, "main", rows);
        destination
            .restore_batch_put(rows.to_vec())
            .await
            .map_err(error)?;
    }
    candidate
        .namespaced_store()
        .restore_durability_barrier()
        .await
        .map_err(error)?;
    Ok((
        crate::hex::hex_lower(digest.finalize()),
        subscriptions
            .into_iter()
            .map(|subscription| subscription.share_prefix)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
    ))
}

fn physical_keys(store: &laststore::LastStore, prefix: &str) -> SyncResult<BTreeSet<String>> {
    let (start, end) = crate::kind_partition::exact_prefix_bounds(prefix);
    let mut output = BTreeSet::new();
    for collection in COLLECTIONS {
        let mut cursor = None;
        loop {
            let page = store
                .walk_all_groups(
                    collection,
                    start.as_str()..end.as_str(),
                    cursor.as_ref(),
                    128,
                    64,
                    laststore::AllGroupsPurpose::Admin,
                )
                .map_err(error)?;
            output.extend(page.rows.into_iter().map(|(key, _)| key));
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
    }
    Ok(output)
}

fn molecule(bare: &str) -> Option<&str> {
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
        if let Some(rest) = bare.strip_prefix(prefix) {
            return rest.split([':', '\0']).next();
        }
    }
    crate::atom::molecule_key_codec::molecule_uuid_from_storage_key(bare)
}

async fn preserve_metadata(
    previous: &DbOperations,
    candidate: &DbOperations,
    molecules: &BTreeSet<String>,
) -> SyncResult<()> {
    if molecules.is_empty() {
        return Ok(());
    }
    let mut covered = BTreeSet::new();
    let schemas = previous
        .schemas()
        .get_all_schemas_strict()
        .await
        .map_err(error)?;
    let selected: BTreeMap<_, _> = schemas
        .into_iter()
        .filter(|(_, schema)| {
            let ids = schema
                .runtime_fields
                .iter()
                .map(|(field, value)| {
                    value.common().molecule_uuid().cloned().unwrap_or_else(|| {
                        crate::atom::deterministic_molecule_uuid(&schema.name, field)
                    })
                })
                .collect::<BTreeSet<_>>();
            covered.extend(ids.intersection(molecules).cloned());
            !ids.is_disjoint(molecules)
        })
        .collect();
    if covered != *molecules {
        return Err(error(
            "prior inbound molecules lack their canonical schemas",
        ));
    }
    let existing = candidate.open_namespace("schemas").await.map_err(error)?;
    let names = selected
        .keys()
        .map(|name| name.as_bytes().to_vec())
        .collect();
    let values = existing.get_many(names).await.map_err(error)?;
    if values.len() != selected.len() {
        return Err(error("inbound schema comparison is incomplete"));
    }
    let mut definitions = Vec::new();
    for ((name, schema), current) in selected.iter().zip(values) {
        if let Some(current) = current {
            let mut current: crate::schema::Schema =
                serde_json::from_slice(&current).map_err(error)?;
            current.populate_runtime_fields().map_err(error)?;
            if serde_json::to_value(current).map_err(error)?
                != serde_json::to_value(schema).map_err(error)?
            {
                return Err(error(
                    "prior inbound schema conflicts with the adopted personal definition",
                ));
            }
        } else {
            definitions.push((
                name.as_bytes().to_vec(),
                serde_json::to_vec(schema).map_err(error)?,
            ));
        }
    }
    let source_keys = previous
        .open_namespace("molecule_keys")
        .await
        .map_err(error)?;
    candidate
        .molecule_keys()
        .preserve_local_node_bundles(source_keys.as_ref(), molecules)
        .await
        .map_err(error)?;
    existing.batch_put(definitions).await.map_err(error)?;
    let state_keys = selected
        .keys()
        .map(|name| name.as_bytes().to_vec())
        .collect::<Vec<_>>();
    let source_states = previous
        .open_namespace("schema_states")
        .await
        .map_err(error)?;
    let states = candidate
        .open_namespace("schema_states")
        .await
        .map_err(error)?;
    let (old, current) = futures::join!(
        source_states.get_many(state_keys.clone()),
        states.get_many(state_keys.clone())
    );
    let (old, current) = (old.map_err(error)?, current.map_err(error)?);
    let additions = state_keys
        .into_iter()
        .zip(old)
        .zip(current)
        .filter_map(|((key, old), current)| {
            current
                .is_none()
                .then(|| old.map(|value| (key, value)))
                .flatten()
        })
        .collect();
    states.batch_put(additions).await.map_err(error)?;
    Ok(())
}

async fn copy_blobs(
    previous: &DbOperations,
    candidate: &DbOperations,
    refs: &BTreeSet<String>,
    digest: &mut sha2::Sha256,
) -> SyncResult<()> {
    let source = previous.open_namespace("cas_blobs").await.map_err(error)?;
    let destination = candidate.open_namespace("cas_blobs").await.map_err(error)?;
    for keys in refs
        .iter()
        .map(|value| value.as_bytes().to_vec())
        .collect::<Vec<_>>()
        .chunks(4)
    {
        let (old, current) = futures::join!(
            source.get_many(keys.to_vec()),
            destination.get_many(keys.to_vec())
        );
        let (old, current) = (old.map_err(error)?, current.map_err(error)?);
        if old.len() != keys.len() || current.len() != keys.len() {
            return Err(error("inbound file batch is incomplete"));
        }
        let mut rows = Vec::new();
        for ((key, old), current) in keys.iter().zip(old).zip(current) {
            // An absent local cache remains fetchable under its unchanged subscription.
            let Some(old) = old else { continue };
            if current.as_ref().is_some_and(|current| current != &old) {
                return Err(error("a prior inbound file conflicts with adopted data"));
            }
            rows.push((key.clone(), old));
        }
        hash(digest, "cas_blobs", &rows);
        destination.restore_batch_put(rows).await.map_err(error)?;
    }
    Ok(())
}

fn hash(digest: &mut sha2::Sha256, namespace: &str, rows: &[(Vec<u8>, Vec<u8>)]) {
    digest.update((namespace.len() as u64).to_be_bytes());
    digest.update(namespace.as_bytes());
    for (key, value) in rows {
        digest.update((key.len() as u64).to_be_bytes());
        digest.update(key);
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value);
    }
}

fn error(value: impl std::fmt::Display) -> SyncError {
    SyncError::CloudPurgeHeld(value.to_string())
}
