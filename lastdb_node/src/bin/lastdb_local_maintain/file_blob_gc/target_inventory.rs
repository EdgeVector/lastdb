//! Private identity evidence before atom reclaim removes the atom content.
//!
//! Source-schema ownership is exact. A field atom first created by another
//! schema needs the preserved molecule/source retirement evidence to join its
//! ownership; this report never infers that relationship from a name pattern.

use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use fold_db::db_operations::AtomStore;
use fold_db::storage::laststore::offline_physical_collection_pair;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Serialize)]
struct AtomIdentity {
    collection: String,
    key_b64: String,
    scope: String,
    uuid: String,
    source_schema: String,
    raw_sha256: String,
    blob_refs: BTreeSet<String>,
}

#[derive(Serialize)]
struct Inventory {
    format: u32,
    home: PathBuf,
    store_root: PathBuf,
    created_at: String,
    schema_file_sha256: String,
    listed_target_schemas: Vec<String>,
    target_schema_spellings: BTreeSet<String>,
    matched_source_schemas: BTreeSet<String>,
    unmatched_listed_identities_require_source_join: Vec<String>,
    requested_atom_ids: BTreeSet<String>,
    requested_atom_ids_file_sha256: Option<String>,
    requested_atom_ids_found: BTreeSet<String>,
    requested_atom_ids_absent_from_complete_physical_walk: BTreeSet<String>,
    namespaces: Vec<String>,
    atoms_read: u64,
    target_atoms: Vec<AtomIdentity>,
    target_blob_refs: BTreeSet<String>,
    other_schema_blob_references: BTreeMap<String, Vec<AtomIdentity>>,
    scope_note: &'static str,
}

#[derive(Default)]
struct Collected {
    atoms_read: u64,
    target_atoms: Vec<AtomIdentity>,
    other_blob_refs: BTreeMap<String, Vec<AtomIdentity>>,
    requested_atom_ids_found: BTreeSet<String>,
}

#[derive(Default)]
struct RequestedAtoms {
    ids: BTreeSet<String>,
    file_sha256: Option<String>,
}

pub(super) async fn run(args: &FileBlobGcArgs, opened: &HomeStore) -> Result<(), String> {
    let path = args
        .target_schema_file
        .as_ref()
        .ok_or("target schema file is absent")?;
    let schema_bytes = std::fs::read(path).map_err(err)?;
    let identities =
        crate::reap::identities::parse(std::str::from_utf8(&schema_bytes).map_err(err)?)
            .map_err(err)?;
    let requested = read_requested_atoms(args.target_atom_ids_file.as_deref())?;
    let home = std::fs::canonicalize(&args.home).map_err(err)?;
    let store_root = std::fs::canonicalize(&opened.store_root).map_err(err)?;
    model::create_plan_dir(&args.plan_dir, &home, &store_root)?;
    let (e2e, _) = lastdb_node::offline_home::load_e2e_keys(&args.home)?;
    let decoder = AtomStore::for_offline_read(
        Arc::clone(&opened.store),
        e2e.encryption_key(),
        e2e.encryption_key(),
    )
    .await
    .map_err(err)?;
    let raw_store = opened
        .base
        .raw_last_store()
        .ok_or("home has no physical LastStore")?;
    let crypto =
        crate::home::load_home_crypto(&args.home).ok_or("home has no at-rest identity key")?;
    let mut namespaces = opened.base.list_namespaces().await.map_err(err)?;
    namespaces.sort();
    if namespaces.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err("duplicate physical namespace".into());
    }
    let mut found = Collected::default();
    for name in &namespaces {
        let (raw, seam) =
            offline_physical_collection_pair(Arc::clone(&raw_store), name, Arc::clone(&crypto));
        read_atoms(
            name,
            raw,
            seam,
            &decoder,
            &identities.spellings,
            &requested.ids,
            &mut found,
        )
        .await?;
    }
    let mut after = opened.base.list_namespaces().await.map_err(err)?;
    after.sort();
    if namespaces != after {
        return Err("physical namespaces changed during target inventory".into());
    }
    prove_stopped(args)?;
    let inventory = finish_inventory(home, store_root, namespaces, identities, requested, found);
    model::write_private(
        &args.plan_dir,
        "target-atom-blob-inventory.json",
        &inventory,
    )?;
    print_summary(args.json, &inventory);
    Ok(())
}

fn finish_inventory(
    home: PathBuf,
    store_root: PathBuf,
    namespaces: Vec<String>,
    identities: crate::reap::identities::Identities,
    requested: RequestedAtoms,
    mut found: Collected,
) -> Inventory {
    let target_blob_refs = found
        .target_atoms
        .iter()
        .flat_map(|atom| atom.blob_refs.iter().cloned())
        .collect::<BTreeSet<_>>();
    found
        .other_blob_refs
        .retain(|reference, _| target_blob_refs.contains(reference));
    found
        .target_atoms
        .sort_by(|a, b| (&a.collection, &a.key_b64).cmp(&(&b.collection, &b.key_b64)));
    let matched_source_schemas = found
        .target_atoms
        .iter()
        .filter(|atom| identities.spellings.contains(&atom.source_schema))
        .map(|atom| atom.source_schema.clone())
        .collect::<BTreeSet<_>>();
    let unmatched_listed_identities_require_source_join =
        identities.listed_without(&matched_source_schemas);
    let missing = requested
        .ids
        .difference(&found.requested_atom_ids_found)
        .cloned()
        .collect();
    Inventory {
        format: 1, home, store_root, created_at: chrono::Utc::now().to_rfc3339(),
        schema_file_sha256: identities.sha256, listed_target_schemas: identities.listed,
        target_schema_spellings: identities.spellings, matched_source_schemas,
        unmatched_listed_identities_require_source_join,
        requested_atom_ids: requested.ids, requested_atom_ids_file_sha256: requested.file_sha256,
        requested_atom_ids_found: found.requested_atom_ids_found,
        requested_atom_ids_absent_from_complete_physical_walk: missing,
        namespaces, atoms_read: found.atoms_read, target_atoms: found.target_atoms,
        target_blob_refs, other_schema_blob_references: found.other_blob_refs,
        scope_note: "all physical atom scopes; target membership is canonical schema-header aliases plus exact caller-supplied atom UUIDs; caller must validate UUID-file source provenance; unmatched schema identities still require a source join; this report grants no delete authority",
    }
}

fn print_summary(json: bool, inventory: &Inventory) {
    if json {
        println!(
            "{}",
            serde_json::json!({
                "event": "file_blob_target_inventory", "read_only": true,
                "atoms_read": inventory.atoms_read, "target_schemas": inventory.listed_target_schemas.len(),
                "target_atom_copies": inventory.target_atoms.len(),
                "target_blob_refs": inventory.target_blob_refs.len(),
                "shared_blob_refs": inventory.other_schema_blob_references.len(),
                "source_schema_ownership_only": inventory.requested_atom_ids.is_empty(),
                "unmatched_identities_require_source_join": inventory.unmatched_listed_identities_require_source_join.len(),
                "requested_atom_ids": inventory.requested_atom_ids.len(),
                "requested_atom_ids_found": inventory.requested_atom_ids_found.len(),
                "requested_atom_ids_absent_from_complete_physical_walk": inventory.requested_atom_ids_absent_from_complete_physical_walk.len(),
            })
        );
    } else {
        println!(
            "target atom copies={} file blobs={} shared file blobs={}",
            inventory.target_atoms.len(),
            inventory.target_blob_refs.len(),
            inventory.other_schema_blob_references.len()
        );
    }
}

fn read_requested_atoms(path: Option<&Path>) -> Result<RequestedAtoms, String> {
    let Some(path) = path else {
        return Ok(RequestedAtoms::default());
    };
    let bytes = std::fs::read(path).map_err(err)?;
    let mut ids = BTreeSet::new();
    for id in std::str::from_utf8(&bytes).map_err(err)?.lines() {
        if id.len() != 64
            || !id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            || !ids.insert(id.to_string())
        {
            return Err("target atom IDs require unique 64-character lowercase hex UUIDs".into());
        }
    }
    if ids.is_empty() {
        return Err("target atom ID file is empty".into());
    }
    Ok(RequestedAtoms {
        ids,
        file_sha256: Some(model::digest(&bytes)),
    })
}

async fn read_atoms(
    collection: &str,
    raw: Arc<dyn fold_db::storage::traits::KvStore>,
    seam: Arc<dyn fold_db::storage::traits::KvStore>,
    decoder: &AtomStore,
    schemas: &BTreeSet<String>,
    requested: &BTreeSet<String>,
    found: &mut Collected,
) -> Result<(), String> {
    let mut walker = crate::reap::walk::Walker::new(raw);
    while let Some(page) = walker.next_page().await.map_err(err)? {
        let mut selected = Vec::new();
        for (key, raw) in page.rows {
            if let Some((scope, uuid)) = pointers::atom_identity(&key)? {
                selected.push((key, raw, scope, uuid));
            } else if collection == "atoms" {
                return Err("atoms collection has an unsupported physical source key".into());
            }
        }
        let values = seam
            .get_many(selected.iter().map(|row| row.0.clone()).collect())
            .await
            .map_err(err)?;
        if values.len() != selected.len() {
            return Err("target atom source batch count differs".into());
        }
        let rows = values
            .into_iter()
            .map(|value| {
                value.ok_or("target atom source is hidden from the decrypted reader".to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let atoms = decoder.decode_stored_atom_batch(&rows).await.map_err(err)?;
        if atoms.len() != selected.len() {
            return Err("target atom decoder batch count differs".into());
        }
        for ((key, raw, scope, uuid), atom) in selected.into_iter().zip(atoms) {
            if atom.uuid() != uuid {
                return Err("target atom body differs from its physical key".into());
            }
            let mut blob_refs = BTreeSet::new();
            pointers::retain(atom.content(), atom.metadata(), &mut blob_refs)?;
            let row = AtomIdentity {
                collection: collection.into(),
                key_b64: STANDARD.encode(key),
                scope,
                uuid,
                source_schema: atom.source_schema_name().into(),
                raw_sha256: model::digest(&raw),
                blob_refs,
            };
            found.atoms_read += 1;
            if requested.contains(atom.uuid()) {
                found.requested_atom_ids_found.insert(atom.uuid().into());
            }
            if schemas.contains(atom.source_schema_name()) || requested.contains(atom.uuid()) {
                found.target_atoms.push(row);
            } else {
                for reference in &row.blob_refs {
                    found
                        .other_blob_refs
                        .entry(reference.clone())
                        .or_default()
                        .push(row.clone());
                }
            }
        }
    }
    Ok(())
}
