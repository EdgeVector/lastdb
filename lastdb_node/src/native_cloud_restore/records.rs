//! Offline format validation over the native atom namespace, never a user query.
//! An undeclared file pointer is a refusal, even when the manifest says zero.
use super::{budget_json, RestoreError, Result};
use fold_db::atom::{atom_key_codec, Atom};
use laststore::disk_pack::home::DiskHome;
use laststore::disk_pack::ReadBudget;
use std::path::Path;

// Two charged JSON values (32 MiB each), bounded native reads and four raw
// buffers stay below the aggregate 96 MiB cloud-owned working budget.
const VALUE_MAX: u64 = 4 * 1024 * 1024;

pub(super) fn verify(root: &Path, key: &[u8; 32]) -> Result<u64> {
    let home = DiskHome::open(root).map_err(|_| RestoreError::Invalid("native home"))?;
    let mut start = Vec::new();
    let mut atoms = 0u64;
    loop {
        let mut read = home
            .read(ReadBudget {
                pages: 4096,
                blocks: 16384,
                bytes: VALUE_MAX,
                keys: 10000,
            })
            .map_err(|_| RestoreError::Limit("native atom read"))?;
        let mut cursor = read
            .administrative_range("atoms", &start, &[])
            .map_err(|_| RestoreError::Limit("native atom inventory"))?;
        let page = cursor
            .next_page_yielding(1, 0, 0)
            .map_err(|_| RestoreError::Limit("native atom page"))?;
        let next = page.resume.clone().or_else(|| {
            page.records
                .last()
                .map(|record| record.key.as_bytes().to_vec())
        });
        for record in page.records {
            fold_db::storage::disk_pack::validate_restored_atom_key(&record.key)
                .map_err(|_| RestoreError::Invalid("native atom key format"))?;
            let Some(id) = atom_key_codec::body_uuid_of(&record.key) else {
                continue;
            };
            let atom = open_atom(key, &record.body)?;
            if atom.uuid() != id {
                return Err(RestoreError::Invalid("atom identity"));
            }
            if !fold_db::atom::file_pointer::blob_refs_of_atom(atom.content(), atom.metadata())
                .is_empty()
            {
                return Err(RestoreError::Unsupported(
                    "uncertified native file reference needs B2",
                ));
            }
            atoms = atoms
                .checked_add(1)
                .ok_or(RestoreError::Limit("atom inventory"))?;
        }
        if page.exhausted {
            return Ok(atoms);
        }
        let mut next = next.ok_or(RestoreError::Invalid("atom continuation"))?;
        if next.len() >= 65535 || (!start.is_empty() && next < start) {
            return Err(RestoreError::Invalid("atom continuation"));
        }
        next.push(0);
        start = next;
    }
}

fn open_atom(key: &[u8; 32], stored: &[u8]) -> Result<Atom> {
    let row = fold_db::crypto::at_rest::open_at_rest_bounded(key, stored, VALUE_MAX)
        .map_err(|_| RestoreError::Invalid("native atom envelope"))?;
    let parts = fold_db::atom::atom_binary_row_parts(&row)
        .map_err(|_| RestoreError::Invalid("native atom frame"))?;
    let (mut header, sealed) = if let Some((header, sealed)) = parts {
        let header = budget_json::decode(header)?;
        if !header.is_object() || header.get("content").is_some() {
            return Err(RestoreError::Invalid("atom binary header"));
        }
        (header, sealed.to_vec())
    } else {
        let mut header = budget_json::decode(&row)?;
        let sealed = header
            .as_object_mut()
            .and_then(|object| object.remove("content"))
            .and_then(|value| value.as_str().map(|text| text.as_bytes().to_vec()))
            .ok_or(RestoreError::Invalid("atom content envelope"))?;
        (header, sealed)
    };
    if header.get("molecule_key_bundle").is_some() {
        return Err(RestoreError::Unsupported("nonpersonal atom key mode"));
    }
    let plain = fold_db::crypto::at_rest::open_at_rest_bounded(key, &sealed, VALUE_MAX)
        .map_err(|_| RestoreError::Invalid("atom content key"))?;
    let content = budget_json::decode(&plain)?;
    header
        .as_object_mut()
        .ok_or(RestoreError::Invalid("atom header"))?
        .insert("content".into(), content);
    serde_json::from_value::<Atom>(header).map_err(|_| RestoreError::Invalid("native atom fields"))
}
