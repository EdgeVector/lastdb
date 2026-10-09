//! Molecule-scoped row measurement for the liveness bootstrap counters.

use crate::atom::molecule_key_codec;
use crate::db_operations::atom_store::{AtomStore, PerKeyRecord};
use crate::db_operations::{MoleculeStorageCounter, MoleculeTipCounterSource};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use std::collections::{HashMap, HashSet};

// lint:fn-size-ok verbatim move from schema_operations.rs; splitting this function is separate work
pub(super) async fn measure_molecule_counter(
    atoms: &AtomStore,
    molecule_uuid: &str,
    storage_prefix: Option<&str>,
) -> Result<
    (
        MoleculeStorageCounter,
        HashMap<String, MoleculeTipCounterSource>,
        HashMap<String, u64>,
    ),
    SchemaError,
> {
    let mut counter = MoleculeStorageCounter {
        molecule_uuid: molecule_uuid.to_string(),
        ..MoleculeStorageCounter::default()
    };
    let mut tip_sources = HashMap::new();
    let mut key_bytes = HashMap::new();
    let Some(molecule) = atoms
        .load_molecule_per_key(molecule_uuid, storage_prefix)
        .await?
    else {
        return Ok((counter, tip_sources, key_bytes));
    };

    counter.counter_epoch = molecule.version();
    for (hash, range, entry, meta) in molecule.per_key_records() {
        let record = PerKeyRecord {
            entry: entry.clone(),
            meta,
        };
        let tip_key = build_storage_key(
            storage_prefix,
            &molecule_key_codec::hash_range_record_key(molecule_uuid, &hash, &range),
        );
        let tip_bytes = crate::db_operations::keep_small::molecule_counter_row_bytes(&record)
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "serialize storage counter tip {molecule_uuid}: {error}"
                ))
            })?;
        let atom = atoms
            .get_atom_by_uuid(&entry.atom_uuid, storage_prefix)
            .await?
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "storage counter bootstrap found missing atom {} for molecule {molecule_uuid}",
                    entry.atom_uuid
                ))
            })?;
        let atom_bytes = serde_json::to_vec(&atom)
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "serialize storage counter atom {}: {error}",
                    entry.atom_uuid
                ))
            })?
            .len() as u64;
        let blob_bytes =
            crate::atom::file_pointer::blob_logical_bytes_of_atom(atom.content(), atom.metadata())
                .ok_or_else(|| {
                    SchemaError::InvalidData(format!(
                "storage counter bootstrap found blob reference without a logical size in atom {}",
                entry.atom_uuid
            ))
                })?;
        counter.active_slot_count = counter.active_slot_count.saturating_add(1);
        counter.active_atom_value_bytes =
            counter.active_atom_value_bytes.saturating_add(atom_bytes);
        counter.active_blob_reference_bytes = counter
            .active_blob_reference_bytes
            .saturating_add(blob_bytes);
        counter.tip_index_bytes = counter.tip_index_bytes.saturating_add(tip_bytes);
        key_bytes.insert(tip_key.clone(), tip_bytes);
        tip_sources.insert(
            tip_key,
            MoleculeTipCounterSource {
                atom_uuid: entry.atom_uuid,
                atom_value_bytes: atom_bytes,
                blob_reference_bytes: blob_bytes,
            },
        );
    }

    let structural_keys = vec![
        build_storage_key(
            storage_prefix,
            &molecule_key_codec::header_key(molecule_uuid),
        ),
        build_storage_key(
            storage_prefix,
            &molecule_key_codec::order_count_key(molecule_uuid),
        ),
    ];
    for key in structural_keys {
        if let Some(value) = atoms
            .raw()
            .get_item::<serde_json::Value>(&key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("read storage counter row {key}: {e}")))?
        {
            let bytes =
                crate::db_operations::keep_small::molecule_counter_row_bytes(&value).unwrap_or(0);
            counter.molecule_metadata_bytes = counter.molecule_metadata_bytes.saturating_add(bytes);
            key_bytes.insert(key, bytes);
        }
    }
    measure_molecule_scoped_rows(
        atoms,
        molecule_uuid,
        storage_prefix,
        &mut counter,
        &mut key_bytes,
    )
    .await?;
    Ok((counter, tip_sources, key_bytes))
}

/// The molecule-scoped structural prefixes: dense and sparse order logs,
/// page-index markers (live and legacy), and hash-key lookups.
///
/// Every prefix ends at a molecule separator (`:` or `\0`) directly after
/// the uuid, so molecule `abc` never reads rows of molecule `abcd`.
fn molecule_scoped_prefixes(molecule_uuid: &str) -> [String; 5] {
    [
        molecule_key_codec::order_log_prefix(molecule_uuid),
        molecule_key_codec::sparse_order_log_prefix(molecule_uuid),
        molecule_key_codec::hash_range_page_index_prefix(molecule_uuid),
        molecule_key_codec::legacy_hash_range_page_index_prefix(molecule_uuid),
        molecule_key_codec::hash_key_lookup_prefix(molecule_uuid),
    ]
}

/// Measure one molecule's scoped structural rows into `counter`.
///
/// Each prefix is read exactly, in both kind forms, and a returned row must
/// still sit under that prefix's logical id. A range such as
/// `colon_plane_bounds` over `mord\0{M}:` reaches rows of unrelated molecules
/// (papercut-meter-repair-order-log-prefix-crosses-molecules-20260925), so the
/// filter rejects any row a backend returns outside the prefix. Rows dedupe
/// by logical id, so an anchored row and its legacy colon twin count once.
pub(in crate::db_operations) async fn measure_molecule_scoped_rows(
    atoms: &AtomStore,
    molecule_uuid: &str,
    storage_prefix: Option<&str>,
    counter: &mut MoleculeStorageCounter,
    key_bytes: &mut HashMap<String, u64>,
) -> Result<(), SchemaError> {
    let mut seen = HashSet::new();
    for base_prefix in molecule_scoped_prefixes(molecule_uuid) {
        let prefix = build_storage_key(storage_prefix, &base_prefix);
        let logical_prefix = crate::kind_partition::logical_row_id(&prefix);
        let rows: Vec<(String, serde_json::Value)> = atoms
            .raw()
            .scan_items_with_prefix(&prefix)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!(
                    "scan storage counter rows for molecule {molecule_uuid}: {e}"
                ))
            })?;
        for (key, value) in rows {
            let logical = crate::kind_partition::logical_row_id(&key);
            if !logical.starts_with(&logical_prefix) {
                return Err(SchemaError::InvalidData(format!(
                    "storage counter scan for molecule {molecule_uuid} returned row {key:?} \
                     outside prefix {prefix:?}"
                )));
            }
            if !seen.insert(logical) {
                continue;
            }
            let bytes =
                crate::db_operations::keep_small::molecule_counter_row_bytes(&value).unwrap_or(0);
            counter.molecule_metadata_bytes = counter.molecule_metadata_bytes.saturating_add(bytes);
            key_bytes.insert(key, bytes);
        }
    }
    Ok(())
}
