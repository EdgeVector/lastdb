//! Copy reachable rows and replace legacy node-wide atom/key encryption.

use super::org_key_recode::{plaintext_hashes, recode_row_key, recode_value};
use super::org_rows::{self, error};
use crate::atom::{molecule_key_codec as codec, Atom};
use crate::db_operations::{DbOperations, MoleculeKeyBundle};
use crate::storage::KvStore;
use crate::sync::SyncResult;
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct OrgRowExport<'a> {
    pub physical: &'a BTreeMap<String, Vec<u8>>,
    pub instances: &'a BTreeMap<String, BTreeSet<String>>,
    pub replacements: &'a BTreeMap<String, String>,
    pub bundles: &'a BTreeMap<String, MoleculeKeyBundle>,
    pub admission: &'a super::memory::Admission,
    pub destination: &'a crate::storage::LastStoreNamespacedStore,
    pub target_key: &'a [u8; 32],
}

pub(super) async fn source_atom_keys(
    ops: &DbOperations,
    main: &dyn KvStore,
    physical: &BTreeMap<String, Vec<u8>>,
    instances: &BTreeMap<String, BTreeSet<String>>,
    admission: &super::memory::Admission,
) -> SyncResult<Vec<String>> {
    reserve_scope(ops, physical, instances, admission)?;
    let mut keys = BTreeSet::new();
    for (instance, molecules) in instances {
        let (mut rows, mut owners, mut versions) =
            current_rows(ops, main, instance, molecules).await?;
        expand_versions(
            ops,
            main,
            instance,
            &mut rows,
            &mut owners,
            &mut versions,
            admission,
        )
        .await?;
        keys.extend(reachable_atom_keys(physical, instance, &owners)?);
    }
    Ok(keys.into_iter().collect())
}

pub(super) async fn export_main_rows(
    ops: &DbOperations,
    main: &dyn KvStore,
    context: OrgRowExport<'_>,
) -> SyncResult<Vec<(Vec<u8>, Vec<u8>)>> {
    let OrgRowExport {
        physical,
        instances,
        replacements,
        bundles,
        target_key,
        destination,
        admission,
    } = context;
    reserve_scope(ops, physical, instances, admission)?;
    let protection = super::org_element_protection::store(destination, "main", target_key).await?;
    let mut output = Vec::new();
    let mut blob_refs = BTreeSet::new();
    for (instance, molecules) in instances {
        // Materialize only authoritative winners. Do not carry old generation
        // shadows, deletion rows, order logs, or derived completion markers.
        let (mut rows, mut owners, mut versions) =
            current_rows(ops, main, instance, molecules).await?;
        expand_versions(
            ops,
            main,
            instance,
            &mut rows,
            &mut owners,
            &mut versions,
            admission,
        )
        .await?;
        let atom_rows = read_atoms(ops, physical, instance, &owners, admission).await?;
        rows.extend(
            super::org_proteins::scoped_protein_rows(
                ops, main, physical, instance, molecules, admission,
            )
            .await?,
        );
        let hashes = plaintext_hashes(&atom_rows);
        let fresh = &replacements[instance];
        for (key, mut value) in rows {
            let key = recode_row_key(ops, &key, molecules, bundles, &hashes)?;
            recode_value(ops, &mut value, molecules, bundles, &hashes)?;
            let bytes = serde_json::to_vec(&value).map_err(error)?;
            output.push(
                super::org_element_protection::row(
                    &protection,
                    format!("{fresh}:{key}").into_bytes(),
                    bytes,
                )
                .await?,
            );
        }
        for (atom, molecule) in atom_rows {
            blob_refs.extend(crate::atom::file_pointer::blob_refs_of_atom(
                atom.content(),
                atom.metadata(),
            ));
            let mut value = serde_json::to_value(&atom).map_err(error)?;
            value
                .as_object_mut()
                .ok_or_else(|| error("invalid atom object"))?
                .insert("molecule_key_bundle".into(), molecule.clone().into());
            crate::atom::seal_atom_json(&bundles[&molecule].content_dek(), &mut value)
                .map_err(error)?;
            let bytes = serde_json::to_vec(&value).map_err(error)?;
            output.push(
                super::org_element_protection::row(
                    &protection,
                    format!(
                        "{fresh}:{}",
                        crate::atom::atom_key_codec::body_key(atom.uuid())
                    )
                    .into_bytes(),
                    bytes,
                )
                .await?,
            );
        }
    }
    super::org_files::copy_blobs(ops, &blob_refs, target_key, destination, admission).await?;
    Ok(output)
}

type OwnedRows = (
    BTreeMap<String, serde_json::Value>,
    BTreeMap<String, String>,
    BTreeMap<String, String>,
);

async fn current_rows(
    ops: &DbOperations,
    main: &dyn KvStore,
    instance: &str,
    molecules: &BTreeSet<String>,
) -> SyncResult<OwnedRows> {
    use futures::{stream, StreamExt};
    let prefix = (instance != crate::db_operations::UNPREFIXED_INSTANCE_ID).then_some(instance);
    let header_keys = molecules
        .iter()
        .map(|molecule| scoped(instance, &codec::header_key(molecule)).into_bytes())
        .collect();
    let header_values = main.get_many(header_keys).await.map_err(error)?;
    let headers: BTreeMap<_, _> = molecules.iter().cloned().zip(header_values).collect();
    let results = stream::iter(molecules.iter().map(|molecule| async move {
        let rows = ops
            .atoms()
            .load_all_mk_records(molecule, prefix)
            .await
            .map_err(error)?;
        Ok::<_, crate::sync::SyncError>((molecule.clone(), rows))
    }))
    .buffer_unordered(4)
    .collect::<Vec<_>>()
    .await;
    let (mut output, mut owners, mut versions) =
        (BTreeMap::new(), BTreeMap::new(), BTreeMap::new());
    for result in results {
        let (molecule, rows) = result?;
        let mut updated_at = chrono::Utc::now();
        let mut version = 0u64;
        for (hash, range, entry, meta) in rows {
            let value = serde_json::json!({"entry":entry,"meta":meta});
            attach_references(&value, &molecule, &mut owners, &mut versions)?;
            output.insert(
                codec::hash_range_record_key(&molecule, &hash, &range),
                value,
            );
        }
        if let Some(Some(bytes)) = headers.get(&molecule) {
            let value: serde_json::Value = serde_json::from_slice(bytes).map_err(error)?;
            if let Some(timestamp) = value.get("updated_at") {
                updated_at = serde_json::from_value(timestamp.clone()).map_err(error)?;
            }
            version = value
                .get("version")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
        }
        output.insert(
            codec::header_key(&molecule),
            serde_json::json!({"version":version,"updated_at":updated_at}),
        );
    }
    Ok((output, owners, versions))
}

fn attach_references(
    value: &serde_json::Value,
    molecule: &str,
    atoms: &mut BTreeMap<String, String>,
    versions: &mut BTreeMap<String, String>,
) -> SyncResult<()> {
    let (mut atom_ids, mut version_ids) = (BTreeSet::new(), BTreeSet::new());
    org_rows::references(value, &mut atom_ids, &mut version_ids);
    for id in atom_ids {
        atoms.entry(id).or_insert_with(|| molecule.into());
    }
    for id in version_ids {
        versions.entry(id).or_insert_with(|| molecule.into());
    }
    Ok(())
}

async fn expand_versions(
    ops: &DbOperations,
    main: &dyn KvStore,
    instance: &str,
    rows: &mut BTreeMap<String, serde_json::Value>,
    atoms: &mut BTreeMap<String, String>,
    versions: &mut BTreeMap<String, String>,
    admission: &super::memory::Admission,
) -> SyncResult<()> {
    let mut loaded = BTreeSet::new();
    loop {
        let next: Vec<_> = versions
            .iter()
            .filter(|(id, _)| !loaded.contains(*id))
            .map(|(id, owner)| (id.clone(), owner.clone()))
            .take(128)
            .collect();
        if next.is_empty() {
            break;
        }
        let keys: Vec<String> = next
            .iter()
            .map(|(id, _)| scoped(instance, &crate::kind_partition::anchored("tv", id)))
            .collect();
        reserve_keys(ops, &keys, admission)?;
        let values = main
            .get_many(keys.into_iter().map(String::into_bytes).collect())
            .await
            .map_err(error)?;
        for ((id, owner), value) in next.into_iter().zip(values) {
            let value: serde_json::Value = serde_json::from_slice(
                &value.ok_or_else(|| error("org source lacks referenced tip history"))?,
            )
            .map_err(error)?;
            attach_references(&value, &owner, atoms, versions)?;
            rows.insert(crate::kind_partition::anchored("tv", &id), value);
            loaded.insert(id);
        }
    }
    Ok(())
}

async fn read_atoms(
    ops: &DbOperations,
    physical: &BTreeMap<String, Vec<u8>>,
    instance: &str,
    owners: &BTreeMap<String, String>,
    admission: &super::memory::Admission,
) -> SyncResult<Vec<(Atom, String)>> {
    let keys = reachable_atom_keys(physical, instance, owners)?;
    reserve_keys(ops, &keys, admission)?;
    let mut output = Vec::new();
    let sizes =
        super::memory::raw_lengths(admission, 0, ops.namespaced_store().as_ref(), "main", &keys)?;
    for (key, size) in keys.iter().zip(sizes) {
        admission.check(super::memory::read_peak(
            size.ok_or_else(|| error("org atom metadata disappeared"))?,
        )?)?;
        let atoms = ops
            .atoms()
            .get_atoms_by_storage_keys(std::slice::from_ref(key))
            .await
            .map_err(error)?;
        for atom in atoms {
            let atom = atom.ok_or_else(|| error("org source atom body disappeared"))?;
            let owner = owners
                .get(atom.uuid())
                .ok_or_else(|| error("org historical atom has no scoped molecule owner"))?
                .clone();
            output.push((atom, owner));
        }
    }
    Ok(output)
}

fn reachable_atom_keys(
    physical: &BTreeMap<String, Vec<u8>>,
    instance: &str,
    owners: &BTreeMap<String, String>,
) -> SyncResult<Vec<String>> {
    let mut keys = BTreeMap::new();
    for key in physical.keys() {
        let (scope, bare) = org_rows::bare(key);
        if scope == instance {
            if let Some(id) = crate::atom::atom_key_codec::body_uuid_of(bare) {
                if owners.contains_key(id) {
                    keys.entry(id.to_string()).or_insert_with(|| key.clone());
                }
            }
        }
    }
    if keys.len() != owners.len() {
        return Err(error("org source lacks an exact reachable atom body"));
    }
    Ok(keys.into_values().collect())
}

fn scoped(instance: &str, key: &str) -> String {
    if instance == crate::db_operations::UNPREFIXED_INSTANCE_ID {
        key.into()
    } else {
        format!("{instance}:{key}")
    }
}

fn reserve_scope(
    ops: &DbOperations,
    physical: &BTreeMap<String, Vec<u8>>,
    instances: &BTreeMap<String, BTreeSet<String>>,
    admission: &super::memory::Admission,
) -> SyncResult<()> {
    let keys = physical
        .keys()
        .filter(|key| {
            let (instance, _) = org_rows::bare(key);
            instances.get(instance).is_some_and(|molecules| {
                org_rows::molecule(key).is_some_and(|molecule| molecules.contains(molecule))
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    reserve_keys(ops, &keys, admission)
}

pub(super) fn reserve_keys(
    ops: &DbOperations,
    keys: &[String],
    admission: &super::memory::Admission,
) -> SyncResult<()> {
    let sizes =
        super::memory::raw_lengths(admission, 0, ops.namespaced_store().as_ref(), "main", keys)?;
    let bound = sizes.iter().flatten().try_fold(0u64, |sum, size| {
        super::memory::checked_sum(&[sum, super::memory::outer_bound(*size), 512])
    })?;
    let open = sizes
        .iter()
        .flatten()
        .map(|size| size.open_peak)
        .max()
        .unwrap_or(0);
    admission.check(super::memory::checked_sum(&[
        super::memory::checked_scale(bound, 16)?,
        open,
    ])?)
}
