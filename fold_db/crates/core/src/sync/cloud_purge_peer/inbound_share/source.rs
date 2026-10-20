//! Exact inbound source bytes and bounded physical key inventory.

use super::*;

pub(super) struct InboundData {
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
    pub molecules: BTreeSet<String>,
    pub atom_keys: Vec<String>,
}

pub(super) async fn read(
    previous: &DbOperations,
    scopes: &BTreeSet<String>,
    admission: &crate::sync::cloud_purge::memory::Admission,
) -> SyncResult<InboundData> {
    let source_raw = previous
        .namespaced_store()
        .raw_last_store()
        .ok_or_else(|| error("inbound preservation requires a LastStore source"))?;
    let source = previous.open_namespace("main").await.map_err(error)?;
    let mut keys = BTreeSet::new();
    for scope in scopes {
        keys.extend(physical_keys(&source_raw, scope, admission)?);
    }
    let keys: Vec<_> = keys.into_iter().collect();
    let sizes = crate::sync::cloud_purge::memory::raw_lengths(
        admission,
        0,
        previous.namespaced_store().as_ref(),
        "main",
        &keys,
    )?;
    let bound = sizes.iter().flatten().try_fold(0u64, |sum, size| {
        crate::sync::cloud_purge::memory::checked_sum(&[
            sum,
            crate::sync::cloud_purge::memory::outer_bound(*size),
            512,
        ])
    })?;
    admission.check(crate::sync::cloud_purge::memory::checked_scale(bound, 16)?)?;
    let mut rows = Vec::new();
    let mut molecules = BTreeSet::new();
    let mut atom_keys = Vec::new();
    for page in keys.chunks(128) {
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
    Ok(InboundData {
        rows,
        molecules,
        atom_keys,
    })
}

pub(super) fn physical_keys(
    store: &laststore::LastStore,
    prefix: &str,
    admission: &crate::sync::cloud_purge::memory::Admission,
) -> SyncResult<BTreeSet<String>> {
    Ok(
        crate::sync::cloud_purge::org_rows::main_rows(store, admission)?
            .into_keys()
            .filter(|key| key.starts_with(prefix))
            .collect(),
    )
}

fn molecule(bare: &str) -> Option<&str> {
    for prefix in ["mgp:v1:", "mgr:v1:", "mgd:v1:", "history\0", "molprot:"] {
        if let Some(rest) = bare.strip_prefix(prefix) {
            return rest.split([':', '\0']).next();
        }
    }
    crate::atom::molecule_key_codec::molecule_uuid_from_storage_key(bare)
}
