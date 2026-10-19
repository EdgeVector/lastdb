//! The molecule rows that an erase removes, grouped by molecule and scope.

use super::targets::Target;
use super::*;
use crate::db_operations::atom_store::MoleculeHeader;

type HeaderMap = HashMap<String, MoleculeHeader>;

pub(super) async fn build_candidate_removals(
    db_ops: &Arc<DbOperations>,
    tips: Vec<Target>,
) -> Result<Vec<OwnedMoleculeKeyRemoval>, SchemaError> {
    let mut groups: std::collections::BTreeMap<(String, Option<String>), Vec<Target>> =
        std::collections::BTreeMap::new();
    for tip in tips {
        let key = (
            tip.candidate.molecule_uuid.clone(),
            tip.candidate.storage_prefix.clone(),
        );
        groups.entry(key).or_default().push(tip);
    }
    let mut header_keys = Vec::new();
    for (uuid, prefix) in groups.keys() {
        header_keys.push(build_storage_key(
            prefix.as_deref(),
            &crate::atom::molecule_key_codec::header_key(uuid),
        ));
    }
    header_keys.sort_unstable();
    header_keys.dedup();
    let headers: Vec<Option<MoleculeHeader>> = db_ops
        .atoms()
        .raw()
        .get_items(&header_keys)
        .await
        .map_err(|error| {
        SchemaError::InvalidData(format!("read Delete molecule headers: {error}"))
    })?;
    let headers: HeaderMap = header_keys
        .into_iter()
        .zip(headers)
        .filter_map(|(key, header)| header.map(|header| (key, header)))
        .collect();
    let mut removals = Vec::with_capacity(groups.len());
    for ((uuid, prefix), tips) in groups {
        let header = removal_header(&headers, &uuid, prefix.as_deref())?;
        let records = tips
            .iter()
            .map(|tip| {
                (
                    tip.candidate.hash.clone(),
                    tip.candidate.range.clone(),
                    tip.record.entry.clone(),
                    tip.record.meta.clone(),
                )
            })
            .collect();
        let mut data = crate::atom::MoleculeHashRange::from_write_records(
            uuid.clone(),
            header.version,
            header.updated_at,
            records,
        );
        let slots: Vec<_> = tips
            .into_iter()
            .map(|tip| (tip.candidate.hash, tip.candidate.range))
            .collect();
        for (hash, range) in &slots {
            data.remove_atom_uuid(hash, range);
        }
        removals.push((uuid, data, slots, prefix));
    }
    Ok(removals)
}

fn removal_header<'a>(
    headers: &'a HeaderMap,
    uuid: &str,
    prefix: Option<&str>,
) -> Result<&'a MoleculeHeader, SchemaError> {
    let key = build_storage_key(prefix, &crate::atom::molecule_key_codec::header_key(uuid));
    headers
        .get(&key)
        .ok_or_else(|| SchemaError::InvalidData("Delete target lacks its molecule header".into()))
}
