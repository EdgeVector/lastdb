//! Exact readable physical targets for normal Delete convergence.

use super::delete_candidates::{delete_candidates_for_key, DeleteCandidate};
use super::*;
use crate::db_operations::atom_store::{MoleculeHeader, PerKeyRecord};

pub(super) struct CandidateTip {
    key_index: usize,
    candidate: DeleteCandidate,
    record: PerKeyRecord,
}

pub(super) async fn load_candidate_tips(
    db_ops: &Arc<DbOperations>,
    schema: &crate::schema::types::Schema,
    keys: &[KeyValue],
    removable: &HashSet<String>,
) -> Result<(Vec<CandidateTip>, Vec<CandidateTip>), SchemaError> {
    let mut plans = Vec::new();
    let mut seen = HashSet::new();
    for field in schema.runtime_fields.values() {
        if field.common().molecule_uuid().is_none() {
            continue;
        }
        for (key_index, key) in keys.iter().enumerate() {
            for candidate in delete_candidates_for_key(db_ops, field, key)? {
                if seen.insert(candidate.mk_key.clone()) {
                    plans.push((key_index, candidate));
                }
            }
        }
    }
    let prefix = plans
        .first()
        .and_then(|(_, candidate)| candidate.storage_prefix.as_deref());
    if plans
        .iter()
        .any(|(_, candidate)| candidate.storage_prefix.as_deref() != prefix)
    {
        return Err(SchemaError::InvalidData(
            "Delete candidates have different storage scopes".into(),
        ));
    }
    let storage_keys: Vec<_> = plans
        .iter()
        .map(|(_, candidate)| candidate.mk_key.clone())
        .collect();
    let records = db_ops
        .atoms()
        .delete_target_tips_with_generations(&storage_keys, prefix)
        .await?;
    Ok(plans
        .into_iter()
        .zip(records)
        .filter_map(|((key_index, candidate), record)| {
            record.map(|record| CandidateTip {
                key_index,
                candidate,
                record,
            })
        })
        .partition(|tip| removable.contains(&tip.candidate.mk_key)))
}

pub(super) fn trace_candidate_tips(
    db_ops: &Arc<DbOperations>,
    tips: &[CandidateTip],
) -> Result<super::converge_plan::ConvergeTrace, SchemaError> {
    let mut trace = super::converge_plan::ConvergeTrace {
        traced: HashSet::new(),
        atom_ref_v2_edge_keys: Vec::new(),
    };
    for tip in tips {
        trace.traced.insert(tip.key_index);
        trace
            .atom_ref_v2_edge_keys
            .push(db_ops.atoms().atom_ref_tip_edge_v2_key(
                &tip.candidate.molecule_uuid,
                &tip.candidate.hash,
                &tip.candidate.range,
                &tip.record.entry,
                tip.candidate.storage_prefix.as_deref(),
            )?);
    }
    Ok(trace)
}

pub(super) fn protect_retained_history(
    removable: &mut super::converge_plan::ConvergeTrace,
    retained: super::converge_plan::ConvergeTrace,
) {
    let protected_v2_edges: HashSet<_> = retained.atom_ref_v2_edge_keys.into_iter().collect();
    removable
        .atom_ref_v2_edge_keys
        .retain(|key| !protected_v2_edges.contains(key));
}

type HeaderMap = HashMap<String, MoleculeHeader>;

pub(super) async fn build_candidate_removals(
    db_ops: &Arc<DbOperations>,
    tips: Vec<CandidateTip>,
) -> Result<Vec<OwnedMoleculeKeyRemoval>, SchemaError> {
    let mut groups: std::collections::BTreeMap<(String, Option<String>), Vec<CandidateTip>> =
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
