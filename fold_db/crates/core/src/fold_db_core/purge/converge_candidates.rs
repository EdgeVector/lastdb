//! Exact readable physical targets for normal Delete convergence.

use super::delete_candidates::{delete_candidates_for_key, DeleteCandidate};
use super::*;
use crate::db_operations::atom_store::{MoleculeHeader, PerKeyRecord};

pub(super) struct CandidateTip {
    key_index: usize,
    canonical_uuid: String,
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
        let Some(uuid) = field.common().molecule_uuid() else {
            continue;
        };
        for (key_index, key) in keys.iter().enumerate() {
            for candidate in delete_candidates_for_key(db_ops, field, key)? {
                if seen.insert(candidate.mk_key.clone()) {
                    plans.push((key_index, uuid.clone(), candidate));
                }
            }
        }
    }
    let prefix = plans
        .first()
        .and_then(|(_, _, candidate)| candidate.storage_prefix.as_deref());
    if plans
        .iter()
        .any(|(_, _, candidate)| candidate.storage_prefix.as_deref() != prefix)
    {
        return Err(SchemaError::InvalidData(
            "Delete candidates have different storage scopes".into(),
        ));
    }
    let storage_keys: Vec<_> = plans
        .iter()
        .map(|(_, _, candidate)| candidate.mk_key.clone())
        .collect();
    let records = db_ops
        .atoms()
        .delete_target_tips_with_generations(&storage_keys, prefix)
        .await?;
    Ok(plans
        .into_iter()
        .zip(records)
        .filter_map(|((key_index, canonical_uuid, candidate), record)| {
            record.map(|record| CandidateTip {
                key_index,
                canonical_uuid,
                candidate,
                record,
            })
        })
        .partition(|tip| removable.contains(&tip.candidate.mk_key)))
}

pub(super) async fn trace_candidate_tips(
    db_ops: &Arc<DbOperations>,
    tips: &[CandidateTip],
) -> Result<super::converge_plan::ConvergeTrace, SchemaError> {
    use futures::{stream, StreamExt, TryStreamExt};
    // Own each task's inputs so the lane future remains Send at its async-trait boundary.
    let mut plans = Vec::with_capacity(tips.len());
    for tip in tips {
        plans.push((
            tip.key_index,
            tip.candidate.clone(),
            tip.record.entry.clone(),
        ));
    }
    let traces: Vec<_> = stream::iter(plans.into_iter().map(|(key_index, candidate, entry)| {
        let db_ops = Arc::clone(db_ops);
        async move {
            let trace =
                target_chain::collect_delete_candidate_chain(&db_ops, &candidate, &entry).await?;
            Ok::<_, SchemaError>((key_index, trace))
        }
    }))
    .buffer_unordered(4)
    .try_collect()
    .await?;
    let mut out = super::converge_plan::ConvergeTrace {
        traced: HashSet::new(),
        tip_version_keys: Vec::new(),
        atom_ref_edge_keys: Vec::new(),
        atom_ref_v2_edge_keys: Vec::new(),
    };
    for (key_index, trace) in traces {
        out.traced.insert(key_index);
        out.tip_version_keys.extend(trace.tip_version_keys);
        out.atom_ref_edge_keys.extend(trace.atom_ref_edge_keys);
        out.atom_ref_v2_edge_keys
            .extend(trace.atom_ref_v2_edge_keys);
    }
    out.tip_version_keys.sort_unstable();
    out.tip_version_keys.dedup();
    Ok(out)
}

pub(super) fn protect_retained_history(
    removable: &mut super::converge_plan::ConvergeTrace,
    retained: super::converge_plan::ConvergeTrace,
) {
    let protected_versions: HashSet<_> = retained.tip_version_keys.into_iter().collect();
    let protected_edges: HashSet<_> = retained.atom_ref_edge_keys.into_iter().collect();
    let protected_v2_edges: HashSet<_> = retained.atom_ref_v2_edge_keys.into_iter().collect();
    removable
        .tip_version_keys
        .retain(|key| !protected_versions.contains(key));
    removable
        .atom_ref_edge_keys
        .retain(|key| !protected_edges.contains(key));
    removable
        .atom_ref_v2_edge_keys
        .retain(|key| !protected_v2_edges.contains(key));
}

type CandidateGroup = (String, Vec<CandidateTip>);
type HeaderMap = HashMap<String, MoleculeHeader>;

pub(super) async fn build_candidate_removals(
    db_ops: &Arc<DbOperations>,
    tips: Vec<CandidateTip>,
) -> Result<Vec<OwnedMoleculeKeyRemoval>, SchemaError> {
    let mut groups: std::collections::BTreeMap<(String, Option<String>), CandidateGroup> =
        std::collections::BTreeMap::new();
    for tip in tips {
        let key = (
            tip.candidate.molecule_uuid.clone(),
            tip.candidate.storage_prefix.clone(),
        );
        groups
            .entry(key)
            .or_insert_with(|| (tip.canonical_uuid.clone(), Vec::new()))
            .1
            .push(tip);
    }
    let mut header_keys = Vec::new();
    for ((uuid, prefix), (canonical, _)) in &groups {
        for spelling in [uuid, canonical] {
            header_keys.push(build_storage_key(
                prefix.as_deref(),
                &crate::atom::molecule_key_codec::header_key(spelling),
            ));
        }
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
    for ((uuid, prefix), (canonical, tips)) in groups {
        let header = removal_header(&headers, &uuid, &canonical, prefix.as_deref())?;
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
    canonical: &str,
    prefix: Option<&str>,
) -> Result<&'a MoleculeHeader, SchemaError> {
    let key = build_storage_key(prefix, &crate::atom::molecule_key_codec::header_key(uuid));
    if let Some(header) = headers.get(&key) {
        return Ok(header);
    }
    let identity = crate::atom::molecule_uuid::parse_molecule_uuid_bytes(uuid);
    if identity.is_none()
        || identity != crate::atom::molecule_uuid::parse_molecule_uuid_bytes(canonical)
    {
        return Err(SchemaError::InvalidData(
            "Delete header fallback has a different molecule identity".into(),
        ));
    }
    let canonical_key = build_storage_key(
        prefix,
        &crate::atom::molecule_key_codec::header_key(canonical),
    );
    headers.get(&canonical_key).ok_or_else(|| {
        SchemaError::InvalidData(
            "Delete target lacks a physical or canonical molecule header".into(),
        )
    })
}
