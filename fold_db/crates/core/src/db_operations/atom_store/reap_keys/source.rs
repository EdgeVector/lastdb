//! Exact source keys for the stopped-home planner, using production builders.

use super::super::atom_ref_edges::{mutation_history_edges, tip_edge};
use super::super::{AtomRefEdge, MoleculeGenerationDelete, MoleculeGenerationSlot, PerKeyRecord};
use crate::atom::{molecule_key_codec as codec, AtomEntry, MutationEvent};
use crate::kind_partition::form_twin;
use crate::schema::SchemaError;

#[derive(Debug, Clone)]
pub struct ReapTipSource {
    pub molecule_uuid: String,
    pub disk_hash: String,
    pub disk_range: String,
    pub entry: AtomEntry,
}

#[derive(Debug, Clone)]
pub struct ReapSourceEdgeKeys {
    pub v1: String,
    pub v2: Option<String>,
}

fn invalid(message: impl Into<String>) -> SchemaError {
    SchemaError::InvalidData(message.into())
}

fn source(key: &str, entry: AtomEntry) -> Result<ReapTipSource, SchemaError> {
    let molecule = key
        .strip_prefix(codec::MK_PREFIX)
        .and_then(|rest| rest.split_once(':').map(|(molecule, _)| molecule))
        .ok_or_else(|| invalid("source key has no molecule"))?;
    let (disk_hash, disk_range) =
        codec::decode_hash_range(key, molecule).ok_or_else(|| invalid("source key has no slot"))?;
    Ok(ReapTipSource {
        molecule_uuid: molecule.into(),
        disk_hash,
        disk_range,
        entry,
    })
}

/// Decode all head/shadow entries in a personal-domain authoritative source.
pub fn reap_tip_sources(key: &str, value: &[u8]) -> Result<Vec<ReapTipSource>, SchemaError> {
    if key.starts_with(codec::MK_PREFIX) {
        let record: PerKeyRecord = serde_json::from_slice(value)
            .map_err(|error| invalid(format!("decode tip source: {error}")))?;
        return Ok(vec![source(key, record.entry)?]);
    }
    if let Some(rest) = key.strip_prefix(codec::MOLECULE_GENERATION_RECORD_PREFIX) {
        let (molecule, rest) = rest
            .split_once(':')
            .ok_or_else(|| invalid("generation has no molecule"))?;
        let (generation, _) = rest
            .split_once(':')
            .ok_or_else(|| invalid("generation has no identity"))?;
        let mk = codec::molecule_record_key_from_generation_key(molecule, generation, key)
            .ok_or_else(|| invalid("generation has no slot"))?;
        let slot: MoleculeGenerationSlot = serde_json::from_slice(value)
            .map_err(|error| invalid(format!("decode generation source: {error}")))?;
        return slot
            .record
            .map(|record| record.entry)
            .into_iter()
            .chain(slot.shadow)
            .map(|entry| source(&mk, entry))
            .collect();
    }
    if let Some(rest) = key.strip_prefix(codec::MOLECULE_GENERATION_DELETE_PREFIX) {
        let (molecule, _) = rest
            .split_once(':')
            .ok_or_else(|| invalid("delete has no molecule"))?;
        let mk = codec::molecule_record_key_from_generation_delete_key(molecule, key)
            .ok_or_else(|| invalid("generation delete has no slot"))?;
        let row: MoleculeGenerationDelete = serde_json::from_slice(value)
            .map_err(|error| invalid(format!("decode generation delete: {error}")))?;
        return Ok(vec![source(&mk, row.shadow)?]);
    }
    Err(invalid("unsupported tip source key"))
}

fn keys(edge: &AtomRefEdge) -> ReapSourceEdgeKeys {
    ReapSourceEdgeKeys {
        v1: edge.storage_key(None),
        v2: edge.storage_key_v2(None).ok(),
    }
}

#[must_use]
pub fn reap_tip_source_keys(source: &ReapTipSource) -> ReapSourceEdgeKeys {
    keys(&tip_edge(
        &source.molecule_uuid,
        &source.disk_hash,
        &source.disk_range,
        &source.entry,
        true,
    ))
}

/// Derived keys retired only after the exact authoritative version row.
#[must_use]
pub fn reap_version_source_keys(
    molecule_uuid: &str,
    disk_hash: &str,
    disk_range: &str,
    version: &str,
    atom_uuid: &str,
) -> (ReapSourceEdgeKeys, String) {
    (
        keys(&AtomRefEdge {
            atom_uuid: atom_uuid.into(),
            edge_type: super::super::AtomRefEdgeType::TipVersion,
            molecule_uuid: molecule_uuid.into(),
            disk_hash: disk_hash.into(),
            disk_range: disk_range.into(),
            version_id: version.into(),
            active: true,
        }),
        codec::tip_version_backref_key(atom_uuid, version),
    )
}

pub fn reap_history_source_keys(
    key: &str,
    value: &[u8],
) -> Result<(String, Vec<ReapSourceEdgeKeys>), SchemaError> {
    let event: MutationEvent = serde_json::from_slice(value)
        .map_err(|error| invalid(format!("decode history source: {error}")))?;
    let prefix = codec::history_molecule_prefix(&event.molecule_uuid);
    if !key.starts_with(&prefix) && !form_twin(&prefix).is_some_and(|twin| key.starts_with(&twin)) {
        return Err(invalid("history source molecule differs from its key"));
    }
    let mut edges = mutation_history_edges(key, &event);
    if let Some(twin) = form_twin(key) {
        edges.extend(mutation_history_edges(&twin, &event));
    }
    Ok((event.molecule_uuid, edges.iter().map(keys).collect()))
}

/// Decode a legacy source edge and confirm its stored key matches its value.
pub fn reap_legacy_atom_edge(
    key: &str,
    value: &[u8],
) -> Result<(String, ReapSourceEdgeKeys), SchemaError> {
    let edge: AtomRefEdge = serde_json::from_slice(value)
        .map_err(|error| invalid(format!("decode legacy atom source edge: {error}")))?;
    let expected = edge.storage_key(None);
    if key != expected && form_twin(&expected).as_deref() != Some(key) {
        return Err(invalid("legacy atom edge key differs from its value"));
    }
    let source_keys = keys(&edge);
    Ok((edge.molecule_uuid, source_keys))
}
