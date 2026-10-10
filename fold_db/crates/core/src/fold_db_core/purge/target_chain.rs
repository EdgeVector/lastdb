//! Per-target tip-chain tracing used by hard-erase purge.

use std::collections::HashSet;
use std::sync::Arc;

use super::chain_walk::MAX_TIP_CHAIN_WALK;
use crate::db_operations::DbOperations;
use crate::schema::types::field::{build_storage_key, FieldVariant};
use crate::schema::types::key_value::KeyValue;
use crate::schema::SchemaError;

/// The purge target's own tip-version chain for one field: the `tv:` rows to
/// delete and the superseded atoms they name.
///
/// Returns `(tv_keys, atom_uuids)`. Empty for a field with no molecule, no
/// slot matching the key's shape, or a depth-1 head — the common case.
///
/// # This is the per-record half of the batch, so it must be O(1) in `R`
///
/// [`super::purge_records_bulk`] calls this once per `(field, key)`. Everything
/// else it does per key is a hashmap lookup ([`current_atom_for_key`],
/// [`remove_key_from_field`]), so this function alone decides whether a batch of
/// `N` costs `O(F·N)` or `O(F·N·R)`.
///
/// It used to call `per_key_records()` and linearly filter the result for one
/// slot. That helper materialises a `Vec` of the WHOLE molecule, cloning every
/// hash, range and [`AtomEntry`] — so finding one slot cost `R` clones, and the
/// batch that had just been de-quadratified in `N` was still quadratic through
/// `R`. Measured on the merged batching change: 200/800/1600 records took
/// 119/755/2841 ms — 8× the records for 23.9× the time, the residual `N^1.5`
/// that made a naive extrapolation to the 74,105-row telemetry plane ~15 min
/// for a single drain.
///
/// `atom_uuids` is `HashMap<hash, BTreeMap<range, AtomEntry>>`, so a
/// `(hash, range)` slot is unique by construction: the old loop could match at
/// most one record and [`get_atom_entry`](crate::atom::MoleculeHashRange::get_atom_entry)
/// returns exactly that one. Same answer, `O(1)` instead of `O(R)`.
///
/// The entry is cloned rather than borrowed only so no borrow of the molecule
/// is held across the `walk_tip_chain` await — one clone, where the scan did
/// `R` of them.
///
/// Pinned by `collect_target_chain_does_not_scan_the_molecule`, which asserts
/// the scan helper is not called at all rather than merely checking the answer
/// is right — the scan returned the right answer too.
pub(crate) struct TargetChainTrace {
    pub tip_version_keys: Vec<String>,
    pub atom_uuids: HashSet<String>,
    pub atom_ref_edge_keys: Vec<String>,
    pub atom_ref_v2_edge_keys: Vec<String>,
}

pub(crate) async fn collect_target_chain(
    db_ops: &Arc<DbOperations>,
    field: &FieldVariant,
    key: &KeyValue,
) -> Result<TargetChainTrace, SchemaError> {
    collect_target_chain_with_mode(db_ops, field, key, ChainTraceMode::AtomErasure).await
}

/// Live Delete needs the slot's source keys and exact reverse-edge keys.
/// It does not plan atom candidates because the later reclaim janitor owns
/// body deletion. The converge batch removes each source before its edges.
pub(crate) async fn collect_target_tip_chain(
    db_ops: &Arc<DbOperations>,
    field: &FieldVariant,
    key: &KeyValue,
) -> Result<TargetChainTrace, SchemaError> {
    collect_target_chain_with_mode(db_ops, field, key, ChainTraceMode::TipVersionsOnly).await
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ChainTraceMode {
    AtomErasure,
    TipVersionsOnly,
}

async fn collect_target_chain_with_mode(
    db_ops: &Arc<DbOperations>,
    field: &FieldVariant,
    key: &KeyValue,
    mode: ChainTraceMode,
) -> Result<TargetChainTrace, SchemaError> {
    let mut tv_keys: Vec<String> = Vec::new();
    let mut atom_uuids: HashSet<String> = HashSet::new();
    let mut atom_ref_edge_keys = Vec::new();
    let mut atom_ref_v2_edge_keys = Vec::new();
    let Some((target_hash, target_range)) = field.disk_slot_for_key(key) else {
        return Ok(TargetChainTrace {
            tip_version_keys: tv_keys,
            atom_uuids,
            atom_ref_edge_keys,
            atom_ref_v2_edge_keys,
        });
    };
    let prefix = field.common().storage_prefix().map(str::to_string);
    let Some(molecule_uuid) = field.common().molecule_uuid() else {
        return Ok(TargetChainTrace {
            tip_version_keys: tv_keys,
            atom_uuids,
            atom_ref_edge_keys,
            atom_ref_v2_edge_keys,
        });
    };
    let Some(molecule) = field.molecule_data() else {
        return Ok(TargetChainTrace {
            tip_version_keys: tv_keys,
            atom_uuids,
            atom_ref_edge_keys,
            atom_ref_v2_edge_keys,
        });
    };
    let Some(entry) = molecule
        .get_atom_entry(&target_hash, &target_range)
        .cloned()
    else {
        return Ok(TargetChainTrace {
            tip_version_keys: tv_keys,
            atom_uuids,
            atom_ref_edge_keys,
            atom_ref_v2_edge_keys,
        });
    };
    atom_ref_edge_keys.push(db_ops.atoms().atom_ref_tip_edge_key(
        molecule_uuid,
        &target_hash,
        &target_range,
        &entry,
        prefix.as_deref(),
    ));
    if let Ok(v2_key) = db_ops.atoms().atom_ref_tip_edge_v2_key(
        molecule_uuid,
        &target_hash,
        &target_range,
        &entry,
        prefix.as_deref(),
    ) {
        atom_ref_v2_edge_keys.push(v2_key);
    }

    let mut version_id = entry.prev_tip_id.clone();
    let mut walked = 0usize;
    while !version_id.is_empty() {
        if walked >= MAX_TIP_CHAIN_WALK {
            return Err(SchemaError::InvalidData(format!(
                "purge: tip-version chain exceeded {MAX_TIP_CHAIN_WALK} nodes — refusing to \
                 partially erase a record (compliance verb)"
            )));
        }
        walked += 1;
        let Some(archived) = db_ops
            .atoms()
            .get_tip_version(&version_id, prefix.as_deref())
            .await?
        else {
            break;
        };
        tv_keys.push(build_storage_key(
            prefix.as_deref(),
            &crate::atom::molecule_key_codec::tip_version_key(&version_id),
        ));
        if mode == ChainTraceMode::AtomErasure {
            atom_uuids.insert(archived.atom_uuid.clone());
        }
        atom_ref_edge_keys.push(db_ops.atoms().atom_ref_tip_version_edge_key(
            molecule_uuid,
            &target_hash,
            &target_range,
            &version_id,
            &archived,
            prefix.as_deref(),
        ));
        if let Ok(v2_key) = db_ops.atoms().atom_ref_tip_version_edge_v2_key(
            molecule_uuid,
            &target_hash,
            &target_range,
            &version_id,
            &archived,
            prefix.as_deref(),
        ) {
            atom_ref_v2_edge_keys.push(v2_key);
        }
        version_id = archived.prev_tip_id;
    }
    Ok(TargetChainTrace {
        tip_version_keys: tv_keys,
        atom_uuids,
        atom_ref_edge_keys,
        atom_ref_v2_edge_keys,
    })
}
