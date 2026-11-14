//! Crash-safe two-home CAS deletion intent checkpointing and recovery.

use super::physical::V2Home;
use super::{map, HomeMutation, StorageError, StorageResult, CAS_COLLECTION};
use laststore::disk_run::home::CheckpointSpec;
use serde::{Deserialize, Serialize};

const MAX_CAS_DELETE_CHECKPOINT_BYTES: usize = 8 << 20;

pub(in crate::storage::disk_pack) const CAS_DELETE_CHECKPOINT: CheckpointSpec = CheckpointSpec {
    file_name: "CAS_DELETE",
    magic: b"LDRCAS1\0",
    max_bytes: MAX_CAS_DELETE_CHECKPOINT_BYTES,
};

#[derive(Clone, Copy, Debug, Serialize)]
pub struct CasDeleteRecovery {
    pub outcome: CasDeleteRecoveryOutcome,
    pub intent_keys: u64,
    pub main_frontier: u64,
    pub cas_frontier_before: u64,
    pub cas_frontier_after: u64,
    pub intent_released: bool,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CasDeleteRecoveryOutcome {
    MainNotPublished,
    Completed,
    AlreadyComplete,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    version: u16,
    main_home: uuid::Uuid,
    cas_home: uuid::Uuid,
    main_before: u64,
    main_after: u64,
    cas_before: u64,
    cas_after_add: u64,
    keys: Vec<String>,
}

pub(in crate::storage::disk_pack) fn reserved_space(
    ops: &[HomeMutation],
    prev: Option<&[HomeMutation]>,
) -> StorageResult<u64> {
    let mut bytes = 0u64;
    for op in ops.iter().chain(prev.into_iter().flatten()) {
        if op.collection == CAS_COLLECTION && op.body.is_none() {
            let add = (op.key.len() as u64).saturating_mul(6) + 4;
            bytes = bytes
                .checked_add(add)
                .ok_or_else(|| StorageError::invalid("CAS size overflow"))?;
        }
    }
    if bytes == 0 {
        return Ok(0);
    }
    let reserved = bytes
        .checked_add(8192)
        .ok_or_else(|| StorageError::invalid("CAS size overflow"))?;
    (reserved <= MAX_CAS_DELETE_CHECKPOINT_BYTES as u64)
        .then_some(reserved)
        .ok_or_else(|| StorageError::write_limit("CAS deletion intent exceeds its byte bound"))
}

pub(in crate::storage::disk_pack) fn save(
    v2: &V2Home,
    puts: bool,
    main: bool,
    deletes: &[HomeMutation],
) -> StorageResult<()> {
    if deletes.is_empty() {
        return Ok(());
    }
    require_clear(v2)?;
    let current = v2.current_root();
    let (main_before, cas_before) = (current.main.frontier(), current.cas.frontier());
    let step = |frontier: u64, inc: bool, msg: &str| {
        frontier
            .checked_add(u64::from(inc))
            .ok_or_else(|| StorageError::invalid(msg))
    };
    let intent = Intent {
        version: 1,
        main_home: v2.home.identity(),
        cas_home: v2.cas.identity(),
        main_before,
        main_after: step(main_before, main, "main frontier overflow")?,
        cas_before,
        cas_after_add: step(cas_before, puts, "CAS frontier overflow")?,
        keys: deletes.iter().map(|op| op.key.clone()).collect(),
    };
    let bytes =
        serde_json::to_vec(&intent).map_err(|_| StorageError::invalid("CAS intent encoding"))?;
    v2.home
        .save_checkpoint(CAS_DELETE_CHECKPOINT, &bytes)
        .map_err(map)
}

pub(in crate::storage) fn require_clear(v2: &V2Home) -> StorageResult<()> {
    let clear = v2
        .home
        .load_checkpoint(CAS_DELETE_CHECKPOINT)
        .map_err(map)?
        .is_none();
    clear
        .then_some(())
        .ok_or_else(|| StorageError::invalid("pending CAS deletion requires native recovery"))
}

pub(in crate::storage::disk_pack) fn recover(
    v2: &V2Home,
) -> StorageResult<Option<CasDeleteRecovery>> {
    let Some(bytes) = v2
        .home
        .load_checkpoint(CAS_DELETE_CHECKPOINT)
        .map_err(map)?
    else {
        return Ok(None);
    };
    let intent: Intent =
        serde_json::from_slice(&bytes).map_err(|_| StorageError::invalid("CAS intent encoding"))?;
    let current = v2.current_root();
    let (main, cas) = (current.main.frontier(), current.cas.frontier());
    let after_del = intent
        .cas_after_add
        .checked_add(1)
        .ok_or_else(|| StorageError::invalid("CAS frontier overflow"))?;
    let step_ok = |after: u64, before: u64| matches!(after.checked_sub(before), Some(0 | 1));
    if intent.version != 1
        || (intent.main_home, intent.cas_home) != (v2.home.identity(), v2.cas.identity())
        || intent.keys.is_empty()
        || intent.keys.windows(2).any(|pair| pair[0] >= pair[1])
        || !step_ok(intent.main_after, intent.main_before)
        || !step_ok(intent.cas_after_add, intent.cas_before)
        || ![intent.main_before, intent.main_after].contains(&main)
        || !(intent.cas_before..=after_del).contains(&cas)
        || (main != intent.main_after && cas > intent.cas_after_add)
    {
        return Err(StorageError::invalid(
            "CAS deletion intent identity or frontier mismatch",
        ));
    }
    let committed = main == intent.main_after && cas >= intent.cas_after_add;
    let intent_keys = intent.keys.len() as u64;
    let outcome = if committed && cas != after_del {
        let ops = intent
            .keys
            .into_iter()
            .map(|key| HomeMutation::new(CAS_COLLECTION, key, None))
            .collect();
        v2.cas.commit(ops).map_err(map)?;
        CasDeleteRecoveryOutcome::Completed
    } else if committed {
        CasDeleteRecoveryOutcome::AlreadyComplete
    } else {
        CasDeleteRecoveryOutcome::MainNotPublished
    };
    v2.home
        .clear_checkpoint(CAS_DELETE_CHECKPOINT)
        .map_err(map)?;
    let cas_frontier_after = v2.cas.current_root().frontier();
    Ok(Some(CasDeleteRecovery {
        outcome,
        intent_keys,
        main_frontier: main,
        cas_frontier_before: cas,
        cas_frontier_after,
        intent_released: true,
    }))
}
