//! One authenticated deletion intent across the paired native homes.
//!
//! The owner saves the keys and both exact frontiers before CAS puts or main
//! publication. A reopen finishes deletion before any reader or writer can
//! reuse those keys. The intent is not a backup file: a
//! capture refuses it until normal recovery completes.

use super::physical::{V2Home, CAS_COLLECTION};
use super::*;
use serde::{Deserialize, Serialize};

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

/// Conservative encoded size; admission reserves this before accepting a batch.
pub(super) fn space(operations: &[HomeMutation]) -> StorageResult<u64> {
    let bytes = operations
        .iter()
        .filter(|op| op.collection == CAS_COLLECTION && op.body.is_none())
        .try_fold(0u64, |bytes, op| {
            bytes.checked_add((op.key.len() as u64).saturating_mul(6) + 4)
        })
        .ok_or_else(|| invalid("CAS deletion intent size overflow"))?;
    if bytes == 0 {
        return Ok(0);
    }
    if bytes + 1024 > laststore::disk_run::home::MAX_CAS_DELETE_CHECKPOINT_BYTES as u64 {
        return Err(StorageError::WriteResourceLimit {
            detail: "CAS deletion intent exceeds its byte bound".into(),
        });
    }
    Ok(bytes + 8192)
}

pub(super) fn save(
    v2: &V2Home,
    puts: bool,
    main: bool,
    deletes: &[HomeMutation],
) -> StorageResult<()> {
    if deletes.is_empty() {
        return Ok(());
    }
    require_clear(v2)?;
    let before = v2.current_root();
    let intent = Intent {
        version: 1,
        main_home: v2.home.identity(),
        cas_home: v2.cas.identity(),
        main_before: before.main.frontier(),
        main_after: before
            .main
            .frontier()
            .checked_add(u64::from(main))
            .ok_or_else(|| invalid("main frontier overflow"))?,
        cas_before: before.cas.frontier(),
        cas_after_add: before
            .cas
            .frontier()
            .checked_add(u64::from(puts))
            .ok_or_else(|| invalid("CAS frontier overflow"))?,
        keys: deletes.iter().map(|op| op.key.clone()).collect(),
    };
    let bytes = serde_json::to_vec(&intent).map_err(|_| invalid("CAS deletion intent encoding"))?;
    v2.home.save_cas_delete_checkpoint(&bytes).map_err(map)
}

pub(super) fn require_clear(v2: &V2Home) -> StorageResult<()> {
    if v2.home.load_cas_delete_checkpoint().map_err(map)?.is_some() {
        return Err(invalid(
            "pending CAS deletion requires native recovery before backup",
        ));
    }
    Ok(())
}

/// Caller owns both homes before any reader, writer or worker can start.
/// A frontier outside the exact intent is refused, never guessed or replayed.
pub(super) fn recover(v2: &V2Home) -> StorageResult<bool> {
    let Some(bytes) = v2.home.load_cas_delete_checkpoint().map_err(map)? else {
        return Ok(false);
    };
    let intent: Intent =
        serde_json::from_slice(&bytes).map_err(|_| invalid("CAS deletion intent encoding"))?;
    let main = v2.home.current_root().frontier();
    let cas = v2.cas.current_root().frontier();
    let deleted = intent
        .cas_after_add
        .checked_add(1)
        .ok_or_else(|| invalid("CAS frontier overflow"))?;
    if intent.version != 1
        || intent.main_home != v2.home.identity()
        || intent.cas_home != v2.cas.identity()
        || intent.keys.is_empty()
        || intent.keys.windows(2).any(|pair| pair[0] >= pair[1])
        || intent.main_after < intent.main_before
        || intent.main_after - intent.main_before > 1
        || intent.cas_after_add < intent.cas_before
        || intent.cas_after_add - intent.cas_before > 1
        || ![intent.main_before, intent.main_after].contains(&main)
        || cas < intent.cas_before
        || cas > deleted
        || (main != intent.main_after && cas > intent.cas_after_add)
    {
        return Err(invalid("CAS deletion intent identity or frontier mismatch"));
    }
    // No main publication: retain every blob. No accepted reference was removed.
    // A CAS-only batch whose additions did not commit is also incomplete.
    let committed = main == intent.main_after && cas >= intent.cas_after_add;
    let mut changed = false;
    if committed && cas != deleted {
        let operations = intent
            .keys
            .into_iter()
            .map(|key| HomeMutation {
                collection: CAS_COLLECTION.into(),
                key,
                body: None,
            })
            .collect();
        v2.cas.commit(operations).map_err(map)?;
        changed = true;
    }
    v2.home.clear_cas_delete_checkpoint().map_err(map)?;
    Ok(changed)
}

fn invalid(message: &str) -> StorageError {
    StorageError::InvalidOperation(message.into())
}
