//! One authenticated deletion intent across the paired native homes.

use super::physical::{V2Home, CAS_COLLECTION};
use super::*;
use crate::storage::current_keys::families::record_file_space;
use crate::storage::format::metadata::FILE_REFERENCE_PREFIX;
use crate::storage::format::{
    METADATA_NAMESPACE, PENDING_UPLOAD_PREFIX, RECORD_FILE_PREFIX, RECORD_FILE_UPLOADED_PREFIX,
    RELEASED_FILE_PREFIX,
};
use laststore::disk_run::home::{CheckpointKind, MAX_CAS_DELETE_CHECKPOINT_BYTES};
use serde::{Deserialize, Serialize};

/// Recovery performed by the normal open during this boot. No file keys enter it.
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

pub(in crate::storage::disk_pack) fn key_bytes(operations: &[HomeMutation]) -> StorageResult<u64> {
    operations
        .iter()
        .filter(|op| op.collection == CAS_COLLECTION && op.body.is_none())
        .try_fold(0u64, |b, op| {
            b.checked_add((op.key.len() as u64).saturating_mul(6) + 4)
        })
        .ok_or_else(|| invalid("CAS deletion intent size overflow"))
}

pub(in crate::storage::disk_pack) fn space(bytes: u64) -> StorageResult<u64> {
    if bytes == 0 {
        return Ok(0);
    }
    let reserved = bytes
        .checked_add(8192)
        .ok_or_else(|| invalid("CAS deletion intent size overflow"))?;
    (reserved <= MAX_CAS_DELETE_CHECKPOINT_BYTES as u64)
        .then_some(reserved)
        .ok_or_else(|| StorageError::WriteResourceLimit {
            detail: "CAS deletion intent exceeds its byte bound".into(),
        })
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
    let before = v2.current_root();
    let (main_before, cas_before) = (before.main.frontier(), before.cas.frontier());
    let intent = Intent {
        version: 1,
        main_home: v2.home.identity(),
        cas_home: v2.cas.identity(),
        main_before,
        main_after: main_before
            .checked_add(u64::from(main))
            .ok_or_else(|| invalid("main frontier overflow"))?,
        cas_before,
        cas_after_add: cas_before
            .checked_add(u64::from(puts))
            .ok_or_else(|| invalid("CAS frontier overflow"))?,
        keys: deletes.iter().map(|op| op.key.clone()).collect(),
    };
    let bytes = serde_json::to_vec(&intent).map_err(|_| invalid("CAS deletion intent encoding"))?;
    v2.home
        .save_checkpoint(CheckpointKind::CasDelete, &bytes)
        .map_err(map)
}

pub(in crate::storage) fn require_clear(v2: &V2Home) -> StorageResult<()> {
    v2.home
        .load_checkpoint(CheckpointKind::CasDelete)
        .map_err(map)?
        .is_none()
        .then_some(())
        .ok_or_else(|| invalid("pending CAS deletion requires native recovery before backup"))
}

pub(in crate::storage::disk_pack) fn recover(
    v2: &V2Home,
) -> StorageResult<Option<CasDeleteRecovery>> {
    let Some(bytes) = v2
        .home
        .load_checkpoint(CheckpointKind::CasDelete)
        .map_err(map)?
    else {
        return Ok(None);
    };
    let intent: Intent =
        serde_json::from_slice(&bytes).map_err(|_| invalid("CAS deletion intent encoding"))?;
    let (main, cas) = (
        v2.home.current_root().frontier(),
        v2.cas.current_root().frontier(),
    );
    let deleted = intent
        .cas_after_add
        .checked_add(1)
        .ok_or_else(|| invalid("CAS frontier overflow"))?;
    if intent.version != 1
        || intent.main_home != v2.home.identity()
        || intent.cas_home != v2.cas.identity()
        || intent.keys.is_empty()
        || intent.keys.windows(2).any(|w| w[0] >= w[1])
        || intent
            .main_after
            .checked_sub(intent.main_before)
            .is_none_or(|d| d > 1)
        || intent
            .cas_after_add
            .checked_sub(intent.cas_before)
            .is_none_or(|d| d > 1)
        || ![intent.main_before, intent.main_after].contains(&main)
        || cas < intent.cas_before
        || cas > deleted
        || (main != intent.main_after && cas > intent.cas_after_add)
    {
        return Err(invalid("CAS deletion intent identity or frontier mismatch"));
    }
    let committed = main == intent.main_after && cas >= intent.cas_after_add;
    let intent_keys = intent.keys.len() as u64;
    let outcome = if committed && cas != deleted {
        let ops = intent
            .keys
            .into_iter()
            .map(|key| HomeMutation {
                collection: CAS_COLLECTION.into(),
                key,
                body: None,
            })
            .collect();
        v2.cas
            .commit(super::blob_backup::with_releases(ops))
            .map_err(map)?;
        CasDeleteRecoveryOutcome::Completed
    } else if committed {
        CasDeleteRecoveryOutcome::AlreadyComplete
    } else {
        CasDeleteRecoveryOutcome::MainNotPublished
    };
    v2.home
        .clear_checkpoint(CheckpointKind::CasDelete)
        .map_err(map)?;
    Ok(Some(CasDeleteRecovery {
        outcome,
        intent_keys,
        main_frontier: main,
        cas_frontier_before: cas,
        cas_frontier_after: v2.cas.current_root().frontier(),
        intent_released: true,
    }))
}

fn invalid(message: &str) -> StorageError {
    StorageError::InvalidOperation(message.into())
}

impl PackRuntime {
    pub async fn has_external_backup_files(self: &Arc<Self>) -> StorageResult<bool> {
        let metadata = PackKvStore {
            runtime: Arc::clone(self),
            namespace: METADATA_NAMESPACE.into(),
        };
        let cas = self.cas_kv();
        for (store, prefix) in [
            (&metadata, FILE_REFERENCE_PREFIX),
            (&cas, PENDING_UPLOAD_PREFIX),
            (&cas, RECORD_FILE_UPLOADED_PREFIX),
            (&cas, RELEASED_FILE_PREFIX),
            (&cas, RECORD_FILE_PREFIX),
        ] {
            if !store
                .prefix_rows(prefix.as_bytes(), 1, false)
                .await?
                .is_empty()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(in crate::storage) async fn markers_present(
        self: &Arc<Self>,
        keys: Vec<Vec<u8>>,
    ) -> StorageResult<Vec<bool>> {
        let wanted = keys.len();
        let present = self.cas_kv().get_many(keys).await?;
        (present.len() == wanted)
            .then(|| present.iter().map(Option::is_some).collect())
            .ok_or_else(|| invalid("incomplete marker presence read"))
    }

    pub(in crate::storage) fn stage_record_file_deletes(
        self: &Arc<Self>,
        keys: Vec<Vec<u8>>,
    ) -> StorageResult<()> {
        let pending = transaction::PendingBatch::current(self)
            .ok_or_else(|| invalid("file retirement requires a native write request"))?;
        let ops = keys
            .into_iter()
            .map(|key| {
                std::str::from_utf8(&key)
                    .ok()
                    .and_then(record_file_space)
                    .ok_or_else(|| invalid("file retirement requires a record file key"))?;
                Ok(HomeMutation {
                    collection: CAS_COLLECTION.into(),
                    key: super::native_routing::encode(&key),
                    body: None,
                })
            })
            .collect::<StorageResult<_>>()?;
        pending.stage(ops)
    }
}
