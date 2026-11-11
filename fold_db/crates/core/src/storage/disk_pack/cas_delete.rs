//! One authenticated deletion intent across the paired native homes.
//!
//! The owner saves the keys and both exact frontiers before CAS puts or main
//! publication. A reopen finishes deletion before any reader or writer can
//! reuse those keys. The intent is not a backup file: a
//! capture refuses it until normal recovery completes.

use super::physical::{V2Home, CAS_COLLECTION};
use super::*;
use crate::storage::format::metadata::FILE_REFERENCE_PREFIX;
use crate::storage::format::{
    METADATA_NAMESPACE, PENDING_UPLOAD_PREFIX, RECORD_FILE_PREFIX, RECORD_FILE_UPLOADED_PREFIX,
    RELEASED_FILE_PREFIX,
};
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

/// Variable encoded bytes only; the accepted prefix adds fixed overhead once.
pub(in crate::storage::disk_pack) fn key_bytes(operations: &[HomeMutation]) -> StorageResult<u64> {
    operations
        .iter()
        .filter(|op| op.collection == CAS_COLLECTION && op.body.is_none())
        .try_fold(0u64, |bytes, op| {
            bytes.checked_add((op.key.len() as u64).saturating_mul(6) + 4)
        })
        .ok_or_else(|| invalid("CAS deletion intent size overflow"))
}

/// Conservative encoded size; admission reserves this before accepting a batch.
pub(in crate::storage::disk_pack) fn space(bytes: u64) -> StorageResult<u64> {
    if bytes == 0 {
        return Ok(0);
    }
    let reserved = bytes
        .checked_add(8192)
        .ok_or_else(|| invalid("CAS deletion intent size overflow"))?;
    if reserved > laststore::disk_run::home::MAX_CAS_DELETE_CHECKPOINT_BYTES as u64 {
        return Err(StorageError::WriteResourceLimit {
            detail: "CAS deletion intent exceeds its byte bound".into(),
        });
    }
    Ok(reserved)
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

pub(in crate::storage) fn require_clear(v2: &V2Home) -> StorageResult<()> {
    if v2.home.load_cas_delete_checkpoint().map_err(map)?.is_some() {
        return Err(invalid(
            "pending CAS deletion requires native recovery before backup",
        ));
    }
    Ok(())
}

/// Caller owns both homes before any reader, writer or worker can start.
/// A frontier outside the exact intent is refused, never guessed or replayed.
pub(in crate::storage::disk_pack) fn recover(
    v2: &V2Home,
) -> StorageResult<Option<CasDeleteRecovery>> {
    let Some(bytes) = v2.home.load_cas_delete_checkpoint().map_err(map)? else {
        return Ok(None);
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
    let committed = main == intent.main_after && cas >= intent.cas_after_add;
    let intent_keys = intent.keys.len() as u64;
    let mut outcome = if committed {
        CasDeleteRecoveryOutcome::AlreadyComplete
    } else {
        CasDeleteRecoveryOutcome::MainNotPublished
    };
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
        v2.cas
            .commit(super::blob_backup::with_releases(operations))
            .map_err(map)?;
        outcome = CasDeleteRecoveryOutcome::Completed;
    }
    v2.home.clear_cas_delete_checkpoint().map_err(map)?;
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
    /// A guest snapshot can carry inline files only. The main reference index
    /// also detects a local-path or restored reference whose CAS bytes are absent.
    pub async fn has_external_backup_files(self: &Arc<Self>) -> StorageResult<bool> {
        let metadata = PackKvStore {
            runtime: Arc::clone(self),
            namespace: METADATA_NAMESPACE.into(),
        };
        let cas = self.blob_store()?;
        for (store, prefix) in [
            (&metadata, FILE_REFERENCE_PREFIX),
            (&cas, PENDING_UPLOAD_PREFIX),
            (&cas, RECORD_FILE_UPLOADED_PREFIX),
            (&cas, RELEASED_FILE_PREFIX),
            (&cas, RECORD_FILE_PREFIX),
        ] {
            if !store
                .scan_prefix_paged(prefix.as_bytes(), 1)
                .await?
                .is_empty()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Which of these `cas_blobs` marker keys are present.
    pub(in crate::storage) async fn markers_present(
        self: &Arc<Self>,
        keys: Vec<Vec<u8>>,
    ) -> StorageResult<Vec<bool>> {
        let store = PackKvStore {
            runtime: Arc::clone(self),
            namespace: CAS_COLLECTION.into(),
        };
        let wanted = keys.len();
        let present = store.get_many(keys).await?;
        if present.len() != wanted {
            return Err(StorageError::InvalidOperation(
                "incomplete marker presence read".into(),
            ));
        }
        Ok(present.iter().map(Option::is_some).collect())
    }

    /// Stage a tombstone without loading the retired payload into the write request.
    pub(in crate::storage) fn stage_record_file_deletes(
        self: &Arc<Self>,
        keys: Vec<Vec<u8>>,
    ) -> StorageResult<()> {
        let pending = transaction::PendingBatch::current(self)
            .ok_or_else(|| invalid("file retirement requires a native write request"))?;
        let operations = keys
            .into_iter()
            .map(|key| {
                let file = std::str::from_utf8(&key)
                    .ok()
                    .and_then(crate::storage::current_keys::families::record_file_space);
                if file.is_none() {
                    return Err(invalid("file retirement requires a record file key"));
                }
                Ok(HomeMutation {
                    collection: CAS_COLLECTION.into(),
                    key: super::native_routing::encode(&key),
                    body: None,
                })
            })
            .collect::<StorageResult<_>>()?;
        pending.stage(operations)
    }
}
