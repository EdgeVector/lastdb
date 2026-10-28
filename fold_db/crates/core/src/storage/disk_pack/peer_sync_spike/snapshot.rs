//! Bounded logical re-encoding into encrypted V2 files, not a plaintext export.

use super::control::{private_directory, read_private, write_new};
use super::keys::{event_key, STATE_KEY};
use super::model::State;
use super::*;
use crate::schema::types::MutationAuthorClockState;
use crate::storage::disk_pack::{PackNamespacedStore, PackReadScope};
use crate::storage::traits::{KvMutation, NamespacedStore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::os::unix::fs::DirBuilderExt;
use std::path::Path;
use std::sync::Arc;

pub(super) const SNAPSHOT_KEY: &[u8] = b"snapshot";
const COMPLETE: &[u8] = b"peer-snapshot-v1\n";
const ROWS_PER_PAGE: usize = 16;
const MAX_PAGES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotInfo {
    pub version: u8,
    pub frontiers: BTreeMap<String, Position>,
    pub clock: MutationAuthorClockState,
    pub records: u64,
    pub events: u64,
    pub rows: u64,
    pub sha256: String,
}

impl Replica {
    /// Snapshot only this experimental namespace. The signer and key stay outside it.
    pub async fn snapshot(&self, output: &Path) -> StorageResult<SnapshotInfo> {
        let control = self
            .control
            .as_ref()
            .ok_or_else(|| invalid("snapshot needs a prototype root"))?;
        control.require_ready()?;
        let _gate = self.runtime.enter_write().await;
        let state = self.state().await?;
        validate_heads(self, &state).await?;
        fs::DirBuilder::new().mode(0o700).create(output)?;
        fs::DirBuilder::new()
            .mode(0o700)
            .create(output.join("data"))?;
        let target = Self::open_generation(&output.join("data"), &control.key).await?;
        let info = copy_namespace(self, &target, &state).await?;
        target.kv.put(SNAPSHOT_KEY, encode(&info)?).await?;
        target.kv.flush().await?;
        drop(target);
        write_new(&output.join("COMPLETE"), COMPLETE)?;
        File::open(output)?.sync_all()?;
        File::open(
            output
                .parent()
                .ok_or_else(|| invalid("snapshot needs a parent"))?,
        )?
        .sync_all()?;
        Ok(info)
    }
}

pub(super) async fn open_snapshot(
    path: &Path,
    key: &[u8; 32],
) -> StorageResult<(Replica, SnapshotInfo)> {
    private_directory(path)?;
    if read_private(&path.join("COMPLETE"), COMPLETE.len())? != COMPLETE {
        return Err(invalid("snapshot is incomplete or has another format"));
    }
    let store = PackNamespacedStore::open_existing_for_backup(&path.join("data"), key)?;
    let runtime = store
        .disk_pack_runtime()
        .ok_or_else(|| invalid("snapshot needs native V2"))?;
    let shared: Arc<dyn NamespacedStore> = Arc::new(store);
    let scope = PackReadScope::for_store(&shared)
        .await?
        .ok_or_else(|| invalid("snapshot needs one native read boundary"))?;
    let kv = scope.run(shared.open_namespace(NAMESPACE)).await?;
    let replica = Replica {
        kv,
        runtime,
        control: None,
    };
    let bytes = replica
        .kv
        .get(SNAPSHOT_KEY)
        .await?
        .ok_or_else(|| invalid("encrypted snapshot descriptor is absent"))?;
    let info: SnapshotInfo = decode(&bytes)?;
    let state = required_state(&replica).await?;
    if info.version != 1 || info.frontiers != state.frontiers || info.clock != state.clock {
        return Err(invalid(
            "snapshot map or author clock differs from its committed data",
        ));
    }
    validate_heads(&replica, &state).await?;
    let actual = walk(&replica, None, &state).await?;
    if actual != info {
        return Err(invalid("snapshot rows or digest differ"));
    }
    Ok((replica, info))
}

pub(super) async fn required_state(replica: &Replica) -> StorageResult<State> {
    let bytes = replica
        .kv
        .get(STATE_KEY)
        .await?
        .ok_or_else(|| invalid("snapshot sequence map is absent"))?;
    let state: State = decode(&bytes)?;
    if state.frontiers.len() > MAX_ORIGINS {
        return Err(invalid("snapshot exceeds 128 origins"));
    }
    Ok(state)
}

pub(super) async fn copy_namespace(
    source: &Replica,
    target: &Replica,
    state: &State,
) -> StorageResult<SnapshotInfo> {
    let info = walk(source, Some(target), state).await?;
    if info != walk(target, None, state).await? {
        return Err(invalid("snapshot re-encoding changed a logical record"));
    }
    Ok(info)
}

async fn walk(
    source: &Replica,
    target: Option<&Replica>,
    state: &State,
) -> StorageResult<SnapshotInfo> {
    let mut info = SnapshotInfo {
        version: 1,
        frontiers: state.frontiers.clone(),
        clock: state.clock,
        records: 0,
        events: 0,
        rows: 0,
        sha256: String::new(),
    };
    let mut digest = Sha256::new();
    digest.update(b"lastdb-peer-logical-snapshot-v1");
    let mut start = Vec::new();
    for page in 0..MAX_PAGES {
        // The final page includes one lookahead; reserve one physical row for the descriptor.
        let limit = ROWS_PER_PAGE + usize::from(page + 1 == MAX_PAGES);
        let rows = source.kv.scan_range_paged(&start, b"t", limit).await?;
        let count = rows.len();
        if page + 1 == MAX_PAGES && count > ROWS_PER_PAGE {
            return Err(invalid("snapshot exceeds 128 pages"));
        }
        let mut operations = Vec::new();
        for (key, value) in &rows {
            if key.as_slice() == SNAPSHOT_KEY {
                continue;
            }
            if key.starts_with(b"d\0") {
                info.records += 1;
            } else if key.starts_with(b"e\0") {
                let entry: Entry = decode(value)?;
                entry.verify()?;
                if event_key(&entry.origin, entry.sequence) != *key {
                    return Err(invalid(
                        "snapshot event key differs from its signed identity",
                    ));
                }
                info.events += 1;
            } else if key.as_slice() != STATE_KEY {
                return Err(invalid("unknown prototype snapshot key"));
            }
            info.rows += 1;
            if info.rows >= (MAX_PAGES * ROWS_PER_PAGE) as u64 {
                return Err(invalid("snapshot exceeds 2047 logical records"));
            }
            digest.update((key.len() as u64).to_be_bytes());
            digest.update(key);
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value);
            if target.is_some() {
                operations.push(KvMutation::put(key.clone(), value.clone()));
            }
        }
        if let Some(target) = target {
            if !operations.is_empty() {
                target.kv.batch_mutate(operations).await?;
            }
        }
        if count < limit {
            info.sha256 = super::model::hex(&digest.finalize());
            return Ok(info);
        }
        if page + 1 == MAX_PAGES {
            return Err(invalid("snapshot exceeds 128 pages of 16 records"));
        }
        start = rows.last().expect("a full page has a last key").0.clone();
        start.push(0);
    }
    unreachable!("the bounded walk returns on its final page")
}

async fn validate_heads(replica: &Replica, state: &State) -> StorageResult<()> {
    if state.frontiers.len() > MAX_ORIGINS {
        return Err(invalid("snapshot exceeds 128 origins"));
    }
    let heads: Vec<_> = state.frontiers.clone().into_iter().collect();
    for chunk in heads.chunks(MAX_EXPORT_BATCH) {
        let keys = chunk
            .iter()
            .map(|(origin, position)| event_key(origin, position.sequence))
            .collect();
        let values = replica.kv.get_many(keys).await?;
        if values.len() != chunk.len() {
            return Err(invalid("snapshot head batch is incomplete"));
        }
        for ((origin, position), bytes) in chunk.iter().zip(values) {
            let entry: Entry =
                decode(&bytes.ok_or_else(|| invalid("snapshot head proof is absent"))?)?;
            entry.verify()?;
            if entry.origin != *origin
                || entry.position() != *position
                || state.clock.physical_nanos < entry.last_written_at()?
                || state.clock.logical_counter < entry.logical_counter
            {
                return Err(invalid("snapshot head proof or clock differs"));
            }
        }
    }
    Ok(())
}
