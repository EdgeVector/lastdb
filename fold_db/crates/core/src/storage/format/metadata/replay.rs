//! Durable state for the signed database log and snapshot author stream forking.

use super::{
    decode_author, initial_local_author, invalid_author, read_err, Metadata, LOCAL_AUTHOR_KEY,
};
use crate::schema::SchemaError;
use crate::storage::disk_pack::PackRuntime;
use crate::storage::error::StorageResult;
use crate::storage::format::METADATA_NAMESPACE;
use lastdb_model::hex::{hex_lower, sha256_hex};
use lastdb_model::record_value::MAX_RECORD_BYTES;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

const MAX_DEFERRED_BYTES: usize = 450 * 1024;
const MAX_BASELINE_BYTES: usize = MAX_RECORD_BYTES + 2048;
const LEGACY_PENDING: &str = "legacy:";
const BASELINE_MSG: &str = "invalid replay baseline length";
const DEFERRED_MSG: &str = "invalid deferred replay envelope length";

type Res<T = ()> = Result<T, SchemaError>;

/// Whole-record ordering for accepted cloud entries, independent of delivery position.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayStamp {
    pub ordering_ms: u64,
    pub writer_key: String,
    pub writer_device: String,
    pub writer_seq: u64,
}

#[derive(Clone, Copy)]
pub(crate) enum ReplayTarget<'a> {
    Record(&'a str),
    Schema(&'a str),
    SchemaRecords(&'a str),
}

pub(crate) struct SchemaReplayDrop<'a> {
    pub(crate) id: &'a [u8; 32],
    pub(crate) digest: &'a [u8; 32],
}

impl ReplayTarget<'_> {
    fn key(self, ctx: &str) -> String {
        let (kind, addr) = match self {
            Self::Record(a) => ("record", a),
            Self::Schema(n) => ("schema", n),
            Self::SchemaRecords(n) => ("schema-records", n),
        };
        format!("signed-replay-{ctx}:{kind}:{}", sha256_hex(addr))
    }
}

fn rkey(addr: &str, ctx: &str) -> String {
    ReplayTarget::Record(addr).key(ctx)
}

impl Metadata {
    pub(in crate::storage) async fn fork_restored_author(
        rt: &Arc<PackRuntime>,
    ) -> StorageResult<()> {
        let store = rt.open_store(METADATA_NAMESPACE).await?;
        let prev = decode_author(store.get(LOCAL_AUTHOR_KEY.as_bytes()).await?.as_deref())?;
        let mut next = decode_author(Some(&initial_local_author(uuid::Uuid::new_v4())?))?;
        next.clock_floor_ms = prev.clock_floor_ms.max(prev.progress.applied_through_ms);
        let value = serde_json::to_vec(&next).map_err(invalid_author)?;
        Self::new(store, Arc::clone(rt))
            .stage(vec![(LOCAL_AUTHOR_KEY.to_owned(), Some(value))])
            .await
    }

    pub(crate) async fn stage_replay_receipt(&self, id: &[u8; 32], dig: &[u8; 32]) -> Res<bool> {
        let key = format!("signed-replay-receipt:{}", hex_lower(id));
        self.stage_receipt(key, dig, "signed replay ID reused")
            .await
    }

    pub(crate) async fn replay_receipt_exists(&self, id: &[u8; 32], dig: &[u8; 32]) -> Res<bool> {
        let key = format!("signed-replay-receipt:{}", hex_lower(id));
        self.receipt_exists(&key, dig, "signed replay ID reused")
            .await
    }

    pub(super) async fn stage_receipt(&self, k: String, dig: &[u8; 32], msg: &str) -> Res<bool> {
        if self.receipt_exists(&k, dig, msg).await? {
            return Ok(true);
        }
        self.stage(vec![(k, Some(dig.to_vec()))])
            .await
            .map_err(read_err)
            .map(|()| false)
    }

    pub(super) async fn receipt_exists(&self, k: &str, dig: &[u8; 32], msg: &str) -> Res<bool> {
        self.require_replay_scope()?;
        match self.0.get(k.as_bytes()).await.map_err(read_err)? {
            Some(saved) if saved.as_slice() == dig => Ok(true),
            Some(_) => Err(SchemaError::InvalidField(msg.into())),
            None => Ok(false),
        }
    }

    pub(crate) async fn guard_schema_drop(
        &self,
        name: &str,
        replay: Option<&SchemaReplayDrop<'_>>,
        stage: bool,
    ) -> Res<bool> {
        self.require_replay_scope()?;
        let (target, key) = (
            ReplayTarget::Schema(name),
            ReplayTarget::Schema(name).key("drop"),
        );
        let live = self.0.get(key.as_bytes()).await.map_err(read_err)?;
        let Some(replay) = replay else {
            return live.is_none().then_some(true).ok_or_else(schema_busy);
        };
        if self.replay_receipt_exists(replay.id, replay.digest).await? {
            return Ok(false);
        }
        let intent = format!("{}:{}", hex_lower(replay.id), hex_lower(replay.digest));
        match live {
            Some(b) if b.as_slice() == intent.as_bytes() => return Ok(true),
            Some(_) => return Err(schema_busy()),
            None if !stage => return Err(schema_busy()),
            None => {}
        }
        let srec = ReplayTarget::SchemaRecords(name);
        if self.pending_replay(target).await?.is_some()
            || self.pending_replay(srec).await?.is_some()
        {
            return Err(schema_busy());
        }
        self.stage(vec![(key, Some(intent.into_bytes()))])
            .await
            .map_err(read_err)
            .map(|()| true)
    }

    pub(crate) async fn finish_schema_drop(&self, s: &str, r: &SchemaReplayDrop<'_>) -> Res {
        self.stage_replay_receipt(r.id, r.digest).await?;
        self.stage(vec![(ReplayTarget::Schema(s).key("drop"), None)])
            .await
            .map_err(read_err)
    }

    pub(crate) fn require_replay_scope(&self) -> Res {
        self.require_scope().map_err(read_err)
    }

    pub(crate) async fn stage_local_replay_targets<'a>(
        &self,
        targets: impl IntoIterator<Item = (ReplayTarget<'a>, &'a str)>,
    ) -> Res {
        let entries = targets
            .into_iter()
            .map(|(t, id)| (t.key("pending"), Some(id.as_bytes().to_vec())))
            .collect();
        self.stage(entries).await.map_err(read_err)
    }

    pub(crate) async fn pending_replay(&self, target: ReplayTarget<'_>) -> Res<Option<String>> {
        self.require_scope().map_err(read_err)?;
        let saved = self
            .0
            .get(target.key("pending").as_bytes())
            .await
            .map_err(read_err)?
            .map(|b| String::from_utf8(b).map_err(read_err))
            .transpose()?;
        Ok(saved.map(|id| match id.strip_prefix(LEGACY_PENDING) {
            Some(s) => s.to_owned(),
            None => id,
        }))
    }

    /// Add old spool items in order without replacing a newer local write's marker.
    pub(crate) async fn stage_legacy_replay_targets<'a>(
        &self,
        targets: impl IntoIterator<Item = (ReplayTarget<'a>, &'a str)>,
    ) -> Res {
        self.require_scope().map_err(read_err)?;
        let map: BTreeMap<_, _> = targets
            .into_iter()
            .map(|(t, id)| (t.key("pending"), id))
            .collect();
        if map.is_empty() {
            return Ok(());
        }
        let existing = self
            .0
            .get_many(map.keys().map(|k| k.as_bytes().to_vec()).collect())
            .await
            .map_err(read_err)?;
        if existing.len() != map.len() {
            let msg = "incomplete pending replay lookup";
            return Err(SchemaError::InvalidData(msg.into()));
        }
        let pfx = LEGACY_PENDING.as_bytes();
        let writes = map
            .into_iter()
            .zip(existing)
            .filter(|(_, cur)| cur.as_deref().is_none_or(|v| v.starts_with(pfx)))
            .map(|((k, id), _)| (k, Some(format!("{LEGACY_PENDING}{id}").into_bytes())))
            .collect();
        self.stage(writes).await.map_err(read_err)
    }

    pub(crate) async fn clear_pending_replay(&self, t: ReplayTarget<'_>, id: &str) -> Res {
        if self.pending_replay(t).await?.as_deref() == Some(id) {
            let mut rows = vec![(t.key("pending"), None)];
            if matches!(t, ReplayTarget::Record(_)) {
                rows.extend([
                    (t.key("pending-attachment"), None),
                    (t.key("baseline"), None),
                ]);
            }
            self.stage(rows).await.map_err(read_err)?;
        }
        Ok(())
    }

    pub(crate) async fn replay_baseline_sources(
        &self,
        addresses: &[&str],
    ) -> Res<Vec<Option<ReplayStamp>>> {
        self.require_replay_scope()?;
        let mut stamps = Vec::with_capacity(addresses.len());
        for chunk in addresses.chunks(5000) {
            let keys: Vec<_> = chunk
                .iter()
                .flat_map(|a| [rkey(a, "pending"), rkey(a, "winner")].map(String::into_bytes))
                .collect();
            let rows = self.0.get_many(keys).await.map_err(read_err)?;
            if rows.len() != chunk.len() * 2 {
                let msg = "incomplete replay baseline read";
                return Err(SchemaError::InvalidData(msg.into()));
            }
            for pair in rows.chunks_exact(2) {
                let s = match (&pair[0], &pair[1]) {
                    (None, Some(b)) => Some(serde_json::from_slice(b).map_err(read_err)?),
                    _ => None,
                };
                stamps.push(s);
            }
        }
        Ok(stamps)
    }

    pub(crate) async fn replay_baseline(&self, addr: &str) -> Res<Option<Vec<u8>>> {
        self.require_replay_scope()?;
        let bytes = self
            .0
            .get(rkey(addr, "baseline").as_bytes())
            .await
            .map_err(read_err)?;
        check_len(bytes.as_deref(), MAX_BASELINE_BYTES, BASELINE_MSG).map(|()| bytes)
    }

    pub(crate) async fn stage_replay_baselines(&self, baselines: Vec<(&str, Vec<u8>)>) -> Res {
        let mut rows = Vec::with_capacity(baselines.len());
        for (addr, bytes) in baselines {
            check_len(Some(&bytes), MAX_BASELINE_BYTES, BASELINE_MSG)?;
            rows.push((rkey(addr, "baseline"), Some(bytes)));
        }
        self.stage(rows).await.map_err(read_err)
    }

    pub(crate) async fn pending_replay_attachment(&self, addr: &str) -> Res<bool> {
        self.require_replay_scope()?;
        let k = rkey(addr, "pending-attachment");
        self.0
            .get(k.as_bytes())
            .await
            .map(|b| b.is_some())
            .map_err(read_err)
    }

    pub(crate) async fn stage_pending_replay_attachments<'a>(
        &self,
        addresses: impl IntoIterator<Item = &'a str>,
    ) -> Res {
        let rows = addresses
            .into_iter()
            .map(|a| (rkey(a, "pending-attachment"), Some(vec![1])))
            .collect();
        self.stage(rows).await.map_err(read_err)
    }

    pub(crate) async fn accept_replay(
        &self,
        t: ReplayTarget<'_>,
        stamp: &ReplayStamp,
    ) -> Res<bool> {
        if self
            .replay_winner(t)
            .await?
            .is_some_and(|prev| prev >= *stamp)
        {
            return Ok(false);
        }
        let bytes = serde_json::to_vec(stamp).map_err(read_err)?;
        self.stage(vec![(t.key("winner"), Some(bytes))])
            .await
            .map_err(read_err)
            .map(|()| true)
    }

    pub(crate) async fn replay_winner(&self, t: ReplayTarget<'_>) -> Res<Option<ReplayStamp>> {
        self.require_scope().map_err(read_err)?;
        self.0
            .get(t.key("winner").as_bytes())
            .await
            .map_err(read_err)?
            .map(|b| serde_json::from_slice(&b).map_err(read_err))
            .transpose()
    }

    pub(crate) async fn deferred_replay(&self, addr: &str) -> Res<Option<Vec<u8>>> {
        let body = self
            .0
            .get(rkey(addr, "deferred").as_bytes())
            .await
            .map_err(read_err)?;
        check_len(body.as_deref(), MAX_DEFERRED_BYTES, DEFERRED_MSG).map(|()| body)
    }

    pub(crate) async fn stage_deferred_replay(&self, a: &str, body: Option<Vec<u8>>) -> Res {
        check_len(body.as_deref(), MAX_DEFERRED_BYTES, DEFERRED_MSG)?;
        self.stage(vec![(rkey(a, "deferred"), body)])
            .await
            .map_err(read_err)
    }
}

fn check_len(body: Option<&[u8]>, max: usize, msg: &str) -> Res {
    body.is_none_or(|b| !b.is_empty() && b.len() <= max)
        .then_some(())
        .ok_or_else(|| SchemaError::InvalidData(msg.into()))
}

fn schema_busy() -> SchemaError {
    SchemaError::Blocked("schema removal awaits local changes or its current replay".into())
}
