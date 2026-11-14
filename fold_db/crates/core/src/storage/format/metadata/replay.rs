//! Durable state for the signed database log and replay ordering.

use super::{read_err, Metadata};
use crate::schema::types::SchemaError;
use lastdb_model::hex::{hex_lower, sha256_hex};
use lastdb_model::record_value::MAX_RECORD_BYTES;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
            Self::Record(addr) => ("record", addr),
            Self::Schema(name) => ("schema", name),
            Self::SchemaRecords(name) => ("schema-records", name),
        };
        format!("signed-replay-{ctx}:{kind}:{}", sha256_hex(addr))
    }
}

fn record_key(addr: &str, ctx: &str) -> String {
    ReplayTarget::Record(addr).key(ctx)
}

impl Metadata {
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

    pub(crate) async fn guard_schema_drop(
        &self,
        name: &str,
        replay: Option<&SchemaReplayDrop<'_>>,
        stage: bool,
    ) -> Res<bool> {
        self.require_replay_scope()?;
        let target = ReplayTarget::Schema(name);
        let key = target.key("drop");
        let live = self.store.get(key.as_bytes()).await.map_err(read_err)?;
        let Some(replay) = replay else {
            return live.is_none().then_some(true).ok_or_else(schema_busy);
        };
        if self.replay_receipt_exists(replay.id, replay.digest).await? {
            return Ok(false);
        }
        let intent = format!("{}:{}", hex_lower(replay.id), hex_lower(replay.digest));
        match live {
            Some(bytes) if bytes.as_slice() == intent.as_bytes() => return Ok(true),
            Some(_) => return Err(schema_busy()),
            None if !stage => return Err(schema_busy()),
            None => {}
        }
        if self.pending_replay(target).await?.is_some()
            || self
                .pending_replay(ReplayTarget::SchemaRecords(name))
                .await?
                .is_some()
        {
            return Err(schema_busy());
        }
        self.stage(vec![(key, Some(intent.into_bytes()))])
            .await
            .map_err(read_err)
            .map(|()| true)
    }

    pub(crate) async fn finish_schema_drop(
        &self,
        name: &str,
        drop_op: &SchemaReplayDrop<'_>,
    ) -> Res {
        self.stage_replay_receipt(drop_op.id, drop_op.digest)
            .await?;
        self.stage(vec![(ReplayTarget::Schema(name).key("drop"), None)])
            .await
            .map_err(read_err)
    }

    pub(crate) async fn stage_local_replay_targets<'a>(
        &self,
        targets: impl IntoIterator<Item = (ReplayTarget<'a>, &'a str)>,
    ) -> Res {
        let entries = targets
            .into_iter()
            .map(|(target, id)| (target.key("pending"), Some(id.as_bytes().to_vec())))
            .collect();
        self.stage(entries).await.map_err(read_err)
    }

    pub(crate) async fn pending_replay(&self, target: ReplayTarget<'_>) -> Res<Option<String>> {
        self.require_scope().map_err(read_err)?;
        let saved = self
            .store
            .get(target.key("pending").as_bytes())
            .await
            .map_err(read_err)?
            .map(|bytes| String::from_utf8(bytes).map_err(read_err))
            .transpose()?;
        Ok(saved.map(|id| match id.strip_prefix(LEGACY_PENDING) {
            Some(stripped) => stripped.to_owned(),
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
            .map(|(target, id)| (target.key("pending"), id))
            .collect();
        if map.is_empty() {
            return Ok(());
        }
        let existing = self
            .store
            .get_many(map.keys().map(|key| key.as_bytes().to_vec()).collect())
            .await
            .map_err(read_err)?;
        if existing.len() != map.len() {
            return Err(SchemaError::InvalidData(
                "incomplete pending replay lookup".into(),
            ));
        }
        let prefix = LEGACY_PENDING.as_bytes();
        let writes = map
            .into_iter()
            .zip(existing)
            .filter(|(_, cur)| cur.as_deref().is_none_or(|val| val.starts_with(prefix)))
            .map(|((key, id), _)| (key, Some(format!("{LEGACY_PENDING}{id}").into_bytes())))
            .collect();
        self.stage(writes).await.map_err(read_err)
    }

    pub(crate) async fn clear_pending_replay(&self, target: ReplayTarget<'_>, id: &str) -> Res {
        if self.pending_replay(target).await?.as_deref() == Some(id) {
            let mut rows = vec![(target.key("pending"), None)];
            if matches!(target, ReplayTarget::Record(_)) {
                rows.extend([
                    (target.key("pending-attachment"), None),
                    (target.key("baseline"), None),
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
                .flat_map(|addr| {
                    [record_key(addr, "pending"), record_key(addr, "winner")]
                        .map(String::into_bytes)
                })
                .collect();
            let rows = self.store.get_many(keys).await.map_err(read_err)?;
            if rows.len() != chunk.len() * 2 {
                return Err(SchemaError::InvalidData(
                    "incomplete replay baseline read".into(),
                ));
            }
            for pair in rows.chunks_exact(2) {
                let stamp = match (&pair[0], &pair[1]) {
                    (None, Some(bytes)) => Some(serde_json::from_slice(bytes).map_err(read_err)?),
                    _ => None,
                };
                stamps.push(stamp);
            }
        }
        Ok(stamps)
    }

    pub(crate) async fn replay_baseline(&self, addr: &str) -> Res<Option<Vec<u8>>> {
        self.require_replay_scope()?;
        let bytes = self
            .store
            .get(record_key(addr, "baseline").as_bytes())
            .await
            .map_err(read_err)?;
        check_len(bytes.as_deref(), MAX_BASELINE_BYTES, BASELINE_MSG).map(|()| bytes)
    }

    pub(crate) async fn stage_replay_baselines(&self, baselines: Vec<(&str, Vec<u8>)>) -> Res {
        let mut rows = Vec::with_capacity(baselines.len());
        for (addr, bytes) in baselines {
            check_len(Some(&bytes), MAX_BASELINE_BYTES, BASELINE_MSG)?;
            rows.push((record_key(addr, "baseline"), Some(bytes)));
        }
        self.stage(rows).await.map_err(read_err)
    }

    pub(crate) async fn pending_replay_attachment(&self, addr: &str) -> Res<bool> {
        self.require_replay_scope()?;
        self.store
            .get(record_key(addr, "pending-attachment").as_bytes())
            .await
            .map(|bytes| bytes.is_some())
            .map_err(read_err)
    }

    pub(crate) async fn stage_pending_replay_attachments<'a>(
        &self,
        addresses: impl IntoIterator<Item = &'a str>,
    ) -> Res {
        let rows = addresses
            .into_iter()
            .map(|addr| (record_key(addr, "pending-attachment"), Some(vec![1])))
            .collect();
        self.stage(rows).await.map_err(read_err)
    }

    pub(crate) async fn accept_replay(
        &self,
        target: ReplayTarget<'_>,
        stamp: &ReplayStamp,
    ) -> Res<bool> {
        if self
            .replay_winner(target)
            .await?
            .is_some_and(|prev| prev >= *stamp)
        {
            return Ok(false);
        }
        let bytes = serde_json::to_vec(stamp).map_err(read_err)?;
        self.stage(vec![(target.key("winner"), Some(bytes))])
            .await
            .map_err(read_err)
            .map(|()| true)
    }

    pub(crate) async fn replay_winner(&self, target: ReplayTarget<'_>) -> Res<Option<ReplayStamp>> {
        self.require_scope().map_err(read_err)?;
        self.store
            .get(target.key("winner").as_bytes())
            .await
            .map_err(read_err)?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(read_err))
            .transpose()
    }

    pub(crate) async fn deferred_replay(&self, addr: &str) -> Res<Option<Vec<u8>>> {
        let body = self
            .store
            .get(record_key(addr, "deferred").as_bytes())
            .await
            .map_err(read_err)?;
        check_len(body.as_deref(), MAX_DEFERRED_BYTES, DEFERRED_MSG).map(|()| body)
    }

    pub(crate) async fn stage_deferred_replay(&self, addr: &str, body: Option<Vec<u8>>) -> Res {
        check_len(body.as_deref(), MAX_DEFERRED_BYTES, DEFERRED_MSG)?;
        self.stage(vec![(record_key(addr, "deferred"), body)])
            .await
            .map_err(read_err)
    }
}

fn check_len(body: Option<&[u8]>, max: usize, msg: &str) -> Res {
    body.is_none_or(|bytes| !bytes.is_empty() && bytes.len() <= max)
        .then_some(())
        .ok_or_else(|| SchemaError::InvalidData(msg.into()))
}

fn schema_busy() -> SchemaError {
    SchemaError::Blocked("schema removal awaits local changes or its current replay".into())
}
