//! Durable state for the signed database log, separate from native revisions.

use super::{read_err, Metadata};
use crate::schema::SchemaError;
use lastdb_model::hex::{hex_lower, sha256_hex};
use lastdb_model::record_value::MAX_RECORD_BYTES;
use serde::{Deserialize, Serialize};

const MAX_DEFERRED_BYTES: usize = 450 * 1024;
const MAX_BASELINE_BYTES: usize = MAX_RECORD_BYTES + 2048;
const LEGACY_PENDING: &str = "legacy:";

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
    fn key(self, context: &str) -> String {
        let (kind, address) = match self {
            Self::Record(address) => ("record", address),
            Self::Schema(name) => ("schema", name),
            Self::SchemaRecords(name) => ("schema-records", name),
        };
        format!("signed-replay-{context}:{kind}:{}", sha256_hex(address))
    }
}

impl Metadata {
    pub(super) async fn receipt_exists(
        &self,
        key: &str,
        digest: &[u8; 32],
        reused: &str,
    ) -> Result<bool, SchemaError> {
        self.require_replay_scope()?;
        match self.0.get(key.as_bytes()).await.map_err(read_err)? {
            Some(saved) if saved.as_slice() == digest => Ok(true),
            Some(_) => Err(SchemaError::InvalidField(reused.into())),
            None => Ok(false),
        }
    }

    pub(crate) async fn guard_schema_drop(
        &self,
        name: &str,
        replay: Option<&SchemaReplayDrop<'_>>,
        stage: bool,
    ) -> Result<bool, SchemaError> {
        self.require_replay_scope()?;
        let target = ReplayTarget::Schema(name);
        let key = target.key("drop");
        let live = self.0.get(key.as_bytes()).await.map_err(read_err)?;
        let Some(replay) = replay else {
            return if live.is_none() {
                Ok(true)
            } else {
                Err(schema_busy())
            };
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
            .map_err(read_err)?;
        Ok(true)
    }

    pub(crate) async fn finish_schema_drop(
        &self,
        schema: &str,
        replay: &SchemaReplayDrop<'_>,
    ) -> Result<(), SchemaError> {
        self.stage_replay_receipt(replay.id, replay.digest).await?;
        self.stage(vec![(ReplayTarget::Schema(schema).key("drop"), None)])
            .await
            .map_err(read_err)
    }

    pub(crate) fn require_replay_scope(&self) -> Result<(), SchemaError> {
        self.require_scope().map_err(read_err)
    }

    pub(crate) async fn stage_local_replay_targets<'a>(
        &self,
        targets: impl IntoIterator<Item = (ReplayTarget<'a>, &'a str)>,
    ) -> Result<(), SchemaError> {
        let entries = targets
            .into_iter()
            .map(|(target, mutation_id)| {
                (target.key("pending"), Some(mutation_id.as_bytes().to_vec()))
            })
            .collect();
        self.stage(entries).await.map_err(read_err)
    }

    pub(crate) async fn pending_replay(
        &self,
        target: ReplayTarget<'_>,
    ) -> Result<Option<String>, SchemaError> {
        self.require_scope().map_err(read_err)?;
        let saved = self
            .0
            .get(target.key("pending").as_bytes())
            .await
            .map_err(read_err)?
            .map(|bytes| String::from_utf8(bytes).map_err(read_err))
            .transpose()?;
        Ok(saved.map(|id| match id.strip_prefix(LEGACY_PENDING) {
            Some(mutation_id) => mutation_id.to_owned(),
            None => id,
        }))
    }

    /// Add old spool items in order without replacing a newer local write's marker.
    pub(crate) async fn stage_legacy_replay_targets<'a>(
        &self,
        targets: impl IntoIterator<Item = (ReplayTarget<'a>, &'a str)>,
    ) -> Result<(), SchemaError> {
        self.require_scope().map_err(read_err)?;
        let targets: std::collections::BTreeMap<_, _> = targets
            .into_iter()
            .map(|(target, mutation_id)| (target.key("pending"), mutation_id))
            .collect();
        if targets.is_empty() {
            return Ok(());
        }
        let existing = self
            .0
            .get_many(targets.keys().map(|key| key.as_bytes().to_vec()).collect())
            .await
            .map_err(read_err)?;
        if existing.len() != targets.len() {
            return Err(SchemaError::InvalidData(
                "incomplete pending replay lookup".into(),
            ));
        }
        let writes = targets
            .into_iter()
            .zip(existing)
            .filter(|(_, current)| {
                current
                    .as_deref()
                    .is_none_or(|value| value.starts_with(LEGACY_PENDING.as_bytes()))
            })
            .map(|((key, mutation_id), _)| {
                (
                    key,
                    Some(format!("{LEGACY_PENDING}{mutation_id}").into_bytes()),
                )
            })
            .collect();
        self.stage(writes).await.map_err(read_err)
    }

    pub(crate) async fn clear_pending_replay(
        &self,
        target: ReplayTarget<'_>,
        mutation_id: &str,
    ) -> Result<(), SchemaError> {
        if self.pending_replay(target).await?.as_deref() == Some(mutation_id) {
            let mut rows = vec![(target.key("pending"), None)];
            if matches!(target, ReplayTarget::Record(_)) {
                rows.push((target.key("pending-attachment"), None));
                rows.push((target.key("baseline"), None));
            }
            self.stage(rows).await.map_err(read_err)?;
        }
        Ok(())
    }

    pub(crate) async fn replay_baseline_sources(
        &self,
        addresses: &[&str],
    ) -> Result<Vec<Option<ReplayStamp>>, SchemaError> {
        self.require_replay_scope()?;
        let mut stamps = Vec::with_capacity(addresses.len());
        for chunk in addresses.chunks(5000) {
            let keys: Vec<_> = chunk
                .iter()
                .flat_map(|address| {
                    let target = ReplayTarget::Record(address);
                    [target.key("pending"), target.key("winner")].map(String::into_bytes)
                })
                .collect();
            let rows = self.0.get_many(keys).await.map_err(read_err)?;
            if rows.len() != chunk.len() * 2 {
                return Err(SchemaError::InvalidData(
                    "incomplete replay baseline read".into(),
                ));
            }
            for pair in rows.chunks_exact(2) {
                let stamp = if pair[0].is_none() {
                    pair[1]
                        .as_deref()
                        .map(serde_json::from_slice)
                        .transpose()
                        .map_err(read_err)?
                } else {
                    None
                };
                stamps.push(stamp);
            }
        }
        Ok(stamps)
    }

    pub(crate) async fn replay_baseline(
        &self,
        address: &str,
    ) -> Result<Option<Vec<u8>>, SchemaError> {
        self.require_replay_scope()?;
        let key = ReplayTarget::Record(address).key("baseline");
        let bytes = self.0.get(key.as_bytes()).await.map_err(read_err)?;
        check_baseline(bytes.as_deref())?;
        Ok(bytes)
    }

    pub(crate) async fn stage_replay_baselines(
        &self,
        baselines: Vec<(&str, Vec<u8>)>,
    ) -> Result<(), SchemaError> {
        let mut rows = Vec::with_capacity(baselines.len());
        for (address, bytes) in baselines {
            check_baseline(Some(&bytes))?;
            rows.push((ReplayTarget::Record(address).key("baseline"), Some(bytes)));
        }
        self.stage(rows).await.map_err(read_err)
    }

    pub(crate) async fn pending_replay_attachment(
        &self,
        address: &str,
    ) -> Result<bool, SchemaError> {
        self.require_replay_scope()?;
        let key = ReplayTarget::Record(address).key("pending-attachment");
        self.0
            .get(key.as_bytes())
            .await
            .map(|bytes| bytes.is_some())
            .map_err(read_err)
    }

    pub(crate) async fn stage_pending_replay_attachments<'a>(
        &self,
        addresses: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), SchemaError> {
        let rows = addresses
            .into_iter()
            .map(|address| {
                (
                    ReplayTarget::Record(address).key("pending-attachment"),
                    Some(vec![1]),
                )
            })
            .collect();
        self.stage(rows).await.map_err(read_err)
    }

    pub(crate) async fn accept_replay(
        &self,
        target: ReplayTarget<'_>,
        stamp: &ReplayStamp,
    ) -> Result<bool, SchemaError> {
        if self
            .replay_winner(target)
            .await?
            .is_some_and(|previous| previous >= *stamp)
        {
            return Ok(false);
        }
        let key = target.key("winner");
        let bytes = serde_json::to_vec(stamp).map_err(read_err)?;
        self.stage(vec![(key, Some(bytes))])
            .await
            .map_err(read_err)?;
        Ok(true)
    }

    pub(crate) async fn replay_winner(
        &self,
        target: ReplayTarget<'_>,
    ) -> Result<Option<ReplayStamp>, SchemaError> {
        self.require_scope().map_err(read_err)?;
        self.0
            .get(target.key("winner").as_bytes())
            .await
            .map_err(read_err)?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(read_err))
            .transpose()
    }

    pub(crate) async fn deferred_replay(
        &self,
        record_address: &str,
    ) -> Result<Option<Vec<u8>>, SchemaError> {
        let key = ReplayTarget::Record(record_address).key("deferred");
        let body = self.0.get(key.as_bytes()).await.map_err(read_err)?;
        check_deferred(body.as_deref())?;
        Ok(body)
    }

    pub(crate) async fn stage_deferred_replay(
        &self,
        record_address: &str,
        body: Option<Vec<u8>>,
    ) -> Result<(), SchemaError> {
        check_deferred(body.as_deref())?;
        let key = ReplayTarget::Record(record_address).key("deferred");
        self.stage(vec![(key, body)]).await.map_err(read_err)
    }
}

fn check_deferred(body: Option<&[u8]>) -> Result<(), SchemaError> {
    if body.is_some_and(|body| body.is_empty() || body.len() > MAX_DEFERRED_BYTES) {
        return Err(SchemaError::InvalidData(
            "invalid deferred replay envelope length".into(),
        ));
    }
    Ok(())
}

fn check_baseline(body: Option<&[u8]>) -> Result<(), SchemaError> {
    if body.is_some_and(|body| body.is_empty() || body.len() > MAX_BASELINE_BYTES) {
        return Err(SchemaError::InvalidData(
            "invalid replay baseline length".into(),
        ));
    }
    Ok(())
}

fn schema_busy() -> SchemaError {
    SchemaError::Blocked("schema removal awaits local changes or its current replay".into())
}
