//! Durable state for the signed database log and replay ordering.
//!
//! Each record has up to three stamp rows: `winner` is the stamp of the newest
//! write of its fields, `file-winner` of the newest write that set or cleared
//! its file, and `inline-winner` the same for its inline file. A delete is the
//! newest write of all three. A schema has one `winner` row for its newest put
//! or drop.
//!
//! A local write gets its stamp in the transaction that commits it, so every
//! row is final from the commit on. An entry of another device is compared
//! with these rows when it arrives, in any order.

use super::{read_err, Metadata};
use crate::schema::types::SchemaError;
use crate::storage::disk_backup::accepted::LocalEntry;
use crate::storage::disk_pack::transaction;
use lastdb_model::hex::{hex_lower, sha256_hex};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

type Res<T = ()> = Result<T, SchemaError>;

/// The order of one entry of the signed log among all entries, independent of
/// the order in which entries arrive.
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
}

/// The parts of a record that carry a stamp of their own.
pub(crate) const FILE_SLOT: &str = "file-winner";
pub(crate) const INLINE_SLOT: &str = "inline-winner";

pub(crate) struct SchemaReplayDrop<'a> {
    pub(crate) id: &'a [u8; 32],
    pub(crate) digest: &'a [u8; 32],
}

impl ReplayTarget<'_> {
    fn key(self, ctx: &str) -> String {
        let (kind, addr) = match self {
            Self::Record(addr) => ("record", addr),
            Self::Schema(name) => ("schema", name),
        };
        format!("signed-replay-{ctx}:{kind}:{}", sha256_hex(addr))
    }
}

/// The row that holds the newest entry of the stream one device signs.
fn chain_key(device: &str) -> String {
    format!("signed-replay-chain:{device}")
}

/// The node key of this store as the stamps name it: lowercase hex of the
/// public key that `writer` holds in base64.
fn writer_hex(writer: &str) -> Res<String> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(writer)
        .ok()
        .filter(|bytes| bytes.len() == 32)
        .map(hex_lower)
        .ok_or_else(|| SchemaError::InvalidData("the node writer id is not a public key".into()))
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
        let key = ReplayTarget::Schema(name).key("drop");
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

    /// The device whose stream the captured writes of this store are signed into.
    fn capture_device(&self) -> Res<&str> {
        let Some(author) = self.runtime.capture_author() else {
            return Err(SchemaError::InvalidData(
                "captured writes have no author".into(),
            ));
        };
        Ok(author.device())
    }

    /// The entry of this device's stream that the next captured write follows.
    /// `None` at the start of the stream. `writer` is the node key in base64.
    ///
    /// The row is written by every captured commit. A home whose writes were
    /// signed when they were sent has none yet: its stream then continues after
    /// the last entry the relay acknowledged, which must be the last of the spool.
    pub(crate) async fn local_replay_head(&self, writer: &str) -> Res<Option<LocalEntry>> {
        self.require_replay_scope()?;
        let device = self.capture_device()?;
        let held = self
            .store
            .get(chain_key(device).as_bytes())
            .await
            .map_err(read_err)?;
        if let Some(bytes) = held {
            return serde_json::from_slice(&bytes).map(Some).map_err(read_err);
        }
        let spool = self.runtime.accepted_status();
        let stream = format!("{}:{device}", writer_hex(writer)?);
        match self.replay_stream_head(&stream).await? {
            Some(sent) if sent.seq == spool.head && spool.acked == spool.head => {
                Ok(Some(LocalEntry {
                    seq: sent.seq,
                    hash: sent.hash,
                    order: sent.order,
                }))
            }
            None if spool.head == 0 => Ok(None),
            _ => Err(SchemaError::Blocked(
                "the spool holds writes that an older build made and did not send, and this \
                 build cannot send them; start the build that made them, wait until it has \
                 sent them, then start this build"
                    .into(),
            )),
        }
    }

    /// Refuse a home whose spool holds writes that an older build left unsigned.
    /// The store calls it once when it opens, so a node does not start on such a
    /// home: this build can neither send those writes nor sign an entry after them.
    pub(crate) async fn require_signed_spool(&self, writer: &str) -> Result<(), String> {
        let head = transaction::scope(Arc::clone(&self.runtime), self.local_replay_head(writer));
        match head.await {
            Ok(_) => Ok(()),
            Err(SchemaError::Blocked(why)) => Err(why),
            Err(other) => Err(other.to_string()),
        }
    }

    /// Record the newest entry of this device's stream, in the commit that made it.
    pub(crate) async fn stage_local_replay_head(&self, head: &LocalEntry) -> Res {
        let device = self.capture_device()?;
        let bytes = serde_json::to_vec(head).map_err(read_err)?;
        self.stage(vec![(chain_key(device), Some(bytes))])
            .await
            .map_err(read_err)
    }

    /// The stamp of the captured write that is entry `seq` of this device's stream.
    pub(crate) fn local_replay_stamp(
        &self,
        writer: &str,
        seq: u64,
        order: u64,
    ) -> Res<ReplayStamp> {
        Ok(ReplayStamp {
            ordering_ms: order,
            writer_key: writer_hex(writer)?,
            writer_device: self.capture_device()?.to_owned(),
            writer_seq: seq,
        })
    }

    /// Write stamp rows without reading them first. `rows` holds a target, the
    /// slot (`None` for the winner row) and the stamp.
    ///
    /// Only a local commit calls this. Its stamp is above every stamp the store
    /// holds, because the author clock is raised past every entry that is applied.
    pub(crate) async fn stage_replay_stamps<'a>(
        &self,
        rows: impl IntoIterator<Item = (ReplayTarget<'a>, Option<&'static str>, &'a ReplayStamp)>,
    ) -> Res {
        let mut staged = Vec::new();
        for (target, slot, stamp) in rows {
            let bytes = serde_json::to_vec(stamp).map_err(read_err)?;
            staged.push((target.key(slot.unwrap_or("winner")), Some(bytes)));
        }
        self.stage(staged).await.map_err(read_err)
    }

    pub(crate) async fn accept_replay(
        &self,
        target: ReplayTarget<'_>,
        stamp: &ReplayStamp,
    ) -> Res<bool> {
        self.accept_replay_slot(target, "winner", stamp).await
    }

    pub(crate) async fn accept_replay_slot(
        &self,
        target: ReplayTarget<'_>,
        slot: &str,
        stamp: &ReplayStamp,
    ) -> Res<bool> {
        if self
            .replay_slot_winner(target, slot)
            .await?
            .is_some_and(|prev| prev >= *stamp)
        {
            return Ok(false);
        }
        let bytes = serde_json::to_vec(stamp).map_err(read_err)?;
        self.stage(vec![(target.key(slot), Some(bytes))])
            .await
            .map_err(read_err)
            .map(|()| true)
    }

    pub(crate) async fn replay_winner(&self, target: ReplayTarget<'_>) -> Res<Option<ReplayStamp>> {
        self.replay_slot_winner(target, "winner").await
    }

    pub(crate) async fn replay_slot_winner(
        &self,
        target: ReplayTarget<'_>,
        slot: &str,
    ) -> Res<Option<ReplayStamp>> {
        self.require_scope().map_err(read_err)?;
        self.store
            .get(target.key(slot).as_bytes())
            .await
            .map_err(read_err)?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(read_err))
            .transpose()
    }
}

fn schema_busy() -> SchemaError {
    SchemaError::Blocked("schema removal awaits its current replay".into())
}
