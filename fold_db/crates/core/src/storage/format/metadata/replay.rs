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
use lastdb_model::hex::{hex_lower, sha256_hex};
use serde::{Deserialize, Serialize};

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

/// The sequence number of the newest captured write of one device. The spool
/// numbers its entries the same way, and the sender signs entry `n` of the
/// spool as entry `n` of the device stream.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalHead {
    seq: u64,
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

    /// The stamps of the next captured writes of this store, one for each order
    /// value, in commit order. `writer` is the node key in base64.
    ///
    /// The sequence numbers continue the spool: a store that has not stamped a
    /// write yet starts after the entries its spool already holds.
    pub(crate) async fn stage_local_replay_stamps(
        &self,
        writer: &str,
        orders: &[u64],
    ) -> Res<Vec<ReplayStamp>> {
        self.require_replay_scope()?;
        let device = self.runtime.capture_device().ok_or_else(|| {
            SchemaError::InvalidData("captured writes have no author device".into())
        })?;
        let writer_key = writer_hex(writer)?;
        let key = format!("signed-replay-local:{device}");
        let mut seq = match self.store.get(key.as_bytes()).await.map_err(read_err)? {
            Some(bytes) => {
                serde_json::from_slice::<LocalHead>(&bytes)
                    .map_err(read_err)?
                    .seq
            }
            None => self.runtime.accepted_status().head,
        };
        let mut stamps = Vec::with_capacity(orders.len());
        for &ordering_ms in orders {
            seq = seq.checked_add(1).ok_or_else(|| {
                SchemaError::InvalidData("the local stream sequence is exhausted".into())
            })?;
            stamps.push(ReplayStamp {
                ordering_ms,
                writer_key: writer_key.clone(),
                writer_device: device.to_owned(),
                writer_seq: seq,
            });
        }
        let bytes = serde_json::to_vec(&LocalHead { seq }).map_err(read_err)?;
        self.stage(vec![(key, Some(bytes))])
            .await
            .map_err(read_err)?;
        Ok(stamps)
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
