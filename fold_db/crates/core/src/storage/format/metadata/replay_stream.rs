//! How far this node has applied each device stream of a signed log.
//!
//! One row per stream, in the metadata namespace beside the `signed-replay-*`
//! rows, so a snapshot of the namespace carries it. The row is written in the
//! same transaction that writes the record an entry changes. "Accepted" and
//! "applied" are one fact, and a restart replays nothing.

use super::{read_err, Metadata};
use crate::schema::types::SchemaError;
use serde::{Deserialize, Serialize};

type Res<T = ()> = Result<T, SchemaError>;

/// The last entry of one device stream that this node applied.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayStreamHead {
    /// Sequence number of the entry. The first entry of a stream is 1.
    pub seq: u64,
    /// Hash of the entry, 64 lowercase hex characters.
    pub hash: String,
    /// Order value of the entry (the wire field `lamport`).
    pub order: u64,
    /// True once the stream carried an entry that failed verification. A frozen
    /// stream is never read or applied again.
    #[serde(default)]
    pub frozen: bool,
}

fn head_key(stream: &str) -> String {
    format!("signed-replay-head:{stream}")
}

impl Metadata {
    pub(crate) async fn replay_stream_head(&self, stream: &str) -> Res<Option<ReplayStreamHead>> {
        self.require_scope().map_err(read_err)?;
        self.store
            .get(head_key(stream).as_bytes())
            .await
            .map_err(read_err)?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(read_err))
            .transpose()
    }

    pub(crate) async fn has_any_replay_stream_head(&self) -> Res<bool> {
        self.require_scope().map_err(read_err)?;
        let (lower, upper) =
            lastdb_model::kind_partition::exact_prefix_bounds("signed-replay-head:");
        let scan = self
            .store
            .scan_range_yielding(lower.as_bytes(), upper.as_bytes(), 1, 1)
            .await
            .map_err(read_err)?;
        Ok(!scan.rows.is_empty())
    }

    pub(crate) async fn stage_replay_stream_head(
        &self,
        stream: &str,
        head: &ReplayStreamHead,
    ) -> Res {
        let bytes = serde_json::to_vec(head).map_err(read_err)?;
        self.stage(vec![(head_key(stream), Some(bytes))])
            .await
            .map_err(read_err)
    }
}
