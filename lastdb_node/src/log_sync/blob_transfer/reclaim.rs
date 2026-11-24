//! `blob.list` and `blob.drop`: what the relay holds of a database, and the drop of the stored
//! files that nothing names.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::log_sync::relay::RelayClient;

/// The most digests one `blob.drop` names.
pub(in crate::log_sync) const DROP_BATCH: usize = 25;
/// The most stream heads one `blob.drop` names as its conditions.
pub(in crate::log_sync) const DROP_HEADS: usize = 32;

/// A stream and the sequence number of its head, as a `blob.drop` names it.
#[derive(Clone, Debug, Serialize)]
pub(in crate::log_sync) struct HeadAt {
    pub(in crate::log_sync) device_id: String,
    pub(in crate::log_sync) writer_seq: u64,
}

/// What one `blob.drop` did.
#[derive(Clone, Copy, Debug, Default, Deserialize)]
pub(in crate::log_sync) struct Dropped {
    /// Stored files the relay dropped.
    pub(in crate::log_sync) dropped: u64,
    /// The bytes it released for them.
    pub(in crate::log_sync) released_bytes: u64,
}

#[derive(Deserialize)]
struct Listed {
    sha256: String,
    bytes: u64,
}

/// True when `error` is the answer of a relay whose stream heads moved since the caller listed
/// them: HTTP 409 with the code `heads`.
fn heads_moved(error: &str) -> bool {
    let body = error
        .find('{')
        .and_then(|at| serde_json::from_str::<Value>(&error[at..]).ok());
    error.contains("HTTP 409") && body.is_some_and(|body| body["code"] == "heads")
}

impl RelayClient {
    /// Every committed blob of the database on the relay, all pages: digest to length.
    pub(in crate::log_sync) async fn blob_list(
        &self,
        database_id: &str,
    ) -> Result<BTreeMap<String, u64>, String> {
        let mut found = BTreeMap::new();
        let mut after: Option<String> = None;
        loop {
            let mut body = json!({ "database_id": database_id });
            if let Some(after) = &after {
                body["after"] = Value::String(after.clone());
            }
            let value = self.post("/relay/v1/blob/list", database_id, &body).await?;
            let page: Vec<Listed> = serde_json::from_value(value["blobs"].clone())
                .map_err(|_| "relay blob.list returned an invalid list".to_owned())?;
            for blob in page {
                found.insert(blob.sha256, blob.bytes);
            }
            let Some(next) = value["next"].as_str() else {
                return Ok(found);
            };
            // The pages ascend by digest, so a relay that does not move forward would never end.
            if after.as_deref().is_some_and(|last| next <= last) {
                return Err("relay blob.list did not move to the next page".into());
            }
            after = Some(next.to_owned());
        }
    }

    /// Ask the relay to drop the blobs `sha256` (at most [`DROP_BATCH`]) on the condition that
    /// the head of the snapshot stream is `snapshot` and every stream in `heads` still ends where
    /// it is named. `None` when a head moved: the relay dropped nothing, and the caller computes
    /// its list again. The relay refuses the manifest that the snapshot entry names.
    pub(in crate::log_sync) async fn blob_drop(
        &self,
        database_id: &str,
        snapshot: &HeadAt,
        heads: &[HeadAt],
        sha256: &[String],
    ) -> Result<Option<Dropped>, String> {
        let body = json!({
            "database_id": database_id,
            "snapshot": snapshot,
            "heads": heads,
            "sha256": sha256,
        });
        let value = match self.post("/relay/v1/blob/drop", database_id, &body).await {
            Ok(value) => value,
            Err(error) if heads_moved(&error) => return Ok(None),
            Err(error) => return Err(error),
        };
        let dropped: Dropped = serde_json::from_value(value)
            .map_err(|_| "relay blob.drop returned an invalid answer".to_owned())?;
        if let Some(usage) = &self.usage {
            usage.blobs_released(database_id, dropped.dropped, dropped.released_bytes)?;
        }
        Ok(Some(dropped))
    }
}
