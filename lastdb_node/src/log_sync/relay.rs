//! HTTP client for `/relay/v1/log/*`, speaking the types of [`relay_protocol`].

mod http;
mod segments;

pub(super) use http::CallError;

use std::time::{Duration, Instant};

use app_identity_crypto::SigningKey;
use mutation_log::{DatabaseId, DeviceId, SignedEntry};
use relay_protocol::{
    LogAppend, LogAppendAnswer, LogAppendRequest, LogStreams, LogStreamsRequest, LogTrim,
    LogTrimRequest,
};

/// Bound a hung peer so the sync loop can return and re-push the log.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// A blob transfer fails when it makes no progress for this long, however slow the link.
pub(super) const STALL_TIMEOUT: Duration = Duration::from_secs(30);
/// CI probe budget: request timeout plus a little slack.
const HUNG_PEER_BUDGET: Duration = Duration::from_secs(45);

/// Starts the error of a read of entries that the relay dropped after a snapshot.
pub(super) const BEHIND_THE_TRIM: &str = "this device is behind history that the relay dropped after a snapshot; restore the database into a new home with `lastdb backup restore`";

// One device stream of a writer as `log.streams` lists it.
pub(super) use relay_protocol::StreamHead;

pub(super) struct RelayClient {
    pub(super) base: String,
    pub(super) signing_key: SigningKey,
    pub(super) node_key_hex: String,
    pub(super) device_id: String,
    pub(super) http: reqwest::Client,
    /// Presigned upload client: no total/read timeout (watched by `blob_transfer`), no idle pool.
    pub(super) transfer: reqwest::Client,
    /// Presigned download client: [`STALL_TIMEOUT`] per read, no idle pool.
    pub(super) download: reqwest::Client,
    /// The account of what this device put, for a client that syncs a home. A client that only
    /// reads has none.
    pub(super) usage: Option<super::usage::Ledger>,
}

impl RelayClient {
    pub(super) fn new(
        base: &str,
        signing_key: SigningKey,
        node_key_hex: String,
        device_id: &mutation_log::DeviceId,
    ) -> Result<Self, String> {
        // trace-egress: public — relay gates by grants; node key only in signed headers.
        let http = reqwest::Client::builder()
            .user_agent(relay_protocol::user_agent(crate::version::build_version()))
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .pool_max_idle_per_host(0)
            .build()
            .map_err(|e| format!("relay http client: {e}"))?;
        // trace-egress: public — presigned blob URLs; the signed headers go only to the relay.
        let transfer = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            // A fresh connection for each upload: a body cannot be sent again on a pooled
            // connection that the store closed while it was idle ("connection closed before
            // message completed").
            .pool_max_idle_per_host(0)
            .build()
            .map_err(|e| format!("relay http client: {e}"))?;
        // trace-egress: public — presigned blob URLs; the signed headers go only to the relay.
        let download = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(STALL_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .pool_max_idle_per_host(0)
            .build()
            .map_err(|e| format!("relay http client: {e}"))?;
        Ok(Self {
            base: base.trim_end_matches('/').to_owned(),
            signing_key,
            node_key_hex,
            device_id: device_id.to_string(),
            http,
            transfer,
            download,
            usage: None,
        })
    }

    /// A client for the relay at `base` with this node's keys and device id.
    pub(super) fn for_host(host: &crate::host::Host, base: &str) -> Result<Self, String> {
        let signing_key = SigningKey::from_bytes(&host.keypair.secret_key_bytes());
        let node_key_hex =
            mutation_log::NodeKey::from_bytes(host.keypair.public_key_bytes()).to_hex();
        let device_id = super::device::device_id(&host.home)?;
        let mut client = Self::new(base, signing_key, node_key_hex, &device_id)?;
        let spool_acked = host.db.db_ops().native_runtime().accepted_status().acked;
        client.usage = Some(super::usage::Ledger::new(&host.home, spool_acked));
        Ok(client)
    }

    pub(super) async fn append(
        &self,
        database_id: &str,
        entries: &[SignedEntry],
    ) -> Result<(u64, u64), String> {
        let values = entries
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| "cannot encode signed entry".to_owned())?;
        let request = LogAppendRequest::entries(database_id.to_owned(), values);
        let answer = self.call::<LogAppend>(&request).await?;
        let (stored, duplicate) = append_counts(answer, entries.len() as u64);
        if let Some(usage) = &self.usage {
            usage.entries_accepted(
                database_id,
                entries,
                stored.saturating_add(duplicate),
                false,
            )?;
        }
        Ok((stored, duplicate))
    }

    /// Read one writer's entries. Omit `device_id` to receive that writer's
    /// entries across every device; the response cursor carries the device.
    pub(super) async fn read(
        &self,
        database_id: &str,
        writer_key: &str,
        device_id: Option<&str>,
        from_seq: u64,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<(Vec<SignedEntry>, Option<String>), String> {
        let page = self
            .read_page(database_id, writer_key, device_id, from_seq, cursor, limit)
            .await?;
        Ok((page.entries, page.cursor))
    }

    /// Ask the relay to drop what the snapshot entry `writer_seq` of the snapshot stream
    /// `device_id` covers, and the snapshot entries and files older than it. The relay reads
    /// that entry itself. `true` when it has nothing more to drop, `false` when it stopped
    /// at its work limit for one call and a repeat continues.
    pub(super) async fn trim(
        &self,
        database_id: &str,
        device_id: &str,
        writer_seq: u64,
    ) -> Result<bool, String> {
        let request = LogTrimRequest {
            database_id: database_id.to_owned(),
            device_id: device_id.to_owned(),
            writer_seq,
        };
        let answer = self.call::<LogTrim>(&request).await?;
        if let Some(usage) = &self.usage {
            usage.released(database_id, &answer)?;
        }
        Ok(answer.done)
    }

    /// The device streams of `writer_key` and how far each reaches: for each,
    /// the contiguous head (the highest `writer_seq` such that every seq from 1
    /// up to it is on the relay) and the hash of that entry. A writer with no
    /// stream gives an empty list. Access is the access of an all-devices
    /// `log.read` of the same writer.
    pub(super) async fn streams(
        &self,
        database_id: &str,
        writer_key: &str,
    ) -> Result<Vec<StreamHead>, String> {
        let request = LogStreamsRequest {
            database_id: database_id.to_owned(),
            writer_key: writer_key.to_owned(),
        };
        Ok(self.call::<LogStreams>(&request).await?.streams)
    }
}

/// `(stored, duplicate)` in entries for an append of `sent` entries. A duplicate answer says that
/// the entries the relay did not store it held already.
fn append_counts(answer: LogAppendAnswer, sent: u64) -> (u64, u64) {
    if answer.stored < sent && answer.duplicate {
        (answer.stored, sent.saturating_sub(answer.stored))
    } else {
        (answer.stored, 0)
    }
}

/// CI: append against a hung `base` must error inside [`HUNG_PEER_BUDGET`].
pub async fn hung_relay_must_timeout(base: &str) -> Result<Duration, String> {
    let signing_key = SigningKey::from_bytes(&[7u8; 32]);
    let node_key_hex =
        mutation_log::Hex32::from_bytes(signing_key.verifying_key().to_bytes()).to_hex();
    let device_id = DeviceId::from_bytes([3u8; 32]);
    let client = RelayClient::new(base, signing_key, node_key_hex, &device_id)?;
    let database_id = DatabaseId::from_bytes([9u8; 32]).to_hex();
    let started = Instant::now();
    match client.append(&database_id, &[]).await {
        Err(_) => {
            let elapsed = started.elapsed();
            if elapsed <= HUNG_PEER_BUDGET {
                Ok(elapsed)
            } else {
                Err(format!(
                    "hung peer error took {:.3}s, over {}s",
                    elapsed.as_secs_f64(),
                    HUNG_PEER_BUDGET.as_secs()
                ))
            }
        }
        Ok(_) => Err("hung peer accepted an empty append".into()),
    }
}
